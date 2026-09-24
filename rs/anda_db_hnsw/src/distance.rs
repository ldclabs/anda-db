use half::bf16;
use rand::{distr::Uniform, prelude::*, rng};
use serde::{Deserialize, Serialize};

use crate::error::HnswError;

/// Distance metric used for similarity computation.
///
/// All variants return **smaller values for more similar vectors**, which keeps
/// the downstream nearest-neighbor logic uniform. In particular
/// [`DistanceMetric::InnerProduct`] returns `−dot(a, b)` so that "higher inner
/// product" maps to "smaller distance".
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean distance — √Σ (aᵢ − bᵢ)².
    Euclidean,
    /// Cosine distance — 1 − cos(θ). Returns `1.0` when either vector is (near-)zero.
    Cosine,
    /// Negative inner product — −Σ aᵢ bᵢ.
    InnerProduct,
    /// Manhattan distance — Σ |aᵢ − bᵢ|.
    Manhattan,
}

impl DistanceMetric {
    /// Computes the distance between two `bf16` vectors.
    ///
    /// Internally promotes each element to `f32` before accumulating; the
    /// result is returned as `f32`.
    ///
    /// # Errors
    /// Returns [`HnswError::DimensionMismatch`] for different lengths, or
    /// [`HnswError::Generic`] for non-finite input or an unrepresentable result.
    pub fn compute(&self, a: &[bf16], b: &[bf16]) -> Result<f32, HnswError> {
        self.checked(a, b)
    }

    /// Computes the distance between two `f32` vectors.
    ///
    /// Useful for callers that want to avoid the `bf16` round-trip at
    /// evaluation time (e.g. during offline evaluation).
    ///
    /// # Errors
    /// Returns [`HnswError::DimensionMismatch`] for different lengths, or
    /// [`HnswError::Generic`] for non-finite input or an unrepresentable result.
    pub fn compute_f32(&self, a: &[f32], b: &[f32]) -> Result<f32, HnswError> {
        self.checked(a, b)
    }

    /// Computes the distance between an `f32` query and a stored `bf16` vector.
    ///
    /// This avoids quantizing the query to `bf16` first: only the stored
    /// vector carries quantization error, which improves ranking fidelity on
    /// the search hot path.
    ///
    /// # Errors
    /// Returns [`HnswError::DimensionMismatch`] for different lengths, or
    /// [`HnswError::Generic`] for non-finite input or an unrepresentable result.
    pub fn compute_mixed(&self, a: &[f32], b: &[bf16]) -> Result<f32, HnswError> {
        self.checked(a, b)
    }

    fn checked<A: AsF32, B: AsF32>(&self, a: &[A], b: &[B]) -> Result<f32, HnswError> {
        check_dimensions(a, b)?;
        if a.iter().any(|v| !v.as_f32().is_finite()) || b.iter().any(|v| !v.as_f32().is_finite()) {
            return Err(numeric_error("vectors must contain only finite values"));
        }
        self.validated(a, b)
    }

    fn validated<A: AsF32, B: AsF32>(&self, a: &[A], b: &[B]) -> Result<f32, HnswError> {
        let value = self.dispatch(a, b);
        if value.is_finite() {
            return Ok(value);
        }
        finite_distance(self.wide(a, b))
    }

    /// Computes a distance that can be represented by the persisted `bf16`
    /// edge format. Modern inserts validate this invariant up front; the
    /// fallible result also keeps legacy snapshots from panicking during
    /// migration when their vectors predate those bounds.
    pub(crate) fn stored(&self, a: &[bf16], b: &[bf16]) -> Result<f32, HnswError> {
        let value = self.validated(a, b)?;
        stored_distance(value)
    }

    /// Internal graph operands have already been validated and their norms are
    /// immutable. Reuse them during selection, pruning and deletion repair.
    pub(crate) fn stored_with_norms(
        &self,
        a: &[bf16],
        norm_a: f64,
        b: &[bf16],
        norm_b: f64,
    ) -> Result<f32, HnswError> {
        let value = if *self == Self::Cosine {
            cosine_with_norms(a, norm_a, b, norm_b)?
        } else {
            self.validated(a, b)?
        };
        stored_distance(value)
    }

    fn wide<A: AsF32, B: AsF32>(&self, a: &[A], b: &[B]) -> f64 {
        match self {
            Self::Euclidean => a
                .iter()
                .zip(b)
                .map(|(a, b)| {
                    let d = a.as_f32() as f64 - b.as_f32() as f64;
                    d * d
                })
                .sum::<f64>()
                .sqrt(),
            Self::Manhattan => a
                .iter()
                .zip(b)
                .map(|(a, b)| (a.as_f32() as f64 - b.as_f32() as f64).abs())
                .sum(),
            Self::InnerProduct => -dot_wide(a, b),
            Self::Cosine => cosine_wide(a, b),
        }
    }

    /// Guarantees that every pair of stored vectors has a finite bf16 edge
    /// distance, before any graph mutation. Cosine accepts all finite inputs.
    pub(crate) fn validate_stored(&self, vector: &[bf16], name: &str) -> Result<(), HnswError> {
        if vector.iter().any(|v| !v.is_finite()) {
            return Err(HnswError::Generic {
                name: name.into(),
                source: "Vector contains NaN or infinity".into(),
            });
        }
        let limit = bf16::MAX.to_f32() as f64 / 4.0;
        let valid = match self {
            Self::Cosine => true,
            Self::Euclidean => norm(vector) <= limit,
            Self::Manhattan => {
                vector
                    .iter()
                    .map(|v| (v.to_f32() as f64).abs())
                    .sum::<f64>()
                    <= limit
            }
            Self::InnerProduct => norm(vector) <= limit.sqrt(),
        };
        if !valid {
            return Err(HnswError::Generic {
                name: name.into(),
                source: format!("Vector magnitude exceeds the safe stored range for {self:?}")
                    .into(),
            });
        }
        Ok(())
    }

    #[inline]
    fn dispatch<A: AsF32, B: AsF32>(&self, a: &[A], b: &[B]) -> f32 {
        match self {
            DistanceMetric::Euclidean => euclidean_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::InnerProduct => inner_product(a, b),
            DistanceMetric::Manhattan => manhattan_distance(a, b),
        }
    }
}

#[inline]
fn check_dimensions<A, B>(a: &[A], b: &[B]) -> Result<(), HnswError> {
    if a.len() != b.len() {
        return Err(HnswError::DimensionMismatch {
            name: "unknown".to_string(),
            expected: a.len(),
            got: b.len(),
        });
    }
    Ok(())
}

/// Random layer generator for HNSW.
///
/// Draws a layer index from a truncated exponential distribution so that the
/// expected number of nodes at layer `ℓ` decays as `M^{-ℓ}`, where `M` is
/// [`crate::HnswConfig::max_connections`]. This reproduces the behavior of
/// the reference HNSW paper.
#[derive(Debug)]
pub struct LayerGen {
    /// Uniform distribution sampler
    uniform: Uniform<f64>,
    /// Scaling factor for the exponential distribution
    scale: f64,
    /// Maximum layer (exclusive)
    max_level: u8,
}

impl LayerGen {
    /// Creates a new layer generator
    ///
    /// # Arguments
    ///
    /// * `max_connections` - Maximum connections per node
    /// * `max_level` - Maximum layer (exclusive)
    ///
    /// # Returns
    ///
    /// * `LayerGen` - New layer generator
    pub fn new(max_connections: u8, max_level: u8) -> Self {
        Self::new_with_scale(max_connections, 1.0, max_level)
    }

    /// Creates a new layer generator with a custom scale factor
    ///
    /// # Arguments
    ///
    /// * `max_connections` - Maximum connections per node
    /// * `scale_factor` - Custom scale factor for the distribution
    /// * `max_level` - Maximum layer (exclusive)
    ///
    /// # Returns
    ///
    /// * `LayerGen` - New layer generator
    pub fn new_with_scale(max_connections: u8, scale_factor: f64, max_level: u8) -> Self {
        let max_connections = max_connections.max(2);
        let max_level = max_level.max(1);
        let scale_factor = if scale_factor.is_finite() && scale_factor > 0.0 {
            scale_factor
        } else {
            1.0
        };
        let base_scale = 1.0 / (max_connections as f64).ln();
        LayerGen {
            uniform: Uniform::<f64>::new(0.0, 1.0).unwrap(),
            scale: base_scale * scale_factor,
            max_level,
        }
    }

    /// Generates a random layer for a new node
    ///
    /// Uses an exponential distribution to determine the layer,
    /// ensuring that higher layers have fewer nodes.
    ///
    /// # Arguments
    ///
    /// * `current_max_layer` - Current maximum layer in the index
    ///
    /// # Returns
    ///
    /// * `u8` - Generated layer
    pub fn generate(&self, current_max_layer: u8) -> u8 {
        self.generate_with(current_max_layer, &mut rng())
    }

    /// Uses a caller-owned RNG for reproducible construction and benchmarks.
    pub fn generate_with<R: Rng + ?Sized>(&self, current_max_layer: u8, r: &mut R) -> u8 {
        let val = r.sample(self.uniform).max(f64::MIN_POSITIVE);

        // Sample l = ⌊−ln(u) · scale⌋ from an exponential distribution.
        let level = (-val.ln() * self.scale).floor() as u8;

        // Clamp into the valid range; never skip more than one layer at a time.
        level
            .min(current_max_layer.saturating_add(1))
            .min(self.max_level.saturating_sub(1))
    }
}

/// Element types that promote losslessly to `f32` for distance computation.
pub(crate) trait AsF32: Copy {
    fn as_f32(self) -> f32;
}

impl AsF32 for f32 {
    #[inline(always)]
    fn as_f32(self) -> f32 {
        self
    }
}

impl AsF32 for bf16 {
    #[inline(always)]
    fn as_f32(self) -> f32 {
        // Branchless widening; half's NaN-quieting branch blocks vectorization.
        f32::from_bits((self.to_bits() as u32) << 16)
    }
}

/// Unroll width for the distance kernels below. Eight independent `f32`
/// accumulators break the floating-point add dependency chain so the loop can
/// be pipelined / auto-vectorized; the slight change in summation order is
/// well within `bf16` quantization noise.
const LANES: usize = 8;

#[inline]
fn euclidean_distance<A: AsF32, B: AsF32>(a: &[A], b: &[B]) -> f32 {
    let mut acc = [0.0f32; LANES];
    let (chunks_a, remainder_a) = a.as_chunks::<LANES>();
    let (chunks_b, remainder_b) = b.as_chunks::<LANES>();
    for (ca, cb) in chunks_a.iter().zip(chunks_b) {
        for i in 0..LANES {
            let d = ca[i].as_f32() - cb[i].as_f32();
            acc[i] += d * d;
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for (&x, &y) in remainder_a.iter().zip(remainder_b) {
        let d = x.as_f32() - y.as_f32();
        sum += d * d;
    }
    if sum < f32::MIN_POSITIVE {
        // Squared differences may underflow even when the final distance is
        // representable in f32. The cold path also handles exact duplicates.
        return a
            .iter()
            .zip(b)
            .map(|(a, b)| {
                let d = a.as_f32() as f64 - b.as_f32() as f64;
                d * d
            })
            .sum::<f64>()
            .sqrt() as f32;
    }
    sum.sqrt()
}

#[inline]
fn cosine_distance<A: AsF32, B: AsF32>(a: &[A], b: &[B]) -> f32 {
    let mut dot = [0.0f32; LANES];
    let mut norm_a2 = [0.0f32; LANES];
    let mut norm_b2 = [0.0f32; LANES];
    let (chunks_a, remainder_a) = a.as_chunks::<LANES>();
    let (chunks_b, remainder_b) = b.as_chunks::<LANES>();
    for (ca, cb) in chunks_a.iter().zip(chunks_b) {
        for i in 0..LANES {
            let x = ca[i].as_f32();
            let y = cb[i].as_f32();
            dot[i] += x * y;
            norm_a2[i] += x * x;
            norm_b2[i] += y * y;
        }
    }
    let mut dot_sum: f32 = dot.iter().sum();
    let mut norm_a2_sum: f32 = norm_a2.iter().sum();
    let mut norm_b2_sum: f32 = norm_b2.iter().sum();
    for (&x, &y) in remainder_a.iter().zip(remainder_b) {
        let x = x.as_f32();
        let y = y.as_f32();
        dot_sum += x * y;
        norm_a2_sum += x * x;
        norm_b2_sum += y * y;
    }
    let norm_a = norm_a2_sum.sqrt();
    let norm_b = norm_b2_sum.sqrt();
    if !dot_sum.is_finite()
        || !norm_a.is_finite()
        || !norm_b.is_finite()
        || !(norm_a * norm_b).is_finite()
    {
        return cosine_wide(a, b) as f32;
    }
    if norm_a < f32::EPSILON || norm_b < f32::EPSILON {
        return 1.0;
    }
    1.0 - (dot_sum / (norm_a * norm_b)).clamp(-1.0, 1.0)
}

fn numeric_error(message: &str) -> HnswError {
    HnswError::Generic {
        name: "distance".into(),
        source: message.to_owned().into(),
    }
}

fn finite_distance(value: f64) -> Result<f32, HnswError> {
    let result = value as f32;
    if result.is_finite() {
        Ok(result)
    } else {
        Err(numeric_error("distance is outside the finite f32 range"))
    }
}

pub(crate) fn norm<A: AsF32>(values: &[A]) -> f64 {
    values
        .iter()
        .map(|v| {
            let v = v.as_f32() as f64;
            v * v
        })
        .sum::<f64>()
        .sqrt()
}

fn dot_wide<A: AsF32, B: AsF32>(a: &[A], b: &[B]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(a, b)| a.as_f32() as f64 * b.as_f32() as f64)
        .sum()
}

fn cosine_wide<A: AsF32, B: AsF32>(a: &[A], b: &[B]) -> f64 {
    let na = norm(a);
    let nb = norm(b);
    if na < f32::EPSILON as f64 || nb < f32::EPSILON as f64 {
        1.0
    } else {
        1.0 - (dot_wide(a, b) / (na * nb)).clamp(-1.0, 1.0)
    }
}

/// Query-side cosine norm is computed once; node norms are immutable.
pub(crate) struct PreparedQuery<'a> {
    pub values: &'a [f32],
    metric: DistanceMetric,
    norm: f64,
}

impl<'a> PreparedQuery<'a> {
    pub fn new(metric: DistanceMetric, values: &'a [f32]) -> Self {
        Self {
            metric,
            values,
            norm: if metric == DistanceMetric::Cosine {
                norm(values)
            } else {
                0.0
            },
        }
    }
    pub fn compute(&self, vector: &[bf16], node_norm: f64) -> Result<f32, HnswError> {
        if self.metric != DistanceMetric::Cosine {
            return self.metric.validated(self.values, vector);
        }
        cosine_with_norms(self.values, self.norm, vector, node_norm)
    }
}

fn stored_distance(value: f32) -> Result<f32, HnswError> {
    if bf16::from_f32(value).is_finite() {
        Ok(value)
    } else {
        Err(numeric_error(
            "distance cannot be represented by the stored edge format",
        ))
    }
}

fn cosine_with_norms<A: AsF32, B: AsF32>(
    a: &[A],
    norm_a: f64,
    b: &[B],
    norm_b: f64,
) -> Result<f32, HnswError> {
    if norm_a < f32::EPSILON as f64 || norm_b < f32::EPSILON as f64 {
        return Ok(1.0);
    }
    let dot = -inner_product(a, b);
    let dot = if dot.is_finite() {
        dot as f64
    } else {
        dot_wide(a, b)
    };
    finite_distance(1.0 - (dot / (norm_a * norm_b)).clamp(-1.0, 1.0))
}

#[inline]
fn inner_product<A: AsF32, B: AsF32>(a: &[A], b: &[B]) -> f32 {
    let mut acc = [0.0f32; LANES];
    let (chunks_a, remainder_a) = a.as_chunks::<LANES>();
    let (chunks_b, remainder_b) = b.as_chunks::<LANES>();
    for (ca, cb) in chunks_a.iter().zip(chunks_b) {
        for i in 0..LANES {
            acc[i] += ca[i].as_f32() * cb[i].as_f32();
        }
    }
    let mut dot: f32 = acc.iter().sum();
    for (&x, &y) in remainder_a.iter().zip(remainder_b) {
        dot += x.as_f32() * y.as_f32();
    }
    -dot
}

#[inline]
fn manhattan_distance<A: AsF32, B: AsF32>(a: &[A], b: &[B]) -> f32 {
    let mut acc = [0.0f32; LANES];
    let (chunks_a, remainder_a) = a.as_chunks::<LANES>();
    let (chunks_b, remainder_b) = b.as_chunks::<LANES>();
    for (ca, cb) in chunks_a.iter().zip(chunks_b) {
        for i in 0..LANES {
            acc[i] += (ca[i].as_f32() - cb[i].as_f32()).abs();
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for (&x, &y) in remainder_a.iter().zip(remainder_b) {
        sum += (x.as_f32() - y.as_f32()).abs();
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layer_distribution() {
        let lg = LayerGen::new(10, 16);
        let mut random = rand::rngs::StdRng::seed_from_u64(42);
        let mut counts = [0; 16];

        // Sample many layers and check the empirical distribution.
        const SAMPLES: usize = 100_000;
        let mut current_max_layer = 0;
        for _ in 0..SAMPLES {
            let level = lg.generate_with(current_max_layer, &mut random);
            current_max_layer = level.max(current_max_layer);
            counts[level as usize] += 1;
        }
        println!("Max layer: {current_max_layer}");

        // Test the populated buckets with a six-sigma tolerance. Sparse tails
        // need not be monotone in any finite random sample.
        for (layer, &count) in counts.iter().enumerate().take(4) {
            let expected = SAMPLES as f64 * 0.9 * 0.1_f64.powi(layer as i32);
            assert!((count as f64 - expected).abs() <= 6.0 * expected.sqrt() + 2.0);
        }

        // The bottom layer should hold the majority of the samples.
        let bottom_ratio = counts[0] as f64 / SAMPLES as f64;
        println!("Bottom layer ratio: {bottom_ratio}");
        assert!(bottom_ratio >= 0.5);
    }

    #[test]
    fn test_distance_impl_vs_scalar() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);

        fn euclidean_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
            a.iter()
                .zip(b.iter())
                .map(|(x, y)| (x - y).powi(2))
                .sum::<f32>()
                .sqrt()
        }

        fn cosine_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
            let dot_product: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                return 1.0; // Cosine distance for zero vectors defaults to 1.0.
            }
            1.0 - (dot_product / (norm_a * norm_b))
        }

        fn inner_product_scalar(a: &[f32], b: &[f32]) -> f32 {
            -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
        }

        fn manhattan_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
            a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum()
        }

        // Build random reference vectors.
        let dims = 128;
        let mut v1: Vec<f32> = Vec::with_capacity(dims);
        let mut v2: Vec<f32> = Vec::with_capacity(dims);

        for _ in 0..dims {
            v1.push(rng.random::<f32>());
            v2.push(rng.random::<f32>());
        }

        // Euclidean.
        let impl_euclidean = euclidean_distance(&v1, &v2);
        let scalar_euclidean = euclidean_distance_scalar(&v1, &v2);
        assert!(
            (impl_euclidean - scalar_euclidean).abs() < 1e-4,
            "euclidean: impl={impl_euclidean}, scalar={scalar_euclidean}"
        );

        // Cosine.
        let impl_cosine = cosine_distance(&v1, &v2);
        let scalar_cosine = cosine_distance_scalar(&v1, &v2);
        assert!(
            (impl_cosine - scalar_cosine).abs() < 1e-4,
            "cosine: impl={impl_cosine}, scalar={scalar_cosine}"
        );

        // Inner product.
        let impl_inner = inner_product(&v1, &v2);
        let scalar_inner = inner_product_scalar(&v1, &v2);
        assert!(
            (impl_inner - scalar_inner).abs() < 1e-4,
            "inner: impl={impl_inner}, scalar={scalar_inner}"
        );

        // Manhattan.
        let impl_manhattan = manhattan_distance(&v1, &v2);
        let scalar_manhattan = manhattan_distance_scalar(&v1, &v2);
        assert!(
            (impl_manhattan - scalar_manhattan).abs() < 1e-4,
            "manhattan: impl={impl_manhattan}, scalar={scalar_manhattan}"
        );
    }

    #[test]
    fn test_compute_mixed_matches_bf16_for_exact_queries() {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        // Vector lengths that exercise both the unrolled body and the remainder.
        for dims in [3, 8, 17, 128] {
            // Build a query already representable in bf16, so `compute_mixed`
            // (f32 query) and `compute` (bf16 query) must agree exactly.
            let query_bf16: Vec<bf16> = (0..dims)
                .map(|_| bf16::from_f32(rng.random::<f32>()))
                .collect();
            let query_f32: Vec<f32> = query_bf16.iter().map(|v| v.to_f32()).collect();
            let stored: Vec<bf16> = (0..dims)
                .map(|_| bf16::from_f32(rng.random::<f32>()))
                .collect();

            for metric in [
                DistanceMetric::Euclidean,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
                DistanceMetric::Manhattan,
            ] {
                let mixed = metric.compute_mixed(&query_f32, &stored).unwrap();
                let bf16_only = metric.compute(&query_bf16, &stored).unwrap();
                assert_eq!(
                    mixed, bf16_only,
                    "metric {metric:?} dims {dims}: mixed={mixed}, bf16={bf16_only}"
                );
            }
        }

        assert!(matches!(
            DistanceMetric::Euclidean.compute_mixed(&[1.0, 2.0], &[bf16::from_f32(1.0)]),
            Err(HnswError::DimensionMismatch {
                expected: 2,
                got: 1,
                ..
            })
        ));
    }

    #[test]
    fn test_distance_metric_public_compute_and_zero_vector_edges() {
        let a = [1.0_f32, 2.0, 3.0];
        let b = [2.0_f32, 4.0, 6.0];

        assert!(DistanceMetric::Euclidean.compute_f32(&a, &b).unwrap() > 0.0);
        assert!(DistanceMetric::Cosine.compute_f32(&a, &b).unwrap() >= 0.0);
        assert_eq!(
            DistanceMetric::InnerProduct.compute_f32(&a, &b).unwrap(),
            -28.0
        );
        assert_eq!(DistanceMetric::Manhattan.compute_f32(&a, &b).unwrap(), 6.0);
        assert!(matches!(
            DistanceMetric::Euclidean.compute_f32(&a, &b[..2]),
            Err(HnswError::DimensionMismatch {
                expected: 3,
                got: 2,
                ..
            })
        ));
        assert_eq!(
            DistanceMetric::Cosine
                .compute_f32(&[0.0, 0.0], &[1.0, 0.0])
                .unwrap(),
            1.0
        );

        let a_bf16: Vec<bf16> = a.iter().copied().map(bf16::from_f32).collect();
        let b_bf16: Vec<bf16> = b.iter().copied().map(bf16::from_f32).collect();
        assert!(DistanceMetric::Euclidean.compute(&a_bf16, &b_bf16).unwrap() > 0.0);
        assert!(DistanceMetric::Cosine.compute(&a_bf16, &b_bf16).unwrap() >= 0.0);
        assert_eq!(
            DistanceMetric::InnerProduct
                .compute(&a_bf16, &b_bf16)
                .unwrap(),
            -28.0
        );
        assert_eq!(
            DistanceMetric::Manhattan.compute(&a_bf16, &b_bf16).unwrap(),
            6.0
        );
        assert!(matches!(
            DistanceMetric::Euclidean.compute(&a_bf16, &b_bf16[..2]),
            Err(HnswError::DimensionMismatch {
                expected: 3,
                got: 2,
                ..
            })
        ));
        assert_eq!(
            DistanceMetric::Cosine
                .compute(
                    &[bf16::from_f32(0.0), bf16::from_f32(0.0)],
                    &[bf16::from_f32(1.0), bf16::from_f32(0.0)],
                )
                .unwrap(),
            1.0
        );
    }
}
