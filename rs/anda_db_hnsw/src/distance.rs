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
    /// Returns [`HnswError::DimensionMismatch`] if `a.len() != b.len()`.
    pub fn compute(&self, a: &[bf16], b: &[bf16]) -> Result<f32, HnswError> {
        if a.len() != b.len() {
            return Err(HnswError::DimensionMismatch {
                name: "unknown".to_string(),
                expected: a.len(),
                got: b.len(),
            });
        }

        match self {
            DistanceMetric::Euclidean => Ok(euclidean_distance_bf16(a, b)),
            DistanceMetric::Cosine => Ok(cosine_distance_bf16(a, b)),
            DistanceMetric::InnerProduct => Ok(inner_product_bf16(a, b)),
            DistanceMetric::Manhattan => Ok(manhattan_distance_bf16(a, b)),
        }
    }

    /// Computes the distance between two `f32` vectors.
    ///
    /// Useful for callers that want to avoid the `bf16` round-trip at
    /// evaluation time (e.g. during offline evaluation).
    ///
    /// # Errors
    /// Returns [`HnswError::DimensionMismatch`] if `a.len() != b.len()`.
    pub fn compute_f32(&self, a: &[f32], b: &[f32]) -> Result<f32, HnswError> {
        if a.len() != b.len() {
            return Err(HnswError::DimensionMismatch {
                name: "unknown".to_string(),
                expected: a.len(),
                got: b.len(),
            });
        }

        match self {
            DistanceMetric::Euclidean => Ok(euclidean_distance_f32(a, b)),
            DistanceMetric::Cosine => Ok(cosine_distance_f32(a, b)),
            DistanceMetric::InnerProduct => Ok(inner_product_f32(a, b)),
            DistanceMetric::Manhattan => Ok(manhattan_distance_f32(a, b)),
        }
    }
}

/// Random layer generator for HNSW.
///
/// Draws a layer index from a truncated exponential distribution so that the
/// expected number of nodes at layer `ℓ` decays as `M^{-ℓ}`, where `M` is
/// [`HnswConfig::max_connections`]. This reproduces the behavior of the
/// reference HNSW paper.
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
        let mut r = rng();
        let val = r.sample(self.uniform);

        // Sample l = ⌊−ln(u) · scale⌋ from an exponential distribution.
        let level = (-val.ln() * self.scale).floor() as u8;

        // Clamp into the valid range; never skip more than one layer at a time.
        level.min(current_max_layer + 1).min(self.max_level - 1)
    }
}

#[inline]
fn euclidean_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        let d = x - y;
        sum += d * d;
    }
    sum.sqrt()
}

#[inline]
fn cosine_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a2 = 0.0f32;
    let mut norm_b2 = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        dot += x * y;
        norm_a2 += x * x;
        norm_b2 += y * y;
    }
    let norm_a = norm_a2.sqrt();
    let norm_b = norm_b2.sqrt();
    if norm_a < f32::EPSILON || norm_b < f32::EPSILON {
        return 1.0;
    }
    1.0 - (dot / (norm_a * norm_b))
}

#[inline]
fn inner_product_f32(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        dot += x * y;
    }
    -dot
}

#[inline]
fn manhattan_distance_f32(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        sum += (x - y).abs();
    }
    sum
}

#[inline]
fn euclidean_distance_bf16(a: &[bf16], b: &[bf16]) -> f32 {
    let mut sum = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        let d = x.to_f32() - y.to_f32();
        sum += d * d;
    }
    sum.sqrt()
}

#[inline]
fn cosine_distance_bf16(a: &[bf16], b: &[bf16]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a2 = 0.0f32;
    let mut norm_b2 = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        let xf = x.to_f32();
        let yf = y.to_f32();
        dot += xf * yf;
        norm_a2 += xf * xf;
        norm_b2 += yf * yf;
    }
    let norm_a = norm_a2.sqrt();
    let norm_b = norm_b2.sqrt();
    if norm_a < f32::EPSILON || norm_b < f32::EPSILON {
        return 1.0;
    }
    1.0 - (dot / (norm_a * norm_b))
}

#[inline]
fn inner_product_bf16(a: &[bf16], b: &[bf16]) -> f32 {
    let mut dot = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        dot += x.to_f32() * y.to_f32();
    }
    -dot
}

#[inline]
fn manhattan_distance_bf16(a: &[bf16], b: &[bf16]) -> f32 {
    let mut sum = 0.0f32;
    for (&x, &y) in a.iter().zip(b) {
        sum += (x.to_f32() - y.to_f32()).abs();
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layer_distribution() {
        let lg = LayerGen::new(10, 16);
        let mut counts = [0; 16];

        // Sample many layers and check the empirical distribution.
        const SAMPLES: usize = 100_000;
        let mut current_max_layer = 0;
        for _ in 0..SAMPLES {
            let level = lg.generate(current_max_layer);
            current_max_layer = level.max(current_max_layer);
            counts[level as usize] += 1;
        }
        println!("Max layer: {current_max_layer}");

        // The histogram must be monotonically non-increasing.
        for i in 1..16 {
            assert!(counts[i] <= counts[i - 1]);
        }

        // The bottom layer should hold the majority of the samples.
        let bottom_ratio = counts[0] as f64 / SAMPLES as f64;
        println!("Bottom layer ratio: {bottom_ratio}");
        assert!(bottom_ratio >= 0.5);
    }

    #[test]
    fn test_distance_impl_vs_scalar() {
        let mut rng = rand::rng();

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
        let impl_euclidean = euclidean_distance_f32(&v1, &v2);
        let scalar_euclidean = euclidean_distance_scalar(&v1, &v2);
        assert!(
            (impl_euclidean - scalar_euclidean).abs() < 1e-4,
            "euclidean: impl={impl_euclidean}, scalar={scalar_euclidean}"
        );

        // Cosine.
        let impl_cosine = cosine_distance_f32(&v1, &v2);
        let scalar_cosine = cosine_distance_scalar(&v1, &v2);
        assert!(
            (impl_cosine - scalar_cosine).abs() < 1e-4,
            "cosine: impl={impl_cosine}, scalar={scalar_cosine}"
        );

        // Inner product.
        let impl_inner = inner_product_f32(&v1, &v2);
        let scalar_inner = inner_product_scalar(&v1, &v2);
        assert!(
            (impl_inner - scalar_inner).abs() < 1e-4,
            "inner: impl={impl_inner}, scalar={scalar_inner}"
        );

        // Manhattan.
        let impl_manhattan = manhattan_distance_f32(&v1, &v2);
        let scalar_manhattan = manhattan_distance_scalar(&v1, &v2);
        assert!(
            (impl_manhattan - scalar_manhattan).abs() < 1e-4,
            "manhattan: impl={impl_manhattan}, scalar={scalar_manhattan}"
        );
    }
}
