//! Quantified recall tests for the HNSW index.
//!
//! HNSW is an *approximate* nearest-neighbour structure: returning results is
//! not the same as returning good results, so correctness must be asserted
//! statistically. These tests build indexes over deterministic random
//! vectors, compute exact ground truth by brute force over the same
//! bf16-rounded data, and assert `recall@k` stays above a floor — before and
//! after deletions and a persistence round-trip.
//!
//! A hit is counted with the standard epsilon tolerance used by
//! ann-benchmarks: a returned neighbour also counts if its true distance is
//! within a tiny factor of the k-th ground-truth distance, so exact ties at
//! the boundary (common with bf16 rounding) are not punished.

use anda_db_hnsw::{DistanceMetric, HnswConfig, HnswIndex, half::bf16};
use std::collections::BTreeMap;

/// Deterministic SplitMix64; keeps the test independent of `rand` versions.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    /// Uniform f32 in [0, 1).
    fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }

    /// A vector rounded through bf16, exactly as the index stores it.
    fn next_vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|_| bf16::from_f32(self.next_f32()).to_f32())
            .collect()
    }
}

fn distance(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        DistanceMetric::Euclidean => a
            .iter()
            .zip(b)
            .map(|(x, y)| (x - y) * (x - y))
            .sum::<f32>()
            .sqrt(),
        DistanceMetric::Cosine => {
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if na < f32::EPSILON || nb < f32::EPSILON {
                1.0
            } else {
                1.0 - dot / (na * nb)
            }
        }
        DistanceMetric::InnerProduct => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
        DistanceMetric::Manhattan => a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum(),
    }
}

/// Exact k nearest neighbours by brute force; returns ids and the k-th distance.
fn ground_truth(
    metric: DistanceMetric,
    data: &BTreeMap<u64, Vec<f32>>,
    query: &[f32],
    k: usize,
) -> (Vec<u64>, f32) {
    let mut scored: Vec<(u64, f32)> = data
        .iter()
        .map(|(id, v)| (*id, distance(metric, query, v)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    scored.truncate(k);
    let kth = scored.last().map(|(_, d)| *d).unwrap_or(0.0);
    (scored.into_iter().map(|(id, _)| id).collect(), kth)
}

/// recall@k of one result list, with epsilon tolerance for boundary ties.
fn recall_at_k(
    metric: DistanceMetric,
    data: &BTreeMap<u64, Vec<f32>>,
    query: &[f32],
    results: &[(u64, f32)],
    k: usize,
) -> f64 {
    let (truth, kth) = ground_truth(metric, data, query, k);
    let threshold = kth * 1.001 + 1e-6;
    let hits = results
        .iter()
        .take(k)
        .filter(|(id, _)| {
            truth.contains(id)
                || data
                    .get(id)
                    .is_some_and(|v| distance(metric, query, v) <= threshold)
        })
        .count();
    hits as f64 / k as f64
}

struct Bench {
    index: HnswIndex,
    data: BTreeMap<u64, Vec<f32>>,
    queries: Vec<Vec<f32>>,
    metric: DistanceMetric,
    k: usize,
}

impl Bench {
    fn build(metric: DistanceMetric, n: usize, dim: usize, num_queries: usize, seed: u64) -> Self {
        let index = HnswIndex::new(
            "recall".to_string(),
            Some(HnswConfig {
                dimension: dim,
                distance_metric: metric,
                ..Default::default()
            }),
        );
        let mut rng = SplitMix64(seed);
        let mut data = BTreeMap::new();
        for id in 1..=(n as u64) {
            let v = rng.next_vector(dim);
            index.insert_f32(id, v.clone(), id).expect("insert failed");
            data.insert(id, v);
        }
        let queries = (0..num_queries).map(|_| rng.next_vector(dim)).collect();
        Bench {
            index,
            data,
            queries,
            metric,
            k: 10,
        }
    }

    /// Average and minimum recall@k over all queries.
    fn measure(&self, index: &HnswIndex) -> (f64, f64) {
        let mut total = 0.0;
        let mut min: f64 = 1.0;
        for query in &self.queries {
            let results = index.search_f32(query, self.k).expect("search failed");
            assert!(
                results.len() <= self.k,
                "search returned more than top_k results"
            );
            for (id, dist) in &results {
                assert!(dist.is_finite(), "non-finite distance for doc {id}");
                assert!(self.data.contains_key(id), "ghost doc {id} in results");
            }
            let r = recall_at_k(self.metric, &self.data, query, &results, self.k);
            total += r;
            min = min.min(r);
        }
        (total / self.queries.len() as f64, min)
    }
}

#[test]
fn euclidean_recall_at_10_meets_floor() {
    let bench = Bench::build(DistanceMetric::Euclidean, 1000, 32, 50, 42);
    let (avg, min) = bench.measure(&bench.index);
    println!("euclidean: avg recall@10 = {avg:.4}, min = {min:.4}");
    assert!(avg >= 0.95, "average recall@10 too low: {avg:.4}");
    assert!(min >= 0.60, "worst-case recall@10 too low: {min:.4}");
}

#[test]
fn cosine_recall_at_10_meets_floor() {
    let bench = Bench::build(DistanceMetric::Cosine, 800, 24, 40, 7);
    let (avg, min) = bench.measure(&bench.index);
    println!("cosine: avg recall@10 = {avg:.4}, min = {min:.4}");
    assert!(avg >= 0.95, "average recall@10 too low: {avg:.4}");
    assert!(min >= 0.60, "worst-case recall@10 too low: {min:.4}");
}

/// Deleting a fifth of the corpus must not poison the survivors' graph, and
/// removed ids must never come back from a search.
#[test]
fn recall_survives_deletions() {
    let mut bench = Bench::build(DistanceMetric::Euclidean, 1000, 32, 50, 99);

    let removed: Vec<u64> = (1..=1000u64).filter(|id| id % 5 == 0).collect();
    for id in &removed {
        assert!(
            bench.index.remove(*id, 2_000),
            "remove({id}) returned false"
        );
        bench.data.remove(id);
    }

    for query in &bench.queries {
        let results = bench
            .index
            .search_f32(query, bench.k)
            .expect("search failed");
        for (id, _) in &results {
            assert!(
                !removed.contains(id),
                "removed doc {id} still returned by search"
            );
        }
    }

    let (avg, min) = bench.measure(&bench.index);
    println!("after deletions: avg recall@10 = {avg:.4}, min = {min:.4}");
    assert!(
        avg >= 0.90,
        "average recall@10 after deletions too low: {avg:.4}"
    );
    assert!(
        min >= 0.50,
        "worst-case recall@10 after deletions too low: {min:.4}"
    );
}

/// A flush/load round-trip must preserve retrieval quality.
#[tokio::test]
async fn recall_survives_persistence_round_trip() {
    let bench = Bench::build(DistanceMetric::Euclidean, 600, 16, 30, 1234);
    let (avg_before, _) = bench.measure(&bench.index);

    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut nodes: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    bench
        .index
        .flush(&mut metadata, &mut ids, 5_000, async |id, data| {
            nodes.insert(id, data.to_vec());
            Ok(true)
        })
        .await
        .expect("flush failed");

    let reloaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        Ok(nodes.get(&id).cloned())
    })
    .await
    .expect("load_all failed");

    assert_eq!(reloaded.len(), bench.index.len());
    let (avg_after, min_after) = bench.measure(&reloaded);
    println!(
        "round-trip: avg before = {avg_before:.4}, after = {avg_after:.4}, min after = {min_after:.4}"
    );
    assert!(
        avg_after >= 0.95,
        "average recall@10 after reload too low: {avg_after:.4}"
    );
    assert!(
        (avg_before - avg_after).abs() <= 0.02,
        "reload changed retrieval quality: before {avg_before:.4}, after {avg_after:.4}"
    );
}
