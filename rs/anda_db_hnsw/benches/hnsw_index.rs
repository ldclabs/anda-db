//! Opt-in repeatable benchmark, CSV output. See benches/README.md.
mod support;
use anda_db_hnsw::{
    BoxError, DistanceMetric, FlushOptions, HnswConfig, HnswIndex, SelectNeighborsStrategy,
    half::bf16,
};
use std::{
    collections::BTreeMap,
    env,
    hint::black_box,
    sync::Mutex,
    time::{Duration, Instant},
};
use support::{CountingAllocator, Snapshot};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct Random(u64);
impl Random {
    fn next(&mut self) -> f32 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^= z >> 31;
        (z >> 40) as f32 / (1u64 << 24) as f32 * 2.0 - 1.0
    }
}
struct Options {
    n: usize,
    dim: usize,
    queries: usize,
    recall_queries: usize,
    seed: u64,
    metric: DistanceMetric,
    strategy: SelectNeighborsStrategy,
    m: u8,
    ef: usize,
    ef_build: usize,
    layers: u8,
    reconnect: bool,
    distribution: String,
    data: Option<String>,
    upload: usize,
    io_ms: u64,
}
impl Options {
    fn parse() -> Result<Self, String> {
        let args: Vec<_> = env::args().skip(1).collect();
        let value = |key: &str, default: &str| {
            args.windows(2)
                .find(|a| a[0] == key)
                .map_or(default.to_string(), |a| a[1].clone())
        };
        let number = |key, default| {
            value(key, default)
                .parse::<usize>()
                .map_err(|_| format!("Invalid {key}"))
        };
        let metric = match value("--metric", "euclidean").as_str() {
            "euclidean" => DistanceMetric::Euclidean,
            "cosine" => DistanceMetric::Cosine,
            "inner" => DistanceMetric::InnerProduct,
            "manhattan" => DistanceMetric::Manhattan,
            _ => return Err("Unknown metric".into()),
        };
        let strategy = match value("--strategy", "heuristic").as_str() {
            "simple" => SelectNeighborsStrategy::Simple,
            "heuristic" => SelectNeighborsStrategy::Heuristic,
            _ => return Err("Unknown strategy".into()),
        };
        Ok(Self {
            n: number("--n", "1000")?,
            dim: number("--dim", "128")?,
            queries: number("--queries", "100")?,
            recall_queries: number("--recall-queries", "20")?,
            seed: number("--seed", "42")? as u64,
            metric,
            strategy,
            m: u8::try_from(number("--m", "32")?).map_err(|_| "M exceeds u8")?,
            ef: number("--ef", "50")?,
            ef_build: number("--ef-construction", "200")?,
            layers: u8::try_from(number("--layers", "16")?)
                .map_err(|_| "layer count exceeds u8")?,
            reconnect: value("--reconnect", "false") == "true",
            distribution: value("--distribution", "uniform"),
            data: args
                .windows(2)
                .find(|a| a[0] == "--data")
                .map(|a| a[1].clone()),
            upload: number("--upload-concurrency", "8")?,
            io_ms: number("--io-ms", "0")? as u64,
        })
    }
    fn config(&self) -> HnswConfig {
        HnswConfig {
            dimension: self.dim,
            distance_metric: self.metric,
            select_neighbors_strategy: self.strategy,
            max_connections: self.m,
            ef_construction: self.ef_build,
            ef_search: self.ef,
            max_layers: self.layers,
            reconnect_on_delete: self.reconnect,
            ..Default::default()
        }
    }
}
fn rounded(v: f32) -> f32 {
    bf16::from_f32(v).to_f32()
}
fn corpus(options: &mut Options) -> Result<Vec<Vec<f32>>, BoxError> {
    if let Some(path) = &options.data {
        let text = std::fs::read_to_string(path)?;
        let rows: Vec<Vec<f32>> = text
            .lines()
            .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
            .map(|line| {
                line.split(|c: char| c == ',' || c.is_whitespace())
                    .filter(|v| !v.is_empty())
                    .map(|v| v.parse::<f32>().map(rounded))
                    .collect()
            })
            .collect::<Result<_, _>>()?;
        options.n = rows.len();
        options.dim = rows.first().ok_or("empty dataset")?.len();
        if rows.iter().any(|r| r.len() != options.dim) {
            return Err("inconsistent row dimensions".into());
        }
        return Ok(rows);
    }
    if options.n < 2 || options.dim == 0 {
        return Err("n>=2 and dim>=1 are required".into());
    }
    let mut random = Random(options.seed);
    let centers: Vec<Vec<f32>> = (0..8)
        .map(|_| (0..options.dim).map(|_| random.next()).collect())
        .collect();
    (0..options.n)
        .map(|i| {
            (0..options.dim)
                .map(|d| {
                    let v = match options.distribution.as_str() {
                        "uniform" => random.next(),
                        "clustered" => centers[i % 8][d] + random.next() * 0.08,
                        "duplicates" => centers[i % 8][d],
                        _ => return Err("Unknown distribution".into()),
                    };
                    Ok(rounded(v))
                })
                .collect()
        })
        .collect()
}
fn create_index(config: HnswConfig, seed: u64) -> HnswIndex {
    HnswIndex::try_new_seeded("benchmark".into(), Some(config), seed).unwrap()
}
fn report(
    name: &str,
    count: usize,
    elapsed: Duration,
    mut samples: Vec<Duration>,
    start: Snapshot,
    bytes: usize,
    recall: f64,
) {
    let end = Snapshot::read();
    samples.sort_unstable();
    let percentile = |p: usize| {
        samples
            .get(samples.len().saturating_sub(1) * p / 100)
            .map_or(0.0, |d| d.as_secs_f64() * 1e6)
    };
    let throughput = count as f64 / elapsed.as_secs_f64().max(1e-12);
    println!(
        "{name},{count},{:.3},{:.3},{:.3},{:.3},{throughput:.1},{},{},{},{bytes},{recall:.5}",
        elapsed.as_secs_f64() * 1e3,
        percentile(50),
        percentile(95),
        percentile(99),
        end.allocations - start.allocations,
        end.live as i128 - start.live as i128,
        end.peak
    );
}
fn timed<F: FnMut(usize)>(name: &str, count: usize, mut f: F) {
    let mut samples = Vec::with_capacity(count);
    let allocation = Snapshot::start();
    let start = Instant::now();
    for i in 0..count {
        let sample = Instant::now();
        f(i);
        samples.push(sample.elapsed());
    }
    report(
        name,
        count,
        start.elapsed(),
        samples,
        allocation,
        0,
        f64::NAN,
    );
}
fn exact(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f64 {
    let dot = || {
        a.iter()
            .zip(b)
            .map(|(a, b)| *a as f64 * *b as f64)
            .sum::<f64>()
    };
    match metric {
        DistanceMetric::Euclidean => a
            .iter()
            .zip(b)
            .map(|(a, b)| {
                let d = *a as f64 - *b as f64;
                d * d
            })
            .sum::<f64>()
            .sqrt(),
        DistanceMetric::Manhattan => a
            .iter()
            .zip(b)
            .map(|(a, b)| (*a as f64 - *b as f64).abs())
            .sum(),
        DistanceMetric::InnerProduct => -dot(),
        DistanceMetric::Cosine => {
            let na = a.iter().map(|v| (*v as f64).powi(2)).sum::<f64>().sqrt();
            let nb = b.iter().map(|v| (*v as f64).powi(2)).sum::<f64>().sqrt();
            if na < f32::EPSILON as f64 || nb < f32::EPSILON as f64 {
                1.0
            } else {
                1.0 - (dot() / (na * nb)).clamp(-1.0, 1.0)
            }
        }
    }
}
fn recall(
    index: &HnswIndex,
    data: &[Vec<f32>],
    active: &[bool],
    queries: &[Vec<f32>],
    options: &Options,
) -> f64 {
    let mut total = 0.0;
    let count = options.recall_queries.min(queries.len());
    for query in queries.iter().take(count) {
        let mut truth: Vec<_> = data
            .iter()
            .enumerate()
            .filter(|(i, _)| active[*i])
            .map(|(id, v)| (id as u64, exact(options.metric, query, v)))
            .collect();
        truth.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        let k = 10.min(truth.len());
        let kth = truth[k - 1].1;
        let tolerance = kth.abs() * 0.001 + 1e-6;
        let hits = index
            .search_f32(query, k)
            .unwrap()
            .iter()
            .filter(|(id, _)| {
                active[*id as usize]
                    && exact(options.metric, query, &data[*id as usize]) <= kth + tolerance
            })
            .count();
        total += hits as f64 / k as f64;
    }
    if count == 0 {
        f64::NAN
    } else {
        total / count as f64
    }
}
fn query_phase(
    name: &str,
    index: &HnswIndex,
    data: &[Vec<f32>],
    active: &[bool],
    queries: &[Vec<f32>],
    options: &Options,
) {
    let score = recall(index, data, active, queries, options);
    let mut samples = Vec::with_capacity(queries.len());
    let allocation = Snapshot::start();
    let start = Instant::now();
    for query in queries {
        let sample = Instant::now();
        black_box(index.search_f32(query, 10).unwrap());
        samples.push(sample.elapsed());
    }
    report(
        name,
        queries.len(),
        start.elapsed(),
        samples,
        allocation,
        0,
        score,
    );
}
#[derive(Default)]
struct Disk {
    metadata: Mutex<Vec<u8>>,
    ids: Mutex<Vec<u8>>,
    nodes: Mutex<BTreeMap<u64, Vec<u8>>>,
}
async fn flush(index: &HnswIndex, disk: &Disk, options: &Options) {
    index
        .flush_with_options(
            1,
            FlushOptions {
                node_concurrency: options.upload,
                ..Default::default()
            },
            |id, bytes| async move {
                if options.io_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(options.io_ms)).await;
                }
                disk.nodes.lock().unwrap().insert(id, bytes);
                Ok(true)
            },
            |bytes| async {
                *disk.ids.lock().unwrap() = bytes;
                Ok(())
            },
            |bytes| async {
                *disk.metadata.lock().unwrap() = bytes;
                Ok(())
            },
        )
        .await
        .unwrap();
}
async fn run(mut options: Options) -> Result<(), BoxError> {
    let mut data = corpus(&mut options)?;
    options.config().validate("benchmark")?;
    if options.queries == 0 {
        return Err("queries must be positive".into());
    }
    let mut random = Random(options.seed.wrapping_add(1));
    let queries: Vec<Vec<f32>> = (0..options.queries)
        .map(|i| {
            if options.data.is_some() || options.distribution != "uniform" {
                data[i % data.len()]
                    .iter()
                    .map(|v| *v + random.next() * 0.01)
                    .collect()
            } else {
                (0..options.dim).map(|_| random.next()).collect()
            }
        })
        .collect();
    let index = create_index(options.config(), options.seed);
    eprintln!(
        "n={} dim={} metric={:?} M={} ef={} efConstruction={} layers={} seed={} distribution={} strategy={:?} reconnect={} upload={} io_ms={}",
        options.n,
        options.dim,
        options.metric,
        options.m,
        options.ef,
        options.ef_build,
        options.layers,
        options.seed,
        options.distribution,
        options.strategy,
        options.reconnect,
        options.upload,
        options.io_ms
    );
    println!(
        "phase,operations,elapsed_ms,p50_us,p95_us,p99_us,ops_per_second,allocations,live_byte_delta,peak_live_bytes,persisted_bytes,recall_at_10"
    );
    let stored: Vec<_> = data[0].iter().map(|v| bf16::from_f32(*v)).collect();
    timed("distance_mixed", 10000, |i| {
        black_box(
            options
                .metric
                .compute_mixed(&queries[i % queries.len()], &stored)
                .unwrap(),
        );
    });
    timed("build", data.len(), |i| {
        index.insert_f32(i as u64, data[i].clone(), 0).unwrap()
    });
    let mut active = vec![true; data.len()];
    query_phase("query", &index, &data, &active, &queries, &options);

    let disk = Disk::default();
    let allocation = Snapshot::start();
    let start = Instant::now();
    flush(&index, &disk, &options).await;
    let elapsed = start.elapsed();
    let bytes = disk
        .nodes
        .lock()
        .unwrap()
        .values()
        .map(Vec::len)
        .sum::<usize>()
        + disk.ids.lock().unwrap().len()
        + disk.metadata.lock().unwrap().len();
    report(
        "flush",
        index.len(),
        elapsed,
        vec![elapsed],
        allocation,
        bytes,
        f64::NAN,
    );
    let metadata = disk.metadata.lock().unwrap().clone();
    let ids = disk.ids.lock().unwrap().clone();
    let allocation = Snapshot::start();
    let start = Instant::now();
    let loaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        if options.io_ms > 0 {
            tokio::time::sleep(Duration::from_millis(options.io_ms)).await;
        }
        Ok(disk.nodes.lock().unwrap().get(&id).cloned())
    })
    .await?;
    let elapsed = start.elapsed();
    report(
        "load",
        loaded.len(),
        elapsed,
        vec![elapsed],
        allocation,
        bytes,
        f64::NAN,
    );
    query_phase(
        "query_reloaded",
        &loaded,
        &data,
        &active,
        &queries,
        &options,
    );
    drop(loaded);
    drop(disk);

    let victims: Vec<_> = (1..data.len()).filter(|i| i % 5 == 0).collect();
    timed("remove", victims.len(), |i| {
        assert!(index.remove(victims[i] as u64, 1));
        active[victims[i]] = false;
    });
    query_phase("query_deleted", &index, &data, &active, &queries, &options);
    timed("reinsert", victims.len(), |i| {
        let id = victims[i];
        data[id]
            .iter_mut()
            .for_each(|v| *v = rounded(*v + random.next() * 0.01));
        index.insert_f32(id as u64, data[id].clone(), 2).unwrap();
        active[id] = true;
    });
    query_phase(
        "query_reinserted",
        &index,
        &data,
        &active,
        &queries,
        &options,
    );
    Ok(())
}
fn main() {
    if !env::args().any(|arg| arg == "--run") {
        eprintln!(
            "Opt-in benchmark: cargo bench -p anda_db_hnsw --bench hnsw_index -- --run (see benches/README.md)"
        );
        return;
    }
    let options = Options::parse().expect("valid benchmark options");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(options))
        .unwrap();
}
