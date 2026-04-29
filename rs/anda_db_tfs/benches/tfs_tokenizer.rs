use anda_db_tfs::{collect_tokens, default_tokenizer};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const SMALL_TEXT: &str = include_str!("../README.md");
const LARGE_TEXT: &str = include_str!("../../../Cargo.lock");

struct TestCase<'a> {
    name: &'a str,
    text: &'a str,
}

// cargo bench --bench tfs_tokenizer --features full -- --save-baseline initial
// cargo bench --bench tfs_tokenizer --features full -- --baseline initial
// cargo bench --bench tfs_tokenizer --features full
pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("tokenizer");
    group.sample_size(60);
    group.measurement_time(std::time::Duration::from_secs(6));

    let test_cases = vec![
        TestCase {
            name: "small",
            text: SMALL_TEXT,
        },
        TestCase {
            name: "large",
            text: LARGE_TEXT,
        },
    ];

    for case in &test_cases {
        println!("Benchmarking {}: {} bytes", case.name, case.text.len());

        group.throughput(Throughput::Bytes(case.text.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("serial", case.name),
            &case.text,
            |b, &text| {
                let mut tokenizer = default_tokenizer();
                b.iter(|| {
                    let tokens = collect_tokens(&mut tokenizer, black_box(text), None);
                    let token_count = tokens.len();
                    assert!(token_count > 0);
                    black_box(token_count);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
