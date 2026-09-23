use anda_kip::{IngestContext, IngestEvidence, Json, Request, parse_kip};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

fn protocol(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_kml");
    for count in [100, 500, 1000, 2000] {
        let text = format!(
            "MUTATE {{{}}}",
            (0..count)
                .map(|i| format!("CREATE CONCEPT ?c{i} {{}}\n"))
                .collect::<String>()
        );
        group.bench_with_input(BenchmarkId::from_parameter(count), &text, |b, text| {
            b.iter(|| black_box(parse_kip(black_box(text)).unwrap()));
        });
    }
    group.finish();
    c.bench_function("parse_kql", |b| {
        b.iter(|| {
            parse_kip(black_box(
                r#"FIND(?c.name) WHERE {?c CONCEPT {type: "Person"}} LIMIT 10"#,
            ))
            .unwrap()
        })
    });
    let mut request = Request::single("CREATE CONCEPT ?c {}");
    request.ingest = Some(IngestContext {
        evidence: vec![IngestEvidence {
            key: "source".into(),
            evidence_class: "document".into(),
            payload: Some(Json::String("content ".repeat(8192))),
            ..Default::default()
        }],
        ..Default::default()
    });
    c.bench_function("prepare_ingest_64k", |b| {
        b.iter(|| request.parse_operations().unwrap())
    });
}
criterion_group!(benches, protocol);
criterion_main!(benches);
