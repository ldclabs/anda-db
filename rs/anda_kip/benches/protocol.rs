use anda_kip::{
    Command, CommandType, Executor, IngestContext, IngestEvidence, Json, Operation,
    PreparedRequest, Request, Response, execute_request, parse_kip,
};
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
// Compare the old server preparation/classification path with the shared
// prepared AST path. This isolates transport/SDK CPU work, not database I/O.
struct Noop;
#[async_trait::async_trait]
impl Executor for Noop {
    async fn execute(&self, _: Command, _: &Request, _: &Operation) -> Response {
        Response::ok(Json::Null)
    }
}

fn server_preparation(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("server_preparation");
    for ingest in [false, true] {
        let mut request = Request::single(format!(
            "MUTATE {{{}}}",
            (0..100)
                .map(|i| format!("CREATE CONCEPT ?c{i} {{}}\n"))
                .collect::<String>()
        ));
        if ingest {
            request.ingest = Some(IngestContext {
                evidence: vec![IngestEvidence {
                    key: "source".into(),
                    evidence_class: "document".into(),
                    payload: Some(Json::String("content ".repeat(8192))),
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
        let value = serde_json::to_value(request).unwrap();
        let workload = if ingest { "ingest_64k" } else { "plain" };
        group.bench_with_input(
            BenchmarkId::new("previous", workload),
            &value,
            |b, value| {
                b.iter(|| {
                    runtime.block_on(async {
                        let request = Request::from_value(black_box(value.clone())).unwrap();
                        for op in &request.operations {
                            black_box(CommandType::from(&op.parse().unwrap()));
                        }
                        black_box(execute_request(&Noop, &request).await)
                    })
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("prepared", workload),
            &value,
            |b, value| {
                b.iter(|| {
                    runtime.block_on(async {
                        let request =
                            PreparedRequest::from_value(black_box(value.clone())).unwrap();
                        for op in request.operations() {
                            black_box(CommandType::from(op.as_ref().unwrap()));
                        }
                        black_box(request.execute(&Noop).await)
                    })
                });
            },
        );
    }
    group.finish();
}
criterion_group!(benches, protocol, server_preparation);
criterion_main!(benches);
