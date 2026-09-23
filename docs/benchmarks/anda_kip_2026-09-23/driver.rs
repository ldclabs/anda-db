use anda_kip::*;
use std::{hint::black_box, time::Instant};
fn run(label: &str, mut f: impl FnMut(), count: usize) {
    for _ in 0..3 {
        f();
    }
    let start = Instant::now();
    for _ in 0..count {
        f();
    }
    println!(
        "{label},{:.3}",
        start.elapsed().as_secs_f64() * 1e6 / count as f64
    );
}
fn main() {
    for n in [100, 500, 1000, 2000] {
        let source = format!(
            "MUTATE {{{}}}",
            (0..n)
                .map(|i| format!("CREATE CONCEPT ?c{i} {{}}\n"))
                .collect::<String>()
        );
        run(
            &format!("kml_{n}"),
            || {
                black_box(parse_kip(black_box(&source)).unwrap());
            },
            20,
        );
    }
    let source = r#"FIND(?c.name) WHERE {?c CONCEPT {type:"Person"}} LIMIT 10"#;
    run(
        "kql",
        || {
            black_box(parse_kip(black_box(source)).unwrap());
        },
        20000,
    );
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
    run(
        "prepare_ingest_64k",
        || {
            black_box(request.parse_operations().unwrap());
        },
        20000,
    );
}
