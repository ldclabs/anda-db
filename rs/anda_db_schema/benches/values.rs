use anda_db_schema::{Document, DocumentOwned, Fe, Ft, Fv, Schema, bf16};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::{collections::BTreeMap, hint::black_box, sync::Arc};

fn values(c: &mut Criterion) {
    let cases = [
        ("scalar", Ft::U64, Fv::U64(42)),
        (
            "vector1536",
            Ft::Vector,
            Fv::Vector(vec![bf16::from_f32(0.5); 1536]),
        ),
        (
            "json100",
            Ft::Json,
            Fv::Json(serde_json::json!(
                (0..100).map(|i| format!("value-{i}")).collect::<Vec<_>>()
            )),
        ),
        (
            "text_array100",
            Ft::Array(vec![Ft::Text]),
            Fv::Array((0..100).map(|i| Fv::Text(format!("value-{i}"))).collect()),
        ),
    ];
    for (name, ft, value) in cases {
        let entry = Fe::new("value".into(), ft).unwrap();
        c.bench_function(&format!("coerce/{name}"), |b| {
            b.iter_batched(
                || value.clone(),
                |v| black_box(entry.coerce(black_box(v)).unwrap()),
                BatchSize::SmallInput,
            )
        });
        let mut builder = Schema::builder();
        builder.add_field(entry).unwrap();
        let schema = Arc::new(builder.build().unwrap());
        let owned = DocumentOwned {
            fields: BTreeMap::from([(0, Fv::U64(1)), (1, value)]),
        };
        let mut cbor = Vec::new();
        cbor2::to_writer(&owned, &mut cbor).unwrap();
        c.bench_function(&format!("materialize/{name}"), |b| {
            b.iter_batched(
                || cbor2::from_reader::<DocumentOwned, _>(cbor.as_slice()).unwrap(),
                |v| black_box(Document::try_from_doc(schema.clone(), black_box(v)).unwrap()),
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, values);
criterion_main!(benches);
