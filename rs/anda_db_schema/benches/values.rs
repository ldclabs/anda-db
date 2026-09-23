use anda_db_schema::{Cbor, Document, DocumentOwned, Fe, Ft, Fv, Schema, bf16};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, hint::black_box, sync::Arc};

fn values(c: &mut Criterion) {
    let cases = [
        ("scalar", Ft::U64, Fv::U64(42)),
        ("bytes4096", Ft::Bytes, Fv::Bytes(vec![7; 4096])),
        (
            "byte_array4096",
            Ft::Bytes,
            Fv::Array((0..4096).map(|i| Fv::U64(i % 256)).collect()),
        ),
        (
            "u64_array4096",
            Ft::Array(vec![Ft::U64]),
            Fv::Array((0..4096).map(Fv::U64).collect()),
        ),
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
        let value = schema.get_field("value").unwrap().coerce(value).unwrap();
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

#[derive(Clone, Serialize, Deserialize)]
struct Row<T> {
    _id: u64,
    payload: T,
}

// Compare against the common byte-stream path without changing the public
// reader. Current cbor2 supports Bytes -> sequence in both decoders, but a
// value tree can expand numeric/vector payloads substantially.
fn read_through_value<T: DeserializeOwned>(doc: Document) -> Row<T> {
    let schema = doc.schema().clone();
    let mut fields = DocumentOwned::from(doc).fields;
    let mut named = Vec::with_capacity(fields.len());
    for field in schema.iter() {
        if let Some(value) = fields.remove(&field.idx()) {
            named.push((
                Cbor::Text(field.name().into()),
                value.try_into_cbor().unwrap(),
            ));
        }
    }
    Cbor::Map(named).deserialized().unwrap()
}

fn document_case<T: Clone + Serialize + DeserializeOwned>(
    c: &mut Criterion,
    name: &str,
    ft: Ft,
    payload: T,
) {
    let mut builder = Schema::builder();
    builder
        .add_field(Fe::new("payload".into(), ft).unwrap())
        .unwrap();
    let schema = Arc::new(builder.build().unwrap());
    let row = Row { _id: 1, payload };
    let doc = Document::try_from(schema.clone(), &row).unwrap();
    let mut wire = Vec::new();
    cbor2::to_writer(&doc, &mut wire).unwrap();

    c.bench_function(&format!("create/{name}"), |b| {
        b.iter(|| black_box(Document::try_from(schema.clone(), black_box(&row)).unwrap()))
    });
    c.bench_function(&format!("typed_update/{name}"), |b| {
        b.iter_batched(
            || doc.clone(),
            |mut doc| {
                doc.set_field_as("payload", black_box(&row.payload))
                    .unwrap();
                black_box(doc)
            },
            BatchSize::SmallInput,
        )
    });
    c.bench_function(&format!("decode_materialize/{name}"), |b| {
        b.iter(|| {
            let raw: DocumentOwned = cbor2::from_reader(black_box(wire.as_slice())).unwrap();
            black_box(Document::try_from_doc(schema.clone(), raw).unwrap())
        })
    });
    c.bench_function(&format!("typed_read_stream/{name}"), |b| {
        b.iter_batched(
            || doc.clone(),
            |doc| black_box(doc.try_into::<Row<T>>().unwrap()),
            BatchSize::SmallInput,
        )
    });
    c.bench_function(&format!("typed_read_value_tree/{name}"), |b| {
        b.iter_batched(
            || doc.clone(),
            |doc| black_box(read_through_value::<T>(doc)),
            BatchSize::SmallInput,
        )
    });
}

fn documents(c: &mut Criterion) {
    document_case(c, "scalar", Ft::U64, 42u64);
    document_case(c, "bytes64k", Ft::Bytes, vec![7u8; 65_536]);
    document_case(c, "vector1536", Ft::Vector, vec![bf16::from_f32(0.5); 1536]);
    document_case(
        c,
        "u64_array4096",
        Ft::Array(vec![Ft::U64]),
        (0..4096u64).collect::<Vec<_>>(),
    );
    document_case(
        c,
        "json100",
        Ft::Json,
        serde_json::json!({
            "items": (0..100).map(|i| format!("value-{i}")).collect::<Vec<_>>()
        }),
    );
}

fn json_encoding(c: &mut Criterion) {
    for (name, value) in [
        ("bytes64k", Fv::Bytes(vec![7; 65_536])),
        (
            "escaped_text64k",
            Fv::Text(format!("txt:{}", "x".repeat(65_536))),
        ),
        (
            "nested_prefixes",
            Fv::Json(serde_json::json!({
                "b64:key": (0..100).map(|i| format!("txt:value-{i}")).collect::<Vec<_>>()
            })),
        ),
    ] {
        c.bench_function(&format!("json_encode/{name}"), |b| {
            let mut output = Vec::new();
            b.iter(|| {
                output.clear();
                serde_json::to_writer(&mut output, black_box(&value)).unwrap();
                black_box(&output);
            })
        });
    }
}

criterion_group!(benches, values, documents, json_encoding);
criterion_main!(benches);
