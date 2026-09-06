//! Boundary regressions: exercise persisted data and the public API together.
use anda_db_schema::{
    Cbor, Document, DocumentOwned, Fe, FieldKey, Ft, Fv, Json, MAX_CONVERSION_DEPTH, Schema,
};
use std::{collections::BTreeMap, sync::Arc};

fn schema(ft: Ft, version: u64) -> Schema {
    let mut builder = Schema::builder();
    builder.with_version(version);
    builder
        .add_field(Fe::new("payload".into(), ft).unwrap())
        .unwrap();
    builder.build().unwrap()
}

fn map(fields: &[(&str, Ft)]) -> Ft {
    Ft::Map(
        fields
            .iter()
            .map(|(key, ft)| ((*key).into(), ft.clone()))
            .collect(),
    )
}

#[test]
fn finite_f32_json_roundtrip_preserves_bits() {
    let mut state = 0x12345678u32;
    for bits in [0, 0x80000000, 1, 0x470e2fd0, 0xfcbe64a0]
        .into_iter()
        .chain((0..200_000).map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state
        }))
    {
        let f = f32::from_bits(bits);
        if !f.is_finite() {
            continue;
        }
        let wire = serde_json::to_string(&Fv::F32(f)).unwrap();
        let mut value: Fv = serde_json::from_str(&wire).unwrap();
        Ft::F32.normalize(&mut value);
        Ft::F32
            .validate(&value)
            .unwrap_or_else(|err| panic!("{bits:08x}: {wire}: {err}"));
        assert_eq!(f32::try_from(value).unwrap().to_bits(), bits, "{wire}");
    }
}

#[test]
fn nested_key_cannot_be_reused_after_multiple_upgrades() {
    let first = schema(map(&[("keep", Ft::Bool), ("x", Ft::Text)]), 1);
    let stored = Document::try_from(
        Arc::new(first.clone()),
        &serde_json::json!({"_id":1,"payload":{"keep":true,"x":"old"}}),
    )
    .unwrap();
    let mut second = schema(map(&[("keep", Ft::Bool)]), 2);
    second.upgrade_with(&first).unwrap();
    let second: Schema = serde_json::from_value(serde_json::to_value(second).unwrap()).unwrap();
    assert!(Document::try_from_doc(Arc::new(second.clone()), stored.clone().into()).is_ok());
    for ft in [Ft::Text, Ft::U64] {
        let mut third = schema(
            map(&[("keep", Ft::Bool), ("x", Ft::Option(Box::new(ft)))]),
            3,
        );
        let before = serde_json::to_value(&third).unwrap();
        assert!(third.upgrade_with(&second).is_err());
        assert_eq!(serde_json::to_value(third).unwrap(), before);
    }
}

#[test]
fn legacy_schema_cannot_allocate_from_unknown_history() {
    let mut wire = serde_json::to_value(schema(Ft::Text, 1)).unwrap();
    wire.as_object_mut().unwrap().remove("next_idx");
    let old: Schema = serde_json::from_value(wire).unwrap();
    let mut b = Schema::builder();
    b.with_version(2);
    b.add_field(Fe::new("payload".into(), Ft::Text).unwrap())
        .unwrap();
    b.add_field(Fe::new("new_field".into(), Ft::Option(Box::new(Ft::Text))).unwrap())
        .unwrap();
    let mut new = b.build().unwrap();
    assert!(new.upgrade_with(&old).is_err());
}

#[test]
fn open_map_cannot_be_narrowed_without_rewriting_values() {
    let old = schema(map(&[]), 1);
    let mut new = schema(map(&[("x", Ft::Option(Box::new(Ft::U64)))]), 2);
    assert!(new.upgrade_with(&old).is_err());
    // Opening a fixed map would expose older keys that had been pruned by
    // the immediately preceding schema, bypassing their tombstones.
    let fixed = schema(map(&[("keep", Ft::Bool)]), 1);
    let mut opened = schema(map(&[]), 2);
    assert!(opened.upgrade_with(&fixed).is_err());
}

#[test]
fn builder_revalidates_deserialized_entries() {
    for (name, ft) in [
        ("Invalid Name!", serde_json::json!("Text")),
        ("valid", serde_json::json!({"Option":{"Option":"Text"}})),
    ] {
        let entry: Fe =
            serde_json::from_value(serde_json::json!({"n":name,"d":"","t":ft,"u":false,"i":0}))
                .unwrap();
        let mut builder = Schema::builder();
        assert!(builder.add_field(entry).is_err());
        assert_eq!(builder.build().unwrap().len(), 1);
    }
}

#[test]
fn json_fields_reject_non_json_shapes_on_read() {
    for value in [
        Fv::Bytes(vec![1]),
        Fv::Map(BTreeMap::from([(FieldKey::I64(1), Fv::U64(2))])),
        Fv::Array(vec![Fv::Bytes(vec![1])]),
    ] {
        assert!(Ft::Json.validate(&value).is_err());
        let owned = DocumentOwned {
            fields: BTreeMap::from([(0, Fv::U64(1)), (1, value)]),
        };
        assert!(Document::try_from_doc(Arc::new(schema(Ft::Json, 0)), owned).is_err());
    }
}

#[test]
fn required_nested_json_distinguishes_missing_from_null() {
    let ft = map(&[("value", Ft::Json)]);
    assert!(ft.extract(Cbor::Map(vec![])).is_err());
    assert!(ft.validate(&Fv::Map(BTreeMap::new())).is_err());
    let value = ft
        .extract(Cbor::Map(vec![(Cbor::Text("value".into()), Cbor::Null)]))
        .unwrap();
    ft.validate(&value).unwrap();
    assert_eq!(
        value.deserialized::<Json>().unwrap(),
        serde_json::json!({"value":null})
    );
}

#[test]
fn json_floats_never_silently_become_null() {
    for value in [Fv::F32(f32::INFINITY), Fv::F64(f64::NEG_INFINITY)] {
        assert!(serde_json::to_string(&value).is_err());
        let mut wire = Vec::new();
        cbor2::to_writer(&value, &mut wire).unwrap();
        let decoded: Fv = cbor2::from_reader(wire.as_slice()).unwrap();
        assert!(matches!(decoded, Fv::F64(f) if f.is_infinite()));
    }
}

#[test]
fn duplicate_document_indexes_and_type_keys_are_rejected() {
    assert!(serde_json::from_str::<DocumentOwned>(r#"{"f":{"0":1,"0":2}}"#).is_err());
    assert!(serde_json::from_str::<Ft>(r#"{"Map":{"x":"U64","x":"Text"}}"#).is_err());
    let raw = Cbor::Map(vec![(
        Cbor::Text("f".into()),
        Cbor::Map(vec![(0.into(), 1.into()), (0.into(), 2.into())]),
    )]);
    let mut bytes = Vec::new();
    cbor2::to_writer(&raw, &mut bytes).unwrap();
    assert!(cbor2::from_reader::<DocumentOwned, _>(bytes.as_slice()).is_err());
}

#[test]
fn json_extraction_shares_the_container_depth_bound() {
    let mut value = Cbor::Null;
    for _ in 0..MAX_CONVERSION_DEPTH {
        value = Cbor::Array(vec![value]);
    }
    assert!(Ft::Json.extract(value.clone()).is_ok());
    assert!(Ft::Json.extract(Cbor::Array(vec![value])).is_err());
}

#[test]
fn recovered_history_covers_raw_indexes_and_nested_keys() {
    let mut wire = serde_json::to_value(schema(map(&[("keep", Ft::Bool)]), 1)).unwrap();
    wire.as_object_mut().unwrap().remove("next_idx");
    wire.as_object_mut().unwrap().remove("history");
    let old: Schema = serde_json::from_value(wire).unwrap();
    let raw = DocumentOwned {
        fields: BTreeMap::from([
            (0, Fv::U64(1)),
            (
                1,
                Fv::Map(BTreeMap::from([
                    ("keep".into(), Fv::Bool(true)),
                    ("retired".into(), Fv::Text("old".into())),
                ])),
            ),
            (9, Fv::Text("retired top field".into())),
        ]),
    };
    let mut recovery = old.history_recovery();
    recovery.observe(&raw).unwrap();
    let recovered = recovery.finish();
    assert_eq!(recovered.allocated_idx_end(), 10);
    let mut bad = schema(
        map(&[
            ("keep", Ft::Bool),
            ("retired", Ft::Option(Box::new(Ft::Text))),
        ]),
        2,
    );
    assert!(bad.upgrade_with(&recovered).is_err());
    let mut good = schema(
        map(&[
            ("keep", Ft::Bool),
            ("fresh", Ft::Option(Box::new(Ft::Text))),
        ]),
        2,
    );
    good.upgrade_with(&recovered).unwrap();
    Document::try_from_doc(Arc::new(good), raw).unwrap();
}

#[test]
fn history_recovery_does_not_expand_a_trusted_allocation_watermark() {
    let mut wire = serde_json::to_value(schema(Ft::Text, 1)).unwrap();
    wire.as_object_mut().unwrap().remove("history");
    let old: Schema = serde_json::from_value(wire).unwrap();
    assert!(old.has_allocation_watermark());
    assert!(!old.has_upgrade_history());

    // Index 2 can be a write from an interrupted upgrade. The durable
    // watermark says it is the next allocatable index, so recovery of the
    // independently missing nested-key history must not move that boundary.
    let raw = DocumentOwned {
        fields: BTreeMap::from([
            (0, Fv::U64(1)),
            (1, Fv::Text("old".into())),
            (2, Fv::Text("written before metadata checkpoint".into())),
        ]),
    };
    let mut recovery = old.history_recovery();
    recovery.observe(&raw).unwrap();
    let recovered = recovery.finish();
    assert_eq!(recovered.allocated_idx_end(), 2);

    let mut builder = Schema::builder();
    builder.with_version(2);
    builder
        .add_field(Fe::new("payload".into(), Ft::Text).unwrap())
        .unwrap();
    builder
        .add_field(Fe::new("fresh".into(), Ft::Option(Box::new(Ft::Text))).unwrap())
        .unwrap();
    let mut upgraded = builder.build().unwrap();
    upgraded.upgrade_with(&recovered).unwrap();
    assert_eq!(upgraded.get_field("fresh").unwrap().idx(), 2);

    let loaded = Document::try_from_doc(Arc::new(upgraded), raw).unwrap();
    assert_eq!(
        loaded.get_field("fresh"),
        Some(&Fv::Text("written before metadata checkpoint".into()))
    );
}

#[test]
fn nested_history_is_preserved_through_arrays_and_wildcard_maps() {
    let nested = |with_key: bool| {
        let mut fields = vec![("keep", Ft::Bool)];
        if with_key {
            fields.push(("retired", Ft::Option(Box::new(Ft::Text))));
        }
        map(&fields)
    };
    let wrap = |ft| {
        Ft::Array(vec![Ft::Option(Box::new(Ft::Map(BTreeMap::from([(
            "*".into(),
            ft,
        )]))))])
    };
    let first = schema(wrap(nested(true)), 1);
    let mut second = schema(wrap(nested(false)), 2);
    second.upgrade_with(&first).unwrap();
    let mut bytes = Vec::new();
    cbor2::to_writer(&second, &mut bytes).unwrap();
    let second: Schema = cbor2::from_reader(bytes.as_slice()).unwrap();
    let mut third = schema(wrap(nested(true)), 3);
    assert!(third.upgrade_with(&second).is_err());
}

#[test]
fn field_updates_are_atomic_and_move_canonical_buffers() {
    let s = Arc::new(schema(Ft::Vector, 0));
    let mut doc = Document::new(s);
    doc.set_id(1);
    let vector = vec![anda_db_schema::bf16::from_f32(1.0); 1536];
    let pointer = vector.as_ptr();
    doc.set_field("payload", Fv::Vector(vector)).unwrap();
    let Some(Fv::Vector(value)) = doc.get_field("payload") else {
        panic!("vector")
    };
    assert_eq!(
        value.as_ptr(),
        pointer,
        "canonical vector allocation must be retained"
    );
    assert!(
        doc.set_field("payload", Fv::Array(vec![Fv::U64(65536)]))
            .is_err()
    );
    assert_eq!(
        doc.get_field_as::<Vec<anda_db_schema::bf16>>("payload")
            .unwrap()
            .len(),
        1536
    );
    let before = serde_json::to_value(&doc).unwrap();
    assert!(
        doc.set_doc(DocumentOwned {
            fields: BTreeMap::from([(0, Fv::U64(1)), (1, Fv::Text("invalid".into()))])
        })
        .is_err()
    );
    assert_eq!(serde_json::to_value(doc).unwrap(), before);
}

#[test]
fn vector_fast_path_falls_back_to_established_cbor_coercion() {
    let entry = Fe::new("vector".into(), Ft::Vector).unwrap();
    assert_eq!(
        entry
            .coerce(Fv::Array(vec![Fv::Json(serde_json::json!(1))]))
            .unwrap(),
        Fv::Vector(vec![anda_db_schema::bf16::from_bits(1)])
    );
    assert!(
        entry
            .coerce(Fv::Array(vec![Fv::Json(serde_json::json!("invalid"))]))
            .is_err()
    );
}

#[test]
fn json_conversion_rejects_cbor_tags_instead_of_discarding_them() {
    let tagged = Cbor::Tag(0, Box::new(Cbor::Text("2026-09-06".into())));
    assert!(Ft::Json.extract(tagged.clone()).is_err());
    assert!(Fv::json_from(tagged).is_err());
    assert_eq!(
        Ft::Json.extract(Cbor::Text("2026-09-06".into())).unwrap(),
        Fv::Json(Json::String("2026-09-06".into()))
    );
}
