use super::*;
use cbor2::Value;
use cbor2::{cbor, from_reader, to_writer};
use ic_auth_types::{Xid, cbor_into_vec};
use serde_json::json;
use std::collections::{BTreeSet, HashMap, HashSet};

/// One stored value through the document read path.
fn read_back(ft: &FieldType, value: FieldValue) -> Result<FieldValue, SchemaError> {
    ft.prepare(value, 0, ValueMode::Read)
}

#[test]
fn test_field_key() {
    let val = FieldValue::Map(BTreeMap::from([(
        FieldKey::Text("*".into()),
        FieldValue::Text("*".into()),
    )]));
    let data = cbor_into_vec(&cbor!({ "*" => "*" }).unwrap()).unwrap();
    assert_eq!(cbor_into_vec(&val).unwrap(), data);
    let val2: FieldValue = from_reader(data.as_slice()).unwrap();
    assert_eq!(val, val2);

    let val = FieldValue::Map(BTreeMap::from([(
        FieldKey::Bytes(b"*".to_vec()),
        FieldValue::Bytes(b"*".to_vec()),
    )]));
    let data = cbor_into_vec(&Value::Map(vec![(
        Value::Bytes(b"*".to_vec()),
        Value::Bytes(b"*".to_vec()),
    )]))
    .unwrap();
    // println!("data: {:?}", hex::encode(&data));
    assert_eq!(cbor_into_vec(&val).unwrap(), data);
    let val2: FieldValue = from_reader(data.as_slice()).unwrap();
    assert_eq!(val, val2);
    let data = serde_json::to_string(&val).unwrap();
    println!("json: {}", data);
    assert_eq!(data, r#"{"b64:Kg==":"b64:Kg=="}"#);
    let val2: FieldValue = serde_json::from_str(&data).unwrap();
    assert_eq!(val, val2);

    let val = FieldValue::Map(BTreeMap::from([(
        FieldKey::I64(-7),
        FieldValue::Text("seven".into()),
    )]));
    let data = cbor_into_vec(&Value::Map(vec![(
        Value::Integer((-7).into()),
        Value::Text("seven".into()),
    )]))
    .unwrap();
    assert_eq!(cbor_into_vec(&val).unwrap(), data);
    let val2: FieldValue = from_reader(data.as_slice()).unwrap();
    assert_eq!(val, val2);
    let data = serde_json::to_string(&val).unwrap();
    assert_eq!(data, r#"{"i64:-7":"seven"}"#);
    let val2: FieldValue = serde_json::from_str(&data).unwrap();
    assert_eq!(val, val2);
}

#[test]
fn test_field_type_debug() {
    assert_eq!(format!("{:?}", FieldType::Bool), "Bool");
    assert_eq!(format!("{:?}", FieldType::I64), "I64");
    assert_eq!(format!("{:?}", FieldType::U64), "U64");
    assert_eq!(format!("{:?}", FieldType::F64), "F64");
    assert_eq!(format!("{:?}", FieldType::F32), "F32");
    assert_eq!(format!("{:?}", FieldType::Bytes), "Bytes");
    assert_eq!(format!("{:?}", FieldType::Text), "Text");
    assert_eq!(format!("{:?}", FieldType::Json), "Json");
    assert_eq!(format!("{:?}", FieldType::Vector), "Vector");

    let array_type = FieldType::Array(vec![FieldType::U64]);
    assert_eq!(format!("{array_type:?}"), "Array([U64])");

    let mut map = BTreeMap::new();
    map.insert("key".into(), FieldType::Text);
    let map_type = FieldType::Map(map);
    assert_eq!(format!("{map_type:?}"), "Map({Text(\"key\"): Text})");

    let option_type = FieldType::Option(Box::new(FieldType::Bool));
    assert_eq!(format!("{option_type:?}"), "Option(Bool)");
}

#[test]
fn test_field_value_debug() {
    assert_eq!(format!("{:?}", FieldValue::Bool(true)), "Bool(true)");
    assert_eq!(format!("{:?}", FieldValue::I64(-42)), "I64(-42)");
    assert_eq!(format!("{:?}", FieldValue::U64(42)), "U64(42)");
    assert_eq!(format!("{:?}", FieldValue::F64(3.15)), "F64(3.15)");
    assert_eq!(format!("{:?}", FieldValue::F32(2.71)), "F32(2.71)");
    assert_eq!(
        format!("{:?}", FieldValue::Bytes(vec![1, 2, 3])),
        "Bytes([1, 2, 3])"
    );
    assert_eq!(
        format!("{:?}", FieldValue::Text("hello".to_string())),
        "Text(\"hello\")"
    );

    let json_val = FieldValue::Json(json!({"name": "test"}));
    assert_eq!(
        format!("{json_val:?}"),
        "Json(Object {\"name\": String(\"test\")})"
    );

    assert_eq!(
        format!("{:?}", FieldValue::Vector(vec![bf16::from_f32(1.5)])),
        "Vector([1.5])"
    );

    let array_val = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
    assert_eq!(format!("{array_val:?}"), "Array([U64(1), U64(2)])");

    let mut map = BTreeMap::new();
    map.insert("key".into(), FieldValue::Text("value".to_string()));
    let map_val = FieldValue::Map(map);
    assert_eq!(
        format!("{map_val:?}"),
        "Map({Text(\"key\"): Text(\"value\")})"
    );

    assert_eq!(format!("{:?}", FieldValue::Null), "Null");
}

#[test]
fn test_field_type_extract() {
    // Bool
    let bool_val = FieldType::Bool.extract(Cbor::Bool(true)).unwrap();
    assert_eq!(bool_val, FieldValue::Bool(true));

    // U64
    let u64_val = FieldType::U64.extract(cbor!(42).unwrap()).unwrap();
    assert_eq!(u64_val, FieldValue::U64(42));

    // I64
    let i64_val = FieldType::I64.extract(Cbor::Integer((-42).into())).unwrap();
    assert_eq!(i64_val, FieldValue::I64(-42));

    // F64
    let f64_val = FieldType::F64.extract(Cbor::Float(3.15)).unwrap();
    assert_eq!(f64_val, FieldValue::F64(3.15));

    // F32
    let f32_val = FieldType::F32.extract(Cbor::Float(2.71)).unwrap();
    assert_eq!(f32_val, FieldValue::F32(2.71_f32));

    // Bytes
    let bytes_val = FieldType::Bytes
        .extract(Cbor::Bytes(vec![1, 2, 3]))
        .unwrap();
    assert_eq!(bytes_val, FieldValue::Bytes(vec![1, 2, 3]));

    // Text
    let text_val = FieldType::Text
        .extract(Cbor::Text("hello".to_string()))
        .unwrap();
    assert_eq!(text_val, FieldValue::Text("hello".to_string()));

    // Vector
    let vector_val = FieldType::Vector
        .extract(Cbor::Array(vec![
            Cbor::Integer(bf16::from_f32(1.1).to_bits().into()),
            Cbor::Integer(bf16::from_f32(1.2).to_bits().into()),
        ]))
        .unwrap();
    assert_eq!(
        vector_val,
        FieldValue::Vector(vec![bf16::from_f32(1.1), bf16::from_f32(1.2)])
    );

    // Array with single type
    let array_type = FieldType::Array(vec![FieldType::U64]);
    let array_cbor = Cbor::Array(vec![Cbor::Integer(1.into()), Cbor::Integer(2.into())]);
    let array_val = array_type.extract(array_cbor).unwrap();
    assert_eq!(
        array_val,
        FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)])
    );

    // Array with multiple types
    let array_type = FieldType::Array(vec![FieldType::U64, FieldType::Text]);
    let array_cbor = Cbor::Array(vec![
        Cbor::Integer(1.into()),
        Cbor::Text("hello".to_string()),
    ]);
    let array_val = array_type.extract(array_cbor).unwrap();
    assert_eq!(
        array_val,
        FieldValue::Array(vec![
            FieldValue::U64(1),
            FieldValue::Text("hello".to_string()),
        ])
    );

    // Map
    let mut map_type = BTreeMap::new();
    map_type.insert("_id".into(), FieldType::U64);
    map_type.insert("name".into(), FieldType::Text);
    let map_type = FieldType::Map(map_type);

    let map_cbor = Cbor::Map(vec![
        (Cbor::Text("_id".to_string()), Cbor::Integer(1.into())),
        (
            Cbor::Text("name".to_string()),
            Cbor::Text("test".to_string()),
        ),
    ]);

    let map_val = map_type.extract(map_cbor).unwrap();
    let mut expected_map = BTreeMap::new();
    expected_map.insert("_id".into(), FieldValue::U64(1));
    expected_map.insert("name".into(), FieldValue::Text("test".to_string()));
    assert_eq!(map_val, FieldValue::Map(expected_map));

    // Option (Some)
    let option_type = FieldType::Option(Box::new(FieldType::Bool));
    let option_val = option_type.extract(Cbor::Bool(true)).unwrap();
    assert_eq!(option_val, FieldValue::Bool(true));

    // Option (None)
    let option_val = option_type.extract(Cbor::Null).unwrap();
    assert_eq!(option_val, FieldValue::Null);
}

#[test]
fn test_field_type_validate() {
    // Bool
    assert!(FieldType::Bool.validate(&FieldValue::Bool(true)).is_ok());
    assert!(FieldType::Bool.validate(&FieldValue::U64(1)).is_err());

    // U64
    assert!(FieldType::U64.validate(&FieldValue::U64(42)).is_ok());
    assert!(FieldType::U64.validate(&FieldValue::I64(42)).is_err());

    // I64: the canonical variant, plus the U64 read-back shape produced
    // by untyped CBOR deserialization of non-negative values.
    assert!(FieldType::I64.validate(&FieldValue::I64(-42)).is_ok());
    assert!(FieldType::I64.validate(&FieldValue::U64(42)).is_ok());
    assert!(
        FieldType::I64
            .validate(&FieldValue::U64(i64::MAX as u64))
            .is_ok()
    );
    assert!(
        FieldType::I64
            .validate(&FieldValue::U64(i64::MAX as u64 + 1))
            .is_err()
    );

    // F64
    assert!(FieldType::F64.validate(&FieldValue::F64(3.15)).is_ok());
    assert!(FieldType::F64.validate(&FieldValue::F64(f64::NAN)).is_err());
    assert!(FieldType::F64.validate(&FieldValue::F32(3.15)).is_err());

    // F32: the canonical variant, plus the two F64 read-back shapes:
    // the exact CBOR widening, and the JSON shortest-decimal round trip
    // (serde_json serializes F32(2.71) as "2.71", which parses back as
    // F64(2.71), not the exact widening).
    assert!(FieldType::F32.validate(&FieldValue::F32(2.71)).is_ok());
    assert!(FieldType::F32.validate(&FieldValue::F32(f32::NAN)).is_err());
    assert!(
        FieldType::F32
            .validate(&FieldValue::F64(2.71f32 as f64))
            .is_ok()
    );
    assert!(
        FieldType::F32
            .validate(&FieldValue::F64(f64::INFINITY))
            .is_ok()
    );
    // JSON read-back of F32(2.71).
    assert!(FieldType::F32.validate(&FieldValue::F64(2.71)).is_ok());
    // Not possible F32 read-backs: excess precision, out of range, or
    // values whose f32 rounding loses far more than a decimal digit.
    assert!(
        FieldType::F32
            .validate(&FieldValue::F64(2.7100000000001))
            .is_err()
    );
    assert!(FieldType::F32.validate(&FieldValue::F64(1e308)).is_err());
    assert!(FieldType::F32.validate(&FieldValue::F64(1e-300)).is_err());
    assert!(FieldType::F32.validate(&FieldValue::F64(f64::NAN)).is_err());

    // Bytes
    assert!(
        FieldType::Bytes
            .validate(&FieldValue::Bytes(vec![1, 2, 3]))
            .is_ok()
    );
    assert!(
        FieldType::Bytes
            .validate(&FieldValue::Text("bytes".to_string()))
            .is_err()
    );

    // Text
    assert!(
        FieldType::Text
            .validate(&FieldValue::Text("hello".to_string()))
            .is_ok()
    );
    assert!(
        FieldType::Text
            .validate(&FieldValue::Bytes(vec![104, 101, 108, 108, 111]))
            .is_err()
    );

    // Json
    assert!(
        FieldType::Json
            .validate(&FieldValue::Json(json!({"key": "value"})))
            .is_ok()
    );
    assert!(
        FieldType::Json
            .validate(&FieldValue::Text("json".to_string()))
            .is_ok()
    );

    // Vector
    assert!(
        FieldType::Vector
            .validate(&FieldValue::Vector(vec![bf16::from_f32(1.5)]))
            .is_ok()
    );
    assert!(
        FieldType::Vector
            .validate(&FieldValue::Array(vec![FieldValue::U64(1)]))
            .is_ok()
    );
    assert!(
        FieldType::Vector
            .validate(&FieldValue::Array(vec![FieldValue::U64(u16::MAX as u64)]))
            .is_ok()
    );
    assert!(
        FieldType::Vector
            .validate(&FieldValue::Array(vec![FieldValue::I64(-1)]))
            .is_err()
    );
    // Elements that cannot be extracted as bf16 bits must not validate either.
    assert!(
        FieldType::Vector
            .validate(&FieldValue::Array(vec![FieldValue::U64(
                u16::MAX as u64 + 1
            )]))
            .is_err()
    );

    // Array with single type
    let array_type = FieldType::Array(vec![FieldType::U64]);
    let array_val = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
    assert!(array_type.validate(&array_val).is_ok());

    let invalid_array_val = FieldValue::Array(vec![
        FieldValue::U64(1),
        FieldValue::Text("invalid".to_string()),
    ]);
    assert!(array_type.validate(&invalid_array_val).is_err());

    // Array with multiple types
    let array_type = FieldType::Array(vec![FieldType::U64, FieldType::Text]);
    let array_val = FieldValue::Array(vec![
        FieldValue::U64(1),
        FieldValue::Text("hello".to_string()),
    ]);
    assert!(array_type.validate(&array_val).is_ok());

    let invalid_array_val = FieldValue::Array(vec![FieldValue::U64(1)]);
    assert!(array_type.validate(&invalid_array_val).is_err());

    // Map
    let mut map_type = BTreeMap::new();
    map_type.insert("_id".into(), FieldType::U64);
    map_type.insert("name".into(), FieldType::Text);
    let map_type = FieldType::Map(map_type);

    let mut map_val = BTreeMap::new();
    map_val.insert("_id".into(), FieldValue::U64(1));
    map_val.insert("name".into(), FieldValue::Text("test".to_string()));
    let map_val = FieldValue::Map(map_val);
    assert!(map_type.validate(&map_val).is_ok());

    let mut invalid_map_val = BTreeMap::new();
    invalid_map_val.insert("_id".into(), FieldValue::Text("invalid".to_string()));
    invalid_map_val.insert("name".into(), FieldValue::Text("test".to_string()));
    let invalid_map_val = FieldValue::Map(invalid_map_val);
    assert!(map_type.validate(&invalid_map_val).is_err());

    // Option (Some)
    let option_type = FieldType::Option(Box::new(FieldType::Bool));
    assert!(option_type.validate(&FieldValue::Bool(true)).is_ok());
    assert!(option_type.validate(&FieldValue::Null).is_ok());
    assert!(option_type.validate(&FieldValue::U64(42)).is_err());
}

#[test]
fn f32_json_read_back_round_trips() {
    // Regression (#28): serde_json serializes an F32 with the f32
    // shortest-decimal form; parsing it back yields an F64 that is NOT
    // the exact widening (2.71f64 != f64::from(2.71f32)). Such values
    // must still validate and convert as F32 read-back shapes.
    let json = serde_json::to_string(&FieldValue::F32(2.71)).unwrap();
    let read_back: FieldValue = serde_json::from_str(&json).unwrap();
    assert!(
        FieldType::F32.validate(&read_back).is_ok(),
        "JSON read-back {read_back:?} of F32(2.71) must validate"
    );
    let f: f32 = read_back.try_into().unwrap();
    assert_eq!(f, 2.71f32);

    // Any stored f32 must survive both read-back channels.
    for f in [
        0f32,
        -0.0,
        1.25,
        2.71,
        -3.15,
        1e-45, // smallest positive f32 subnormal
        1.0e-40,
        f32::MIN,
        f32::MAX,
        f32::INFINITY,
        f32::NEG_INFINITY,
    ] {
        // CBOR read-back: exact widening.
        assert!(
            FieldType::F32
                .validate(&FieldValue::F64(f64::from(f)))
                .is_ok(),
            "CBOR read-back of {f}"
        );
        // JSON read-back: shortest-decimal round trip.
        let v: f64 = format!("{f}").parse().unwrap();
        assert!(
            FieldType::F32.validate(&FieldValue::F64(v)).is_ok(),
            "JSON read-back of {f}"
        );
        let converted: f32 = (&FieldValue::F64(v)).try_into().unwrap();
        assert_eq!(converted, f, "conversion of JSON read-back of {f}");
    }
}

#[test]
fn read_back_folds_shapes_into_canonical_variants() {
    // I64 <- U64 within range.
    assert_eq!(
        read_back(&FieldType::I64, FieldValue::U64(5)).unwrap(),
        FieldValue::I64(5)
    );
    // Out-of-range U64 is rejected.
    assert!(read_back(&FieldType::I64, FieldValue::U64(i64::MAX as u64 + 1)).is_err());

    // F32 <- F64 read-back shapes (both channels).
    assert_eq!(
        read_back(&FieldType::F32, FieldValue::F64(f64::from(2.71f32))).unwrap(),
        FieldValue::F32(2.71)
    );
    assert_eq!(
        read_back(&FieldType::F32, FieldValue::F64(2.71)).unwrap(),
        FieldValue::F32(2.71)
    );
    // A non-read-back F64 is rejected.
    assert!(read_back(&FieldType::F32, FieldValue::F64(2.7100000000001)).is_err());

    // Vector <- Array(U64 bf16 bits).
    let expected = vec![bf16::from_f32(1.5), bf16::from_f32(-2.0)];
    let v = FieldValue::Array(
        expected
            .iter()
            .map(|value| FieldValue::U64(value.to_bits() as u64))
            .collect(),
    );
    assert_eq!(
        read_back(&FieldType::Vector, v).unwrap(),
        FieldValue::Vector(expected)
    );
    // Invalid bit shapes are rejected.
    let v = FieldValue::Array(vec![FieldValue::U64(u16::MAX as u64 + 1)]);
    assert!(read_back(&FieldType::Vector, v).is_err());

    // Composites recurse.
    let v = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::I64(-2)]);
    assert_eq!(
        read_back(&FieldType::Array(vec![FieldType::I64]), v).unwrap(),
        FieldValue::Array(vec![FieldValue::I64(1), FieldValue::I64(-2)])
    );

    let v = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::Text("x".into())]);
    assert_eq!(
        read_back(&FieldType::Array(vec![FieldType::I64, FieldType::Text]), v).unwrap(),
        FieldValue::Array(vec![FieldValue::I64(1), FieldValue::Text("x".into())])
    );

    let v = FieldValue::Map(BTreeMap::from([("a".into(), FieldValue::U64(3))]));
    let ft = FieldType::Map(BTreeMap::from([("*".into(), FieldType::I64)]));
    assert_eq!(
        read_back(&ft, v).unwrap(),
        FieldValue::Map(BTreeMap::from([("a".into(), FieldValue::I64(3))]))
    );

    let v = FieldValue::Map(BTreeMap::from([("a".into(), FieldValue::U64(3))]));
    let ft = FieldType::Map(BTreeMap::from([("a".into(), FieldType::I64)]));
    assert_eq!(
        read_back(&ft, v).unwrap(),
        FieldValue::Map(BTreeMap::from([("a".into(), FieldValue::I64(3))]))
    );

    // Option unwraps; Null stays.
    let ft = FieldType::Option(Box::new(FieldType::I64));
    assert_eq!(
        read_back(&ft, FieldValue::U64(7)).unwrap(),
        FieldValue::I64(7)
    );
    assert_eq!(read_back(&ft, FieldValue::Null).unwrap(), FieldValue::Null);

    // Canonical values are left untouched.
    assert_eq!(
        read_back(&FieldType::U64, FieldValue::U64(9)).unwrap(),
        FieldValue::U64(9)
    );
    assert_eq!(
        read_back(&FieldType::F64, FieldValue::F64(1.5)).unwrap(),
        FieldValue::F64(1.5)
    );
}

#[test]
fn read_back_restores_json_values_after_a_storage_round_trip() {
    // Regression: a `Json` payload is stored as its plain CBOR shape and
    // reads back as `Map` / `Array` / a primitive. Without normalization
    // index maintenance derives different text for the insert-time and
    // the read-back value of the same document.
    let value = FieldValue::Json(json!({"tags": [1, "urgent"], "note": "hello"}));
    let mut data = Vec::new();
    to_writer(&value, &mut data).unwrap();
    let restored: FieldValue = from_reader(data.as_slice()).unwrap();
    assert_eq!(
        restored,
        FieldValue::Map(BTreeMap::from([
            ("note".into(), FieldValue::Text("hello".into())),
            (
                "tags".into(),
                FieldValue::Array(vec![FieldValue::U64(1), FieldValue::Text("urgent".into())])
            ),
        ]))
    );
    assert_eq!(read_back(&FieldType::Json, restored).unwrap(), value);

    // Scalar payloads and `Option(Json)` behave the same way.
    assert_eq!(
        read_back(&FieldType::Json, FieldValue::Text("hello".into())).unwrap(),
        FieldValue::Json(json!("hello"))
    );
    let ft = FieldType::Option(Box::new(FieldType::Json));
    assert_eq!(
        read_back(&ft, FieldValue::Array(vec![FieldValue::U64(1)])).unwrap(),
        FieldValue::Json(json!([1]))
    );
    assert_eq!(read_back(&ft, FieldValue::Null).unwrap(), FieldValue::Null);

    // An already canonical value is left alone; a shape with no JSON
    // representation is rejected.
    assert_eq!(
        read_back(&FieldType::Json, FieldValue::Json(json!({"a": 1}))).unwrap(),
        FieldValue::Json(json!({"a": 1}))
    );
    assert!(read_back(&FieldType::Json, FieldValue::Bytes(vec![1, 2, 3])).is_err());
}

#[test]
fn test_map_from_rejects_duplicate_keys() {
    let dup = Cbor::Map(vec![
        (Cbor::Text("k".to_string()), Cbor::Integer(1.into())),
        (Cbor::Text("k".to_string()), Cbor::Integer(2.into())),
    ]);

    // Untyped extraction must not silently drop duplicate entries.
    let err = FieldValue::map_from(dup.clone(), &BTreeMap::new()).unwrap_err();
    assert!(err.to_string().contains("duplicate map key"));

    // Typed (wildcard) extraction rejects duplicates as well.
    let types = BTreeMap::from([(TEXT_WILDCARD_KEY.clone(), FieldType::U64)]);
    let err = FieldValue::map_from(dup, &types).unwrap_err();
    assert!(err.to_string().contains("duplicate map key"));
}

#[test]
fn wildcard_maps_enforce_the_declared_key_variant() {
    // Regression: only the *value* type used to be checked, so a
    // `Map<Text, U64>` accepted integer and byte keys. The document was
    // persisted but could never be read back into its declared Rust type.
    let text_keyed = FieldType::Map(BTreeMap::from([(TEXT_WILDCARD_KEY.clone(), Ft::U64)]));
    let foreign_keys = Fv::Map(BTreeMap::from([
        (FieldKey::I64(5), Fv::U64(1)),
        (FieldKey::Bytes(vec![9]), Fv::U64(2)),
    ]));
    let err = text_keyed.validate(&foreign_keys).unwrap_err();
    assert!(err.to_string().contains("expected a Text key"), "{err}");

    // What the acceptance used to cost: the stored value no longer
    // deserializes into the `BTreeMap<String, u64>` the schema declares.
    let mut data = Vec::new();
    to_writer(&foreign_keys, &mut data).unwrap();
    let cbor: Cbor = from_reader(data.as_slice()).unwrap();
    assert!(cbor.deserialized::<BTreeMap<String, u64>>().is_err());

    // `map_from`'s wildcard branch is the same hole seen from the JSON
    // API: `"i64:5"` decodes to `FieldKey::I64(5)` (see value_serde.rs).
    let err = FieldValue::map_from(
        Cbor::Map(vec![(Cbor::Integer(5.into()), Cbor::Integer(1.into()))]),
        &BTreeMap::from([(TEXT_WILDCARD_KEY.clone(), Ft::U64)]),
    )
    .unwrap_err();
    assert!(err.to_string().contains("expected a Text key"), "{err}");

    // The mirror holes: a `Map<I64, T>` / `Map<Bytes, T>` filled with
    // keys of another variant.
    let i64_keyed = FieldType::Map(BTreeMap::from([(I64_WILDCARD_KEY.clone(), Ft::U64)]));
    let err = i64_keyed
        .validate(&Fv::Map(BTreeMap::from([(
            FieldKey::Text("5".into()),
            Fv::U64(1),
        )])))
        .unwrap_err();
    assert!(err.to_string().contains("expected a I64 key"), "{err}");
    let err = i64_keyed
        .extract(Cbor::Map(vec![(
            Cbor::Text("5".into()),
            Cbor::Integer(1.into()),
        )]))
        .unwrap_err();
    assert!(err.to_string().contains("expected a I64 key"), "{err}");

    let bytes_keyed = FieldType::Map(BTreeMap::from([(BYTES_WILDCARD_KEY.clone(), Ft::U64)]));
    let err = bytes_keyed
        .validate(&Fv::Map(BTreeMap::from([(
            FieldKey::Text("k".into()),
            Fv::U64(1),
        )])))
        .unwrap_err();
    assert!(err.to_string().contains("expected a Bytes key"), "{err}");

    // Matching keys — including the sentinel itself — still pass, and a
    // map declaring its keys explicitly is unaffected (a non-wildcard
    // `Map` is checked key by key, as before).
    let matching = Fv::Map(BTreeMap::from([
        (FieldKey::Text("a".into()), Fv::U64(1)),
        (TEXT_WILDCARD_KEY.clone(), Fv::U64(2)),
    ]));
    assert!(text_keyed.validate(&matching).is_ok());
    assert_eq!(
        text_keyed
            .extract(Cbor::Map(vec![
                (Cbor::Text("a".into()), Cbor::Integer(1.into())),
                (Cbor::Text("*".into()), Cbor::Integer(2.into())),
            ]))
            .unwrap(),
        matching
    );
}

#[test]
fn coerce_accepts_exactly_what_document_try_from_accepts() {
    // Regression: `FieldEntry::validate` rejected wire shapes that
    // `Document::try_from`'s CBOR coercion accepts, so an API validating
    // raw values (`doc.update`) refused documents its own create path
    // (`doc.add`) had just stored.
    let blob = Fe::new("blob".to_string(), Ft::Bytes).unwrap();
    let wire = Fv::Array(vec![Fv::U64(1), Fv::U64(2), Fv::U64(3)]);
    assert!(blob.validate(&wire).is_err());
    assert_eq!(blob.coerce(wire).unwrap(), Fv::Bytes(vec![1, 2, 3]));

    // Canonical values pass through unchanged, and real mismatches still
    // fail.
    assert_eq!(blob.coerce(Fv::Bytes(vec![7])).unwrap(), Fv::Bytes(vec![7]));
    assert!(blob.coerce(Fv::Text("nope".into())).is_err());
    assert!(blob.coerce(Fv::Array(vec![Fv::U64(256)])).is_err());

    // A missing value keeps `validate`'s required/optional wording.
    let err = blob.coerce(Fv::Null).unwrap_err();
    assert!(err.to_string().contains("is required"), "{err}");
    let opt = Fe::new("opt".to_string(), Ft::Option(Box::new(Ft::Bytes))).unwrap();
    assert_eq!(opt.coerce(Fv::Null).unwrap(), Fv::Null);
    assert_eq!(
        opt.coerce(Fv::Array(vec![Fv::U64(9)])).unwrap(),
        Fv::Bytes(vec![9])
    );

    // Other read-back shapes are folded into the declared variant too.
    let vector = Fe::new("v".to_string(), Ft::Vector).unwrap();
    assert_eq!(
        vector.coerce(Fv::Array(vec![Fv::U64(16256)])).unwrap(),
        Fv::Vector(vec![bf16::from_bits(16256)])
    );

    // The complexity budget is enforced, exactly as in `try_from`.
    let deep = Fe::new("deep".to_string(), Ft::Array(vec![])).unwrap();
    let over_budget = deeply_nested_field_value(FieldValueBudget::default().max_depth + 2);
    assert!(deep.coerce(over_budget).is_err());
}

#[test]
fn nested_map_upgrades_are_compatible_only_when_data_stays_readable() {
    let v1 = Ft::Map(BTreeMap::from([("a".into(), Ft::Text)]));
    let gained = Ft::Map(BTreeMap::from([
        ("a".into(), Ft::Text),
        ("b".into(), Ft::Option(Box::new(Ft::Text))),
    ]));

    // Gaining an *optional* key and losing a key are both compatible.
    assert!(gained.is_compatible_upgrade_of(&v1));
    assert!(v1.is_compatible_upgrade_of(&gained));
    assert!(v1.is_compatible_upgrade_of(&v1));

    // A new *required* key would make every stored document invalid.
    let required = Ft::Map(BTreeMap::from([
        ("a".into(), Ft::Text),
        ("b".into(), Ft::Text),
    ]));
    assert!(!required.is_compatible_upgrade_of(&v1));
    // A key whose type changed needs stored values rewritten.
    assert!(!Ft::Map(BTreeMap::from([("a".into(), Ft::U64)])).is_compatible_upgrade_of(&v1));

    // The rule recurses through `Option` and `Array` wrappers.
    assert!(
        Ft::Option(Box::new(gained.clone()))
            .is_compatible_upgrade_of(&Ft::Option(Box::new(v1.clone())))
    );
    assert!(Ft::Array(vec![gained.clone()]).is_compatible_upgrade_of(&Ft::Array(vec![v1.clone()])));
    // Array arity is part of the shape.
    assert!(
        !Ft::Array(vec![Ft::Text, Ft::Text]).is_compatible_upgrade_of(&Ft::Array(vec![Ft::Text]))
    );

    // Wildcard maps: the value type must stay put and the sentinel (hence
    // the key variant) must not change; and a wildcard map is never
    // interchangeable with an explicitly keyed one.
    let wild_text = Ft::Map(BTreeMap::from([(TEXT_WILDCARD_KEY.clone(), Ft::U64)]));
    let wild_i64 = Ft::Map(BTreeMap::from([(I64_WILDCARD_KEY.clone(), Ft::U64)]));
    assert!(wild_text.is_compatible_upgrade_of(&wild_text));
    assert!(!wild_i64.is_compatible_upgrade_of(&wild_text));
    assert!(
        !Ft::Map(BTreeMap::from([(TEXT_WILDCARD_KEY.clone(), Ft::Text)]))
            .is_compatible_upgrade_of(&wild_text)
    );
    assert!(!wild_text.is_compatible_upgrade_of(&v1));
    assert!(!v1.is_compatible_upgrade_of(&wild_text));

    // Unrelated types never become one another. Making a type optional
    // is read-safe and allowed; the reverse is not.
    assert!(!Ft::Text.is_compatible_upgrade_of(&Ft::U64));
    assert!(Ft::Option(Box::new(Ft::Text)).is_compatible_upgrade_of(&Ft::Text));
    assert!(!Ft::Text.is_compatible_upgrade_of(&Ft::Option(Box::new(Ft::Text))));
}

#[test]
fn read_back_drops_only_removed_nested_keys() {
    let stale = || {
        Fv::Map(BTreeMap::from([
            ("a".into(), Fv::Text("keep".into())),
            ("b".into(), Fv::Text("gone".into())),
        ]))
    };

    // A non-wildcard map drops what it no longer declares, at every depth.
    let ft = Ft::Map(BTreeMap::from([("a".into(), Ft::Text)]));
    assert_eq!(
        read_back(&ft, stale()).unwrap(),
        Fv::Map(BTreeMap::from([("a".into(), Fv::Text("keep".into()))]))
    );

    let ft = Ft::Array(vec![Ft::Option(Box::new(Ft::Map(BTreeMap::from([(
        "a".into(),
        Ft::Text,
    )]))))]);
    assert_eq!(
        read_back(&ft, Fv::Array(vec![stale()])).unwrap(),
        Fv::Array(vec![Fv::Map(BTreeMap::from([(
            "a".into(),
            Fv::Text("keep".into())
        )]))])
    );

    // A wildcard map declares no key names, so nothing is dropped; an
    // empty `Map` type accepts everything, likewise.
    let ft = Ft::Map(BTreeMap::from([(TEXT_WILDCARD_KEY.clone(), Ft::Text)]));
    assert_eq!(read_back(&ft, stale()).unwrap(), stale());
    assert_eq!(
        read_back(&Ft::Map(BTreeMap::new()), stale()).unwrap(),
        stale()
    );
}

#[test]
fn test_field_type_extract_rejects_missing_required_map_key() {
    let map_type = FieldType::Map(BTreeMap::from([
        ("name".into(), FieldType::Text),
        ("age".into(), FieldType::Option(Box::new(FieldType::U64))),
    ]));

    let missing_required = Cbor::Map(vec![(
        Cbor::Text("age".to_string()),
        Cbor::Integer(42.into()),
    )]);
    assert!(map_type.extract(missing_required).is_err());

    let missing_optional = Cbor::Map(vec![(
        Cbor::Text("name".to_string()),
        Cbor::Text("Ada".to_string()),
    )]);
    let extracted = map_type.extract(missing_optional).unwrap();
    assert!(map_type.validate(&extracted).is_ok());
}

#[test]
fn test_field_value_conversion() {
    // Bool
    let bool_val = FieldValue::Bool(true);
    let cbor: Cbor = bool_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), bool_val);

    // U64
    let u64_val = FieldValue::U64(42);
    let cbor: Cbor = u64_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), u64_val);

    // I64
    let i64_val = FieldValue::I64(-42);
    let cbor: Cbor = i64_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), i64_val);

    // F64
    let f64_val = FieldValue::F64(3.15);
    let cbor: Cbor = f64_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), f64_val);

    // F32
    let f32_val = FieldValue::F32(2.71);
    let cbor: Cbor = f32_val.clone().into();
    // 注意：F32转换为CBOR后再转回来会变成F64
    if let FieldValue::F64(f64_val) = FieldValue::try_from(cbor).unwrap() {
        assert!((f64_val - 2.71).abs() < f32::EPSILON as f64);
    } else {
        panic!("Expected F64");
    }

    // Bytes
    let bytes_val = FieldValue::Bytes(vec![1, 2, 3]);
    let cbor: Cbor = bytes_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), bytes_val);

    // Text
    let text_val = FieldValue::Text("hello".to_string());
    let cbor: Cbor = text_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), text_val);

    // Json
    let json_val = FieldValue::Json(json!({"name": "test"}));
    let cbor: Cbor = json_val.into();
    // JSON转换为CBOR后再转回来会变成Map
    let mut expected_map = BTreeMap::new();
    expected_map.insert("name".into(), FieldValue::Text("test".to_string()));
    assert_eq!(
        FieldValue::try_from(cbor).unwrap(),
        FieldValue::Map(expected_map)
    );

    // Vector
    let vector_val = FieldValue::Vector(vec![bf16::from_f32(1.5)]);
    let cbor: Cbor = vector_val.clone().into();
    // Vector转换为CBOR后再转回来会变成Array
    let expected_array =
        FieldValue::Array(vec![FieldValue::U64(bf16::from_f32(1.5).to_bits() as u64)]);
    assert_eq!(FieldValue::try_from(cbor).unwrap(), expected_array);

    // Array
    let array_val = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
    let cbor: Cbor = array_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), array_val);

    // Map
    let mut map = BTreeMap::new();
    map.insert("key".into(), FieldValue::Text("value".to_string()));
    let map_val = FieldValue::Map(map);
    let cbor: Cbor = map_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), map_val);

    // Null
    let null_val = FieldValue::Null;
    let cbor: Cbor = null_val.clone().into();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), null_val);
}

#[test]
fn test_field_entry() {
    // 创建字段
    let field = FieldEntry::new("user_id".to_string(), FieldType::U64)
        .unwrap()
        .with_unique()
        .with_idx(1);

    assert_eq!(field.name(), "user_id");
    assert_eq!(field.r#type(), &FieldType::U64);
    assert!(field.unique());
    assert_eq!(field.idx(), 1);

    // 测试提取值
    let val = field.extract(Cbor::Integer(42.into()), true).unwrap();
    assert_eq!(val, FieldValue::U64(42));

    // 测试验证值
    assert!(field.validate(&FieldValue::U64(42)).is_ok());
    assert!(field.validate(&FieldValue::I64(42)).is_err());

    // 测试必填字段的空值验证
    assert!(field.validate(&FieldValue::Null).is_err());

    // 测试非必填字段的空值验证
    let optional_field = FieldEntry::new(
        "optional".to_string(),
        FieldType::Option(Box::new(FieldType::U64)),
    )
    .unwrap();
    assert!(optional_field.validate(&FieldValue::Null).is_ok());
}

#[test]
fn test_validate_field_name() {
    // 有效的字段名
    assert!(validate_field_name("user_id").is_ok());
    assert!(validate_field_name("a").is_ok());
    assert!(validate_field_name("a1").is_ok());
    assert!(validate_field_name("a_1").is_ok());

    // 无效的字段名
    assert!(validate_field_name("").is_err()); // 空字符串
    assert!(validate_field_name("A").is_err()); // 大写字母
    assert!(validate_field_name("user-id").is_err()); // 包含连字符
    assert!(validate_field_name("user.id").is_err()); // 包含点
    assert!(validate_field_name("user id").is_err()); // 包含空格

    // 超长字段名
    let long_name = "a".repeat(65);
    assert!(validate_field_name(&long_name).is_err());
}

#[test]
fn test_serialization() {
    // 测试 FieldType 序列化和反序列化
    let field_type = Ft::Array(vec![Ft::U64, Ft::Text]);
    let serialized = serde_json::to_string(&field_type).unwrap();
    println!("Serialized FieldType: {serialized}");
    let deserialized: Ft = serde_json::from_str(&serialized).unwrap();
    assert_eq!(field_type, deserialized);
    let mut serialized = Vec::new();
    to_writer(&field_type, &mut serialized).unwrap();
    println!("Serialized FieldType: {:?}", hex::encode(&serialized));
    let deserialized: Ft = from_reader(&serialized[..]).unwrap();
    assert_eq!(field_type, deserialized);

    // 测试 FieldValue 序列化和反序列化
    let field_value = Fv::Array(vec![Fv::U64(1), Fv::Text("hello".to_string())]);
    let mut serialized = Vec::new();
    to_writer(&field_value, &mut serialized).unwrap();
    println!("Serialized FieldValue: {:?}", hex::encode(&serialized));
    assert_eq!(hex::encode(&serialized), "82016568656c6c6f");
    let deserialized: Fv = from_reader(&serialized[..]).unwrap();
    assert_eq!(field_value, deserialized);

    let field_value = Fv::Bytes(vec![1, 2, 3, 4]);
    let mut serialized = Vec::new();
    to_writer(&field_value, &mut serialized).unwrap();
    println!("Serialized bytes: {:?}", hex::encode(&serialized));
    assert_eq!(hex::encode(&serialized), "4401020304");
    let deserialized: Fv = from_reader(&serialized[..]).unwrap();
    assert_eq!(field_value, deserialized);

    // 测试 FieldEntry 序列化和反序列化
    let field_entry = Fe::new("id".to_string(), Ft::Bytes)
        .unwrap()
        .with_unique()
        .with_idx(0);
    let mut serialized = Vec::new();
    to_writer(&field_entry, &mut serialized).unwrap();
    let deserialized: Fe = from_reader(&serialized[..]).unwrap();
    assert_eq!(field_entry, deserialized);

    let xid = Xid([1u8; 12]);
    let mut data = Vec::new();
    to_writer(&xid, &mut data).unwrap();
    println!("Serialized Xid: {:?}", hex::encode(&data));
    assert_eq!(hex::encode(&data), "4c010101010101010101010101");
    let cb: Cbor = from_reader(&data[..]).unwrap();
    let fv: FieldValue = FieldValue::try_from(cb).unwrap();
    let deserialized_xid: Xid = fv.deserialized().unwrap();
    assert_eq!(xid, deserialized_xid);

    let vv = vec![
        [bf16::from_f32(1.0), bf16::from_f32(1.1)],
        [bf16::from_f32(2.0), bf16::from_f32(2.1)],
    ];
    // bf16 使用了 u16 存储，未提供 Ft 时将序列化成 u64
    let fv = Fv::serialized(&vv, None).unwrap();
    assert_eq!(
        fv,
        Fv::Array(vec![
            Fv::Array(vec![Fv::U64(16256), Fv::U64(16269)]),
            Fv::Array(vec![Fv::U64(16384), Fv::U64(16390),])
        ])
    );
    // 虽然 Fv 类型不对，但还是可以反序列化成 Vec<[bf16; 2]>
    let vv2: Vec<[bf16; 2]> = fv.deserialized().unwrap();
    assert_eq!(vv, vv2);

    // 提供了 Ft 后才能完全正确的序列化
    let fv = Fv::serialized(&vv, Some(&Ft::Array(vec![Ft::Vector]))).unwrap();
    assert_eq!(
        fv,
        Fv::Array(vec![
            Fv::Vector(vec![bf16::from_f32(1.0), bf16::from_f32(1.1)]),
            Fv::Vector(vec![bf16::from_f32(2.0), bf16::from_f32(2.1),])
        ])
    );
    let vv2: Vec<[bf16; 2]> = fv.deserialized().unwrap();
    assert_eq!(vv, vv2);
}

#[test]
fn test_nan_field_value_rejected_by_serde() {
    assert!(serde_json::to_string(&Fv::F64(f64::NAN)).is_err());
    assert!(serde_json::to_string(&Fv::F32(f32::NAN)).is_err());

    let mut serialized = Vec::new();
    to_writer(&Cbor::Float(f64::NAN), &mut serialized).unwrap();
    assert!(from_reader::<Fv, _>(&serialized[..]).is_err());
}

#[test]
fn field_type_and_key_helpers_cover_byte_keys_and_wildcards() {
    assert!(!FieldType::Text.allows_null());
    assert!(FieldType::Option(Box::new(FieldType::Text)).allows_null());
    assert!(
        FieldType::Array(vec![])
            .validate(&FieldValue::Array(vec![]))
            .is_ok()
    );

    let text_key = FieldKey::from("name".to_string());
    assert_eq!(text_key.field_type(), FieldType::Text);
    assert_eq!(text_key.as_bytes(), b"name");
    assert_eq!(text_key.to_string(), "name");

    let i64_key = FieldKey::from(-42_i64);
    assert_eq!(i64_key.field_type(), FieldType::I64);
    assert_eq!(i64_key.as_bytes(), &(-42_i64).to_ne_bytes());
    assert_eq!(i64_key.to_string(), "-42");
    assert_eq!(FieldKey::from(-2_isize), FieldKey::I64(-2));

    let bytes_from_vec = FieldKey::from(vec![1, 2, 3]);
    let bytes_from_array = FieldKey::from([4, 5, 6]);
    let bytes_from_slice = FieldKey::from(&[7, 8, 9][..]);
    assert_eq!(bytes_from_vec.field_type(), FieldType::Bytes);
    assert_eq!(bytes_from_vec.as_bytes(), &[1, 2, 3]);
    assert_eq!(bytes_from_array, FieldKey::Bytes(vec![4, 5, 6]));
    assert_eq!(bytes_from_slice, FieldKey::Bytes(vec![7, 8, 9]));
    assert_eq!(bytes_from_vec.to_string(), "AQID");

    assert_eq!(
        FieldKey::try_from(Value::Bytes(vec![10, 11])).unwrap(),
        FieldKey::Bytes(vec![10, 11])
    );
    assert_eq!(
        FieldKey::try_from(Value::Integer((-42).into())).unwrap(),
        FieldKey::I64(-42)
    );
    assert!(FieldKey::try_from(Value::Bool(true)).is_err());
    assert_eq!(*TEXT_WILDCARD_KEY, FieldKey::Text("*".to_string()));
    assert_eq!(*BYTES_WILDCARD_KEY, FieldKey::Bytes(b"*".to_vec()));
    assert_eq!(*I64_WILDCARD_KEY, FieldKey::I64(i64::MIN));
    assert!(TEXT_WILDCARD_KEY.is_wildcard());
    assert!(BYTES_WILDCARD_KEY.is_wildcard());
    assert!(I64_WILDCARD_KEY.is_wildcard());
    assert!(!FieldKey::from("**").is_wildcard());
    assert!(!FieldKey::from(b"").is_wildcard());
    assert!(!FieldKey::from(0_i64).is_wildcard());

    let wildcard_type = FieldType::Map(BTreeMap::from([(
        FieldKey::from(b"*".as_slice()),
        FieldType::U64,
    )]));
    let wildcard_value = FieldValue::Map(BTreeMap::from([
        (FieldKey::from(vec![0]), FieldValue::U64(1)),
        (FieldKey::from(vec![1]), FieldValue::U64(2)),
    ]));
    assert!(wildcard_type.validate(&wildcard_value).is_ok());

    let wildcard_cbor = Cbor::Map(vec![
        (Cbor::Bytes(vec![0]), Cbor::Integer(1.into())),
        (Cbor::Bytes(vec![1]), Cbor::Integer(2.into())),
    ]);
    assert_eq!(
        wildcard_type.extract(wildcard_cbor).unwrap(),
        wildcard_value
    );

    let wildcard_type = FieldType::Map(BTreeMap::from([(
        I64_WILDCARD_KEY.clone(),
        FieldType::Text,
    )]));
    let wildcard_value = FieldValue::Map(BTreeMap::from([
        (FieldKey::from(-1_i64), FieldValue::Text("neg".into())),
        (FieldKey::from(2_i64), FieldValue::Text("pos".into())),
    ]));
    assert!(wildcard_type.validate(&wildcard_value).is_ok());
    let wildcard_cbor = Cbor::Map(vec![
        (Cbor::Integer((-1).into()), Cbor::Text("neg".into())),
        (Cbor::Integer(2.into()), Cbor::Text("pos".into())),
    ]);
    assert_eq!(
        wildcard_type.extract(wildcard_cbor).unwrap(),
        wildcard_value
    );

    let fixed_type = FieldType::Map(BTreeMap::from([
        (FieldKey::from(b"id".as_slice()), FieldType::U64),
        (
            FieldKey::from(b"optional".as_slice()),
            FieldType::Option(Box::new(FieldType::Text)),
        ),
    ]));
    let missing_optional = FieldValue::Map(BTreeMap::from([(
        FieldKey::from(b"id".as_slice()),
        FieldValue::U64(9),
    )]));
    assert!(fixed_type.validate(&missing_optional).is_ok());
    let invalid_key = FieldValue::Map(BTreeMap::from([(
        FieldKey::from(b"unknown".as_slice()),
        FieldValue::U64(9),
    )]));
    assert!(fixed_type.validate(&invalid_key).is_err());
}

#[test]
fn field_value_from_impls_cover_collections_and_cbor_byte_map_branch() {
    assert_eq!(FieldValue::from(true), FieldValue::Bool(true));
    assert_eq!(FieldValue::from(-7_i64), FieldValue::I64(-7));
    assert_eq!(FieldValue::from(7_u64), FieldValue::U64(7));
    assert_eq!(FieldValue::from(1.5_f64), FieldValue::F64(1.5));
    assert_eq!(FieldValue::from(2.5_f32), FieldValue::F32(2.5));
    assert_eq!(
        FieldValue::from(vec![1_u8, 2, 3]),
        FieldValue::Bytes(vec![1, 2, 3])
    );
    assert_eq!(
        FieldValue::from("hello".to_string()),
        FieldValue::Text("hello".to_string())
    );
    assert_eq!(
        FieldValue::from(json!({"a": 1})),
        FieldValue::Json(json!({"a": 1}))
    );

    let vector = vec![bf16::from_f32(1.0), bf16::from_f32(2.0)];
    assert_eq!(FieldValue::from(vector.clone()), FieldValue::Vector(vector));

    let from_vec: FieldValue = vec![1_u64, 2_u64].into();
    assert_eq!(
        from_vec,
        FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)])
    );

    let mut ordered = BTreeSet::new();
    ordered.insert(1_u64);
    ordered.insert(2_u64);
    assert_eq!(FieldValue::from(ordered), from_vec);

    let mut unordered = HashSet::new();
    unordered.insert(1_u64);
    unordered.insert(2_u64);
    let mut unordered_values = match FieldValue::from(unordered) {
        FieldValue::Array(values) => values,
        other => panic!("expected array, got {other:?}"),
    };
    unordered_values.sort_by_key(|v| match v {
        FieldValue::U64(v) => *v,
        other => panic!("unexpected value {other:?}"),
    });
    assert_eq!(
        unordered_values,
        vec![FieldValue::U64(1), FieldValue::U64(2)]
    );

    let tree_map = BTreeMap::from([(FieldKey::from(vec![1, 2]), 9_u64)]);
    let tree_map_value = FieldValue::from(tree_map);
    assert_eq!(
        tree_map_value,
        FieldValue::Map(BTreeMap::from([(
            FieldKey::Bytes(vec![1, 2]),
            FieldValue::U64(9)
        )]))
    );
    let cbor: Cbor = tree_map_value.into();
    assert_eq!(
        cbor,
        Cbor::Map(vec![(Cbor::Bytes(vec![1, 2]), Cbor::Integer(9.into()))])
    );

    let tree_map = BTreeMap::from([(-7_i64, "lucky".to_string())]);
    let tree_map_value = FieldValue::from(tree_map);
    assert_eq!(
        tree_map_value,
        FieldValue::Map(BTreeMap::from([(
            FieldKey::I64(-7),
            FieldValue::Text("lucky".to_string())
        )]))
    );
    let cbor: Cbor = tree_map_value.into();
    assert_eq!(
        cbor,
        Cbor::Map(vec![(
            Cbor::Integer((-7).into()),
            Cbor::Text("lucky".to_string())
        )])
    );

    let hash_map = HashMap::from([("answer".to_string(), 42_u64)]);
    assert_eq!(
        FieldValue::from(hash_map),
        FieldValue::Map(BTreeMap::from([(
            FieldKey::Text("answer".to_string()),
            FieldValue::U64(42),
        )]))
    );

    let json_map = serde_json::Map::from_iter([("flag".to_string(), json!(true))]);
    assert_eq!(
        FieldValue::from(json_map),
        FieldValue::Map(BTreeMap::from([(
            FieldKey::Text("flag".to_string()),
            FieldValue::Json(json!(true)),
        )]))
    );

    assert_eq!(
        FieldValue::from(FieldKey::Text("key".to_string())),
        FieldValue::Text("key".to_string())
    );
    assert_eq!(FieldValue::from(FieldKey::I64(-7)), FieldValue::I64(-7));
    assert_eq!(
        FieldValue::from(FieldKey::Bytes(vec![1, 2])),
        FieldValue::Bytes(vec![1, 2])
    );
}

#[test]
fn field_value_try_from_impls_cover_success_and_error_paths() {
    assert!(bool::try_from(FieldValue::Bool(true)).unwrap());
    assert!(bool::try_from(&FieldValue::Bool(true)).unwrap());
    assert!(bool::try_from(FieldValue::Text("no".into())).is_err());
    assert!(bool::try_from(&FieldValue::Text("no".into())).is_err());

    let i64_value = FieldValue::I64(-9);
    assert_eq!(i64::try_from(i64_value.clone()).unwrap(), -9);
    assert_eq!(i64::try_from(&i64_value).unwrap(), -9);
    assert_eq!(<&i64>::try_from(&i64_value).unwrap(), &-9);
    // The U64 read-back shape converts when it fits in i64.
    assert_eq!(i64::try_from(FieldValue::U64(9)).unwrap(), 9);
    assert_eq!(i64::try_from(&FieldValue::U64(9)).unwrap(), 9);
    assert!(i64::try_from(FieldValue::U64(u64::MAX)).is_err());
    assert!(i64::try_from(&FieldValue::U64(u64::MAX)).is_err());
    // The reference conversion cannot reinterpret a U64 in place.
    assert!(<&i64>::try_from(&FieldValue::U64(9)).is_err());

    let u64_value = FieldValue::U64(9);
    assert_eq!(u64::try_from(u64_value.clone()).unwrap(), 9);
    assert_eq!(u64::try_from(&u64_value).unwrap(), 9);
    assert_eq!(<&u64>::try_from(&u64_value).unwrap(), &9);
    assert!(u64::try_from(FieldValue::I64(-9)).is_err());
    assert!(u64::try_from(&FieldValue::I64(-9)).is_err());
    assert!(<&u64>::try_from(&FieldValue::I64(-9)).is_err());

    assert_eq!(f64::try_from(FieldValue::F64(1.25)).unwrap(), 1.25);
    assert_eq!(f64::try_from(&FieldValue::F64(1.25)).unwrap(), 1.25);
    assert!(f64::try_from(FieldValue::F32(1.25)).is_err());
    assert!(f64::try_from(&FieldValue::F32(1.25)).is_err());

    assert_eq!(f32::try_from(FieldValue::F32(1.25)).unwrap(), 1.25);
    assert_eq!(f32::try_from(&FieldValue::F32(1.25)).unwrap(), 1.25);
    // The F64 read-back shapes convert: exact CBOR widening and the
    // JSON shortest-decimal round trip (see `is_f32_read_back`).
    assert_eq!(f32::try_from(FieldValue::F64(1.25)).unwrap(), 1.25);
    assert_eq!(f32::try_from(&FieldValue::F64(1.25)).unwrap(), 1.25);
    assert_eq!(f32::try_from(FieldValue::F64(2.71)).unwrap(), 2.71);
    assert_eq!(f32::try_from(&FieldValue::F64(2.71)).unwrap(), 2.71);
    // Not a possible F32 read-back: excess precision.
    assert!(f32::try_from(FieldValue::F64(2.7100000000001)).is_err());
    assert!(f32::try_from(&FieldValue::F64(2.7100000000001)).is_err());

    let bytes = FieldValue::Bytes(vec![1, 2, 3]);
    assert_eq!(Vec::<u8>::try_from(bytes.clone()).unwrap(), vec![1, 2, 3]);
    assert_eq!(<&Vec<u8>>::try_from(&bytes).unwrap(), &vec![1, 2, 3]);
    assert_eq!(<[u8; 3]>::try_from(bytes.clone()).unwrap(), [1, 2, 3]);
    assert!(<[u8; 2]>::try_from(bytes.clone()).is_err());
    assert!(Vec::<u8>::try_from(FieldValue::Text("bytes".into())).is_err());
    assert!(<&Vec<u8>>::try_from(&FieldValue::Text("bytes".into())).is_err());
    assert!(<[u8; 3]>::try_from(FieldValue::Text("bytes".into())).is_err());

    let text = FieldValue::Text("hello".to_string());
    assert_eq!(String::try_from(text.clone()).unwrap(), "hello");
    assert_eq!(<&String>::try_from(&text).unwrap(), "hello");
    assert_eq!(<&str>::try_from(&text).unwrap(), "hello");
    assert!(String::try_from(FieldValue::Bytes(vec![])).is_err());
    assert!(<&String>::try_from(&FieldValue::Bytes(vec![])).is_err());
    assert!(<&str>::try_from(&FieldValue::Bytes(vec![])).is_err());

    let json_value = FieldValue::Json(json!({"name": "Ada"}));
    assert_eq!(
        Json::try_from(json_value.clone()).unwrap(),
        json!({"name": "Ada"})
    );
    assert_eq!(
        <&Json>::try_from(&json_value).unwrap(),
        &json!({"name": "Ada"})
    );
    assert!(Json::try_from(FieldValue::Text("json".into())).is_err());
    assert!(<&Json>::try_from(&FieldValue::Text("json".into())).is_err());

    let vector = FieldValue::Vector(vec![bf16::from_f32(1.0), bf16::from_f32(2.0)]);
    assert_eq!(
        Vec::<bf16>::try_from(vector.clone()).unwrap(),
        vec![bf16::from_f32(1.0), bf16::from_f32(2.0),]
    );
    assert_eq!(
        <&Vec<bf16>>::try_from(&vector).unwrap(),
        &vec![bf16::from_f32(1.0), bf16::from_f32(2.0),]
    );
    assert_eq!(
        <[bf16; 2]>::try_from(vector.clone()).unwrap(),
        [bf16::from_f32(1.0), bf16::from_f32(2.0)]
    );
    assert!(<[bf16; 3]>::try_from(vector.clone()).is_err());
    // The Array(U64) read-back shape converts element-wise from bf16 bits.
    assert_eq!(
        Vec::<bf16>::try_from(FieldValue::Array(vec![
            FieldValue::U64(bf16::from_f32(1.0).to_bits() as u64),
            FieldValue::U64(bf16::from_f32(2.0).to_bits() as u64),
        ]))
        .unwrap(),
        vec![bf16::from_f32(1.0), bf16::from_f32(2.0)]
    );
    assert_eq!(
        Vec::<bf16>::try_from(FieldValue::Array(vec![])).unwrap(),
        vec![]
    );
    assert!(
        Vec::<bf16>::try_from(FieldValue::Array(vec![FieldValue::U64(
            u16::MAX as u64 + 1
        )]))
        .is_err()
    );
    assert!(Vec::<bf16>::try_from(FieldValue::Array(vec![FieldValue::I64(-1)])).is_err());
    assert!(<&Vec<bf16>>::try_from(&FieldValue::Array(vec![])).is_err());
    assert!(<[bf16; 2]>::try_from(FieldValue::Array(vec![])).is_err());

    let array = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
    assert_eq!(Vec::<u64>::try_from(array.clone()).unwrap(), vec![1, 2]);
    assert_eq!(Vec::<&u64>::try_from(&array).unwrap(), vec![&1, &2]);
    assert!(Vec::<u64>::try_from(FieldValue::U64(1)).is_err());
    assert!(Vec::<&u64>::try_from(&FieldValue::U64(1)).is_err());
    assert!(Vec::<u64>::try_from(FieldValue::Array(vec![FieldValue::Text("bad".into())])).is_err());
    assert!(
        Vec::<&u64>::try_from(&FieldValue::Array(vec![FieldValue::Text("bad".into())])).is_err()
    );

    let map = FieldValue::Map(BTreeMap::from([("id".into(), FieldValue::U64(42))]));
    let converted = BTreeMap::<FieldKey, u64>::try_from(map).unwrap();
    assert_eq!(converted.get(&FieldKey::Text("id".into())), Some(&42));
    assert!(BTreeMap::<FieldKey, u64>::try_from(FieldValue::U64(1)).is_err());
    assert!(
        BTreeMap::<FieldKey, u64>::try_from(FieldValue::Map(BTreeMap::from([(
            "bad".into(),
            FieldValue::Text("not u64".into()),
        )])))
        .is_err()
    );
}

#[test]
fn field_value_extract_error_branches_and_accessors_are_exercised() {
    assert!(FieldValue::i64_from(Cbor::Integer(u64::MAX.into())).is_err());
    assert!(FieldValue::i64_from(Cbor::Text("bad".into())).is_err());
    assert!(FieldValue::u64_from(Cbor::Integer((-1).into())).is_err());
    assert!(FieldValue::u64_from(Cbor::Text("bad".into())).is_err());
    assert!(FieldValue::f64_from(Cbor::Float(f64::NAN)).is_err());
    assert!(FieldValue::f32_from(Cbor::Float(f64::NAN)).is_err());
    assert!(FieldValue::json_from(Cbor::Map(vec![(Cbor::Bytes(vec![1]), Cbor::Null,)])).is_err());
    assert!(FieldValue::vector_from(Cbor::Text("bad".into())).is_err());
    assert!(FieldValue::bf16_from(Cbor::Integer((u64::from(u16::MAX) + 1).into())).is_err());
    assert!(FieldValue::bf16_from(Cbor::Text("bad".into())).is_err());
    assert!(FieldValue::array_from(Cbor::Text("bad".into()), &[]).is_err());
    assert!(
        FieldValue::array_from(
            Cbor::Array(vec![Cbor::Integer(1.into())]),
            &[FieldType::U64, FieldType::Text],
        )
        .is_err()
    );
    assert!(FieldValue::map_from(Cbor::Text("bad".into()), &BTreeMap::new()).is_err());
    assert_eq!(
        FieldValue::map_from(
            Cbor::Map(vec![(Cbor::Integer(1.into()), Cbor::Text("ok".into()))]),
            &BTreeMap::new(),
        )
        .unwrap(),
        FieldValue::Map(BTreeMap::from([(
            FieldKey::I64(1),
            FieldValue::Text("ok".into())
        )]))
    );
    assert!(
        FieldValue::map_from(
            Cbor::Map(vec![(Cbor::Integer(1.into()), Cbor::Text("bad".into()))]),
            &BTreeMap::from([("name".into(), FieldType::Text)]),
        )
        .is_err()
    );
    assert!(
        FieldValue::map_from(
            Cbor::Map(vec![(
                Cbor::Text("unknown".into()),
                Cbor::Text("bad".into())
            )]),
            &BTreeMap::from([("name".into(), FieldType::Text)]),
        )
        .is_err()
    );
    assert_eq!(
        FieldValue::try_from(Cbor::Tag(1, Box::new(Cbor::Text("tagged".to_string())),)).unwrap(),
        FieldValue::Text("tagged".to_string())
    );

    let map = FieldValue::Map(BTreeMap::from([(
        FieldKey::Text("name".into()),
        FieldValue::Text("Ada".into()),
    )]));
    assert_eq!(
        map.get_field_as::<str>(&FieldKey::Text("name".into())),
        Some("Ada")
    );
    assert_eq!(
        map.get_field_as::<str>(&FieldKey::Text("missing".into())),
        None
    );
    assert_eq!(
        FieldValue::Text("Ada".into()).get_field_as::<str>(&FieldKey::Text("name".into())),
        None
    );

    let mut entry = FieldEntry::new("nickname".to_string(), FieldType::Text)
        .unwrap()
        .with_description("Display name".to_string());
    assert_eq!(entry.name(), "nickname");
    assert_eq!(entry.r#type(), &FieldType::Text);
    assert!(entry.required());
    assert!(!entry.unique());
    assert_eq!(entry.idx(), 0);
    assert_eq!(entry.set_idx(3).idx(), 3);
    assert_eq!(
        entry.extract(Cbor::Text("Ada".to_string()), false).unwrap(),
        FieldValue::Text("Ada".into())
    );
    assert!(entry.extract(Cbor::Integer(1.into()), true).is_err());
}

#[test]
fn field_value_complexity_budget_rejects_deep_and_wide_values() {
    let budget = FieldValueBudget {
        max_depth: 2,
        max_nodes: 16,
        max_array_len: 2,
        max_map_entries: 2,
    };

    let ok = FieldValue::Array(vec![
        FieldValue::Text("a".into()),
        FieldValue::Map(BTreeMap::from([(
            FieldKey::Text("k".into()),
            FieldValue::Text("v".into()),
        )])),
    ]);
    ok.validate_complexity_with(budget).unwrap();

    let too_deep = FieldValue::Array(vec![FieldValue::Array(vec![FieldValue::Array(vec![
        FieldValue::Text("x".into()),
    ])])]);
    assert!(too_deep.validate_complexity_with(budget).is_err());

    let too_wide = FieldValue::Array(vec![
        FieldValue::Text("a".into()),
        FieldValue::Text("b".into()),
        FieldValue::Text("c".into()),
    ]);
    assert!(too_wide.validate_complexity_with(budget).is_err());

    let too_many_json_nodes = FieldValue::Json(serde_json::json!({
        "a": ["x", "y", "z"]
    }));
    assert!(
        too_many_json_nodes
            .validate_complexity_with(budget)
            .is_err()
    );
}

#[test]
fn f32_extract_rejects_out_of_range_f64() {
    // Values outside the finite f32 range must error instead of silently
    // becoming infinite.
    assert!(FieldType::F32.extract(Cbor::Float(1e308)).is_err());
    assert!(FieldType::F32.extract(Cbor::Float(-1e308)).is_err());
    assert!(FieldType::F32.extract(Cbor::Float(f64::MAX)).is_err());

    // Explicit infinities are representable and pass through.
    assert_eq!(
        FieldType::F32.extract(Cbor::Float(f64::INFINITY)).unwrap(),
        FieldValue::F32(f32::INFINITY)
    );
    assert_eq!(
        FieldType::F32
            .extract(Cbor::Float(f64::NEG_INFINITY))
            .unwrap(),
        FieldValue::F32(f32::NEG_INFINITY)
    );

    // Subnormal f64 values truncate (here to zero); truncation is allowed.
    assert_eq!(
        FieldType::F32.extract(Cbor::Float(1e-320)).unwrap(),
        FieldValue::F32(0.0)
    );
    // Ordinary precision truncation is allowed too.
    assert_eq!(
        FieldType::F32.extract(Cbor::Float(2.71)).unwrap(),
        FieldValue::F32(2.71f64 as f32)
    );
}

/// Builds `depth` levels of single-element CBOR arrays around a Bool.
fn deeply_nested_cbor(depth: usize) -> Cbor {
    let mut v = Cbor::Bool(true);
    for _ in 0..depth {
        v = Cbor::Array(vec![v]);
    }
    v
}

#[test]
fn deeply_nested_values_error_instead_of_overflowing_the_stack() {
    // Depths within the conversion bound still work.
    let ok = deeply_nested_cbor(MAX_CONVERSION_DEPTH);
    assert!(FieldValue::try_from(ok).is_ok());

    // Untyped conversion entry.
    let err = FieldValue::try_from(deeply_nested_cbor(2000)).unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));

    // Typed extraction entry (open-ended Array type).
    let err = FieldType::Array(vec![])
        .extract(deeply_nested_cbor(2000))
        .unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));

    // Deeply nested tags are bounded as well.
    let mut tagged = Cbor::Bool(true);
    for _ in 0..2000 {
        tagged = Cbor::Tag(1, Box::new(tagged));
    }
    let err = FieldValue::try_from(tagged).unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));

    // Nested maps are bounded too.
    let mut map = Cbor::Bool(true);
    for _ in 0..2000 {
        map = Cbor::Map(vec![(Cbor::Text("k".into()), map)]);
    }
    let err = FieldValue::try_from(map).unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));
}

/// Builds `depth` levels of single-element field arrays around a Bool.
fn deeply_nested_field_value(depth: usize) -> FieldValue {
    let mut v = FieldValue::Bool(true);
    for _ in 0..depth {
        v = FieldValue::Array(vec![v]);
    }
    v
}

#[test]
fn deeply_nested_values_convert_to_cbor_without_overflowing_the_stack() {
    // Regression: `From<FieldValue> for Cbor` and `json_to_cbor` used to
    // recurse unbounded, so the outbound direction aborted the process on
    // stack exhaustion where the inbound `FieldValue::try_from` returns an
    // error for the identical structure.
    let ok = deeply_nested_field_value(MAX_CONVERSION_DEPTH);
    let cbor = ok.clone().try_into_cbor().unwrap();
    assert_eq!(FieldValue::try_from(cbor).unwrap(), ok);

    let err = deeply_nested_field_value(2000).try_into_cbor().unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));

    // Nested maps are bounded too.
    let mut map = FieldValue::Bool(true);
    for _ in 0..2000 {
        map = FieldValue::Map(BTreeMap::from([(FieldKey::Text("k".into()), map)]));
    }
    let err = map.try_into_cbor().unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));

    // A `Json` payload carries its own nesting through `json_to_cbor`.
    let mut json = Json::Bool(true);
    for _ in 0..300 {
        json = Json::Array(vec![json]);
    }
    let err = FieldValue::Json(json).try_into_cbor().unwrap_err();
    assert!(err.to_string().contains("maximum nesting depth"));

    // The infallible `From` impl has no way to report the overflow, so it
    // truncates the over-deep subtree instead of exhausting the stack.
    let truncated = Cbor::from(deeply_nested_field_value(2000));
    let mut level = &truncated;
    for _ in 0..=MAX_CONVERSION_DEPTH {
        let Cbor::Array(items) = level else {
            panic!("expected an array at every level within the bound");
        };
        level = &items[0];
    }
    assert_eq!(level, &Cbor::Null);
}

#[test]
fn deeply_nested_serde_inputs_error_instead_of_overflowing_the_stack() {
    // The CBOR wire format is bounded by cbor2's recursion limit (256):
    // 2000 nested arrays are `0x81` headers followed by one `0xf5` (true).
    let mut bytes = vec![0x81u8; 2000];
    bytes.push(0xf5);
    assert!(from_reader::<Fv, _>(bytes.as_slice()).is_err());

    // JSON is bounded by serde_json's recursion limit (128).
    let deep_json = format!("{}true{}", "[".repeat(2000), "]".repeat(2000));
    assert!(serde_json::from_str::<Fv>(&deep_json).is_err());
}

#[test]
fn field_key_from_cbor_accepts_u8_arrays_like_bytes_from() {
    // `Vec<u8>` / `[u8; N]` map keys serialize as integer arrays.
    assert_eq!(
        FieldKey::try_from(Value::Array(vec![
            Value::Integer(1.into()),
            Value::Integer(2.into()),
        ]))
        .unwrap(),
        FieldKey::Bytes(vec![1, 2])
    );
    assert!(FieldKey::try_from(Value::Array(vec![Value::Integer(256.into())])).is_err());
    assert!(FieldKey::try_from(Value::Array(vec![Value::Text("x".into())])).is_err());

    let types = BTreeMap::from([(BYTES_WILDCARD_KEY.clone(), Ft::U64)]);
    let value = Cbor::Map(vec![(
        Cbor::Array(vec![Cbor::Integer(1.into()), Cbor::Integer(2.into())]),
        Cbor::Integer(7.into()),
    )]);
    assert_eq!(
        FieldValue::map_from(value, &types).unwrap(),
        Fv::Map(BTreeMap::from([(FieldKey::Bytes(vec![1, 2]), Fv::U64(7))]))
    );
}

#[test]
fn float_fields_accept_integers() {
    // JSON writes `1.0` as `1`: extract, validate, normalize and the
    // typed conversions all accept an integer for a float field.
    assert_eq!(
        FieldType::F64.extract(Cbor::Integer(1.into())).unwrap(),
        Fv::F64(1.0)
    );
    assert_eq!(
        FieldType::F64.extract(Cbor::Integer((-2).into())).unwrap(),
        Fv::F64(-2.0)
    );
    assert_eq!(
        FieldType::F32.extract(Cbor::Integer(3.into())).unwrap(),
        Fv::F32(3.0)
    );
    assert_eq!(
        FieldType::F64
            .extract(Cbor::Integer(u64::MAX.into()))
            .unwrap(),
        Fv::F64(u64::MAX as f64)
    );

    FieldType::F64.validate(&Fv::U64(1)).unwrap();
    FieldType::F64.validate(&Fv::I64(-1)).unwrap();
    FieldType::F32.validate(&Fv::U64(1)).unwrap();
    FieldType::F32.validate(&Fv::I64(i64::MIN)).unwrap();

    assert_eq!(
        read_back(&FieldType::F64, Fv::U64(1)).unwrap(),
        Fv::F64(1.0)
    );
    assert_eq!(
        read_back(&FieldType::F32, Fv::I64(-5)).unwrap(),
        Fv::F32(-5.0)
    );
    let v = Fv::Array(vec![Fv::U64(1), Fv::F64(2.5)]);
    assert_eq!(
        read_back(&FieldType::Array(vec![FieldType::F64]), v).unwrap(),
        Fv::Array(vec![Fv::F64(1.0), Fv::F64(2.5)])
    );

    assert_eq!(f64::try_from(Fv::U64(4)).unwrap(), 4.0);
    assert_eq!(f64::try_from(&Fv::I64(-4)).unwrap(), -4.0);
    assert_eq!(f32::try_from(Fv::U64(4)).unwrap(), 4.0);
    assert_eq!(f32::try_from(&Fv::I64(-4)).unwrap(), -4.0);

    // Integer fields still reject floats.
    assert!(FieldType::U64.validate(&Fv::F64(1.0)).is_err());
    assert!(FieldType::I64.extract(Cbor::Float(1.0)).is_err());

    // `F32` answers the same way whichever spelling of a value arrives:
    // 2^24 + 1 is not an f32, so both the integer and the float form are
    // rejected rather than one being silently rounded to 2^24.
    let inexact = 16_777_217u64;
    assert!(!is_f32_read_back(inexact as f64));
    assert!(FieldType::F32.validate(&Fv::F64(inexact as f64)).is_err());
    assert!(FieldType::F32.validate(&Fv::U64(inexact)).is_err());
    assert!(
        FieldType::F32
            .validate(&Fv::I64(-(inexact as i64)))
            .is_err()
    );
    assert!(
        FieldType::F32
            .extract(Cbor::Integer(inexact.into()))
            .is_err()
    );
    assert!(f32::try_from(Fv::U64(inexact)).is_err());
    assert!(f32::try_from(&Fv::I64(-(inexact as i64))).is_err());
    assert!(
        read_back(&FieldType::F32, Fv::U64(inexact)).is_err(),
        "an inexact integer is rejected on read"
    );

    // Exact ones — every |v| <= 2^24, and larger powers of two — pass.
    FieldType::F32.validate(&Fv::U64(1 << 24)).unwrap();
    FieldType::F32.validate(&Fv::I64(i64::MIN)).unwrap();
    assert_eq!(
        FieldType::F32
            .extract(Cbor::Integer((1u64 << 24).into()))
            .unwrap(),
        Fv::F32((1u64 << 24) as f32)
    );
    // `F64` keeps taking any integer: it accepts every non-NaN f64 too.
    FieldType::F64.validate(&Fv::U64(u64::MAX)).unwrap();
}

#[test]
fn validate_declaration_rejects_ambiguous_types() {
    let text_opt = FieldType::Option(Box::new(FieldType::Text));
    text_opt.validate_declaration().unwrap();
    FieldType::Array(vec![text_opt.clone(), FieldType::U64])
        .validate_declaration()
        .unwrap();
    FieldType::Map(BTreeMap::new())
        .validate_declaration()
        .unwrap();
    FieldType::Map(BTreeMap::from([(
        TEXT_WILDCARD_KEY.clone(),
        FieldType::U64,
    )]))
    .validate_declaration()
    .unwrap();
    FieldType::Map(BTreeMap::from([
        (FieldKey::from("a"), FieldType::U64),
        (FieldKey::from("b"), text_opt.clone()),
    ]))
    .validate_declaration()
    .unwrap();

    let nested_option = FieldType::Option(Box::new(text_opt.clone()));
    let err = nested_option.validate_declaration().unwrap_err();
    assert!(matches!(err, SchemaError::FieldType(_)), "{err}");
    assert!(
        FieldType::Array(vec![nested_option.clone()])
            .validate_declaration()
            .is_err()
    );
    assert!(
        FieldType::Map(BTreeMap::from([(FieldKey::from("k"), nested_option)]))
            .validate_declaration()
            .is_err()
    );

    let mixed = FieldType::Map(BTreeMap::from([
        (TEXT_WILDCARD_KEY.clone(), FieldType::U64),
        (FieldKey::from("a"), FieldType::U64),
    ]));
    let err = mixed.validate_declaration().unwrap_err();
    assert!(err.to_string().contains("wildcard"), "{err}");
    assert!(
        FieldType::Map(BTreeMap::from([
            (I64_WILDCARD_KEY.clone(), FieldType::U64),
            (FieldKey::from(1_i64), FieldType::U64),
        ]))
        .validate_declaration()
        .is_err()
    );

    let mut deep = FieldType::Text;
    for _ in 0..=MAX_CONVERSION_DEPTH {
        deep = FieldType::Array(vec![deep]);
    }
    assert!(deep.validate_declaration().is_err());

    // `FieldEntry::new` runs the check.
    assert!(FieldEntry::new("f".to_string(), FieldType::Option(Box::new(text_opt))).is_err());
}

#[test]
fn compatible_upgrade_allows_making_types_optional() {
    let opt = |ft: FieldType| FieldType::Option(Box::new(ft));
    assert!(opt(Ft::Text).is_compatible_upgrade_of(&Ft::Text));
    assert!(!Ft::Text.is_compatible_upgrade_of(&opt(Ft::Text)));
    assert!(!opt(Ft::U64).is_compatible_upgrade_of(&Ft::Text));

    // Nested: a required key may become optional, not the reverse.
    let old = Ft::Map(BTreeMap::from([(FieldKey::from("a"), Ft::U64)]));
    let new = Ft::Map(BTreeMap::from([(FieldKey::from("a"), opt(Ft::U64))]));
    assert!(new.is_compatible_upgrade_of(&old));
    assert!(!old.is_compatible_upgrade_of(&new));
    assert!(Ft::Array(vec![opt(Ft::Text)]).is_compatible_upgrade_of(&Ft::Array(vec![Ft::Text])));
}

#[test]
fn json_normalize_rebuilds_payload_without_cbor_round_trip() {
    let v = Fv::Map(BTreeMap::from([
        (
            FieldKey::from("a"),
            Fv::Array(vec![
                Fv::U64(1),
                Fv::I64(-1),
                Fv::F64(1.5),
                Fv::F32(0.5),
                Fv::Null,
                Fv::Bool(true),
            ]),
        ),
        (FieldKey::from("s"), Fv::Text("txt".into())),
        (FieldKey::from("v"), Fv::Vector(vec![bf16::from_f32(1.0)])),
        (FieldKey::from("j"), Fv::Json(serde_json::json!({"x": 1}))),
    ]));
    assert_eq!(
        read_back(&FieldType::Json, v).unwrap(),
        Fv::Json(serde_json::json!({
            "a": [1, -1, 1.5, 0.5, null, true],
            "s": "txt",
            "v": [bf16::from_f32(1.0).to_bits()],
            "j": {"x": 1},
        }))
    );

    // Mirrors the CBOR path: a non-finite float becomes JSON null.
    assert_eq!(
        read_back(&FieldType::Json, Fv::F64(f64::INFINITY)).unwrap(),
        Fv::Json(Json::Null)
    );

    // Shapes with no JSON representation are rejected.
    for v in [
        Fv::Bytes(vec![1]),
        Fv::Map(BTreeMap::from([(FieldKey::from(1_i64), Fv::U64(1))])),
        Fv::Array(vec![Fv::Bytes(vec![1])]),
    ] {
        assert!(read_back(&FieldType::Json, v).is_err());
    }
}
