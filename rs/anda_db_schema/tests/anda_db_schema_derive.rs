use anda_db_derive::{AndaDBSchema, FieldTyped};
use anda_db_schema::{FieldEntry, FieldKey, FieldType, Json, Schema, SchemaError};
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct TestUser {
    /// User's unique username handle
    #[unique]
    handle: String,
    /// User's display name
    name: String,
    /// User's age in years
    age: Option<u64>,
    /// Whether the user account is active
    active: bool,
    /// User tags for categorization
    tags: Vec<String>,
    /// User metadata with creation and update timestamps
    #[serde(rename = "metadata")]
    meta: Option<BTreeMap<String, u64>>,
}

// 测试包含 _id 字段的结构体
#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct TestUserWithId {
    _id: u64,
    username: String,
    email: String,
}

// // 测试包含错误类型 _id 字段的结构体
// #[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
// struct TestUserWithStringId {
//     _id: String,
//     username: String,
//     email: String,
// }

// 测试各种数据类型
#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct TestAllTypes {
    // 数字类型
    byte_val: u8,
    short_val: u16,
    int_val: u32,
    long_val: u64,
    signed_byte: i8,
    signed_short: i16,
    signed_int: i32,
    signed_long: i64,
    float_val: f32,
    double_val: f64,

    // 文本类型
    text: String,

    // 布尔类型
    flag: bool,

    // 字节数组
    data: Vec<u8>,
    array: [u8; 32],
    opt_array: Option<[u8; 32]>,

    // 数组类型
    numbers: Vec<i32>,
    strings: Vec<String>,

    // 可选类型
    optional_text: Option<String>,
    optional_number: Option<i64>,

    // Map 类型
    string_map: BTreeMap<String, String>,
    number_map: BTreeMap<String, i64>,
    json_map: Map<String, Json>,
    json_map2: Map<String, serde_json::Value>,
}

// 测试自定义字段类型属性
#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct TestCustomFieldType {
    #[field_type = "Json"]
    custom_field: String,
    #[field_type = "Bytes"]
    binary_data: String,
    #[field_type = "Vector"]
    embedding: Vec<f32>,
}

// 测试重命名和唯一性约束
#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct TestConstraints {
    #[unique]
    #[serde(rename = "user_id")]
    id: String,

    #[unique]
    email: String,

    /// User's full name with description
    #[serde(rename = "full_name")]
    name: String,

    /// Optional bio information
    bio: Option<String>,
}

// 测试 serde 容器级 rename_all 与字段级 rename/skip:schema 字段名必须与序列化结果一致。
// 注:AndaDB 顶层字段名仅允许 [a-z0-9_],因此 camelCase 等规则会被编译期拒绝;
// snake_case/lowercase 是合法的。
#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
#[serde(rename_all = "snake_case")]
struct TestRenameAll {
    _id: u64,
    /// Creation timestamp
    created_at: u64,
    /// Runtime-only state, never serialized
    #[allow(dead_code)]
    #[serde(skip)]
    runtime_cache: Option<String>,
    #[serde(rename = "explicit_name")]
    some_field: String,
}

// 测试 serde 透明智能指针的类型推断
// (注:Arc 的 serde 序列化需启用 serde 的 `rc` feature,此处仅验证 schema 推断)
#[allow(dead_code)]
#[allow(clippy::box_collection)]
#[derive(Debug, AndaDBSchema)]
struct TestSmartPointers {
    _id: u64,
    boxed_text: Box<String>,
    shared_text: std::sync::Arc<String>,
    cow_text: std::borrow::Cow<'static, str>,
    boxed_bytes: Box<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct TestQualifiedPathSchema<T> {
    _id: std::primitive::u64,
    title: std::string::String,
    tags: std::option::Option<std::vec::Vec<std::string::String>>,
    lookup: std::collections::HashMap<std::string::String, std::primitive::u64>,
    #[field_type = " Json "]
    payload: T,
    #[serde(rename(serialize = "public_name", deserialize = "input_name"))]
    renamed: String,
    #[serde(rename(deserialize = "input_only"))]
    deserialize_only: String,
}

#[derive(Clone, Debug, Default, PartialEq, cbor2::Cbor, FieldTyped)]
struct SimplifiedClaims {
    #[cbor(key = 1)]
    #[serde(rename = "iss", skip_serializing_if = "Option::is_none", default)]
    issuer: Option<String>,
    #[cbor(key = 4)]
    #[serde(rename = "exp", skip_serializing_if = "Option::is_none", default)]
    expiration: Option<u64>,
    #[cbor(key = 7)]
    #[serde(
        rename = "cti",
        with = "serde_bytes",
        skip_serializing_if = "Option::is_none",
        default
    )]
    cwt_id: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, AndaDBSchema)]
struct TestClaimsAsValue {
    _id: u64,
    claims: SimplifiedClaims,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db_schema::{Document, Fv, TEXT_WILDCARD_KEY};

    #[test]
    fn test_generated_schema() {
        let schema = TestUser::schema().unwrap();
        println!("{schema:#?}");

        // 验证字段数量 (包含 _id 字段)
        assert_eq!(schema.len(), 7);

        // 验证 handle 字段
        let handle_field = schema.get_field("handle").unwrap();
        assert_eq!(handle_field.r#type(), &FieldType::Text);
        assert!(handle_field.unique());
        assert!(handle_field.required());

        // 验证 name 字段
        let name_field = schema.get_field("name").unwrap();
        assert_eq!(name_field.r#type(), &FieldType::Text);
        assert!(!name_field.unique());
        assert!(name_field.required());

        // 验证 age 字段 (Optional)
        let age_field = schema.get_field("age").unwrap();
        if let FieldType::Option(inner) = age_field.r#type() {
            assert_eq!(**inner, FieldType::U64);
        } else {
            panic!("Expected Option<U64>");
        }
        assert!(!age_field.required());

        // 验证 active 字段
        let active_field = schema.get_field("active").unwrap();
        assert_eq!(active_field.r#type(), &FieldType::Bool);
        assert!(active_field.required());

        // 验证 tags 字段
        let tags_field = schema.get_field("tags").unwrap();
        if let FieldType::Array(types) = tags_field.r#type() {
            assert_eq!(types.len(), 1);
            assert_eq!(types[0], FieldType::Text);
        } else {
            panic!("Expected Array<Text>");
        }

        // 验证 meta 字段 (重命名为 metadata)
        let meta_field = schema.get_field("metadata").unwrap();
        if let FieldType::Option(inner) = meta_field.r#type() {
            if let FieldType::Map(map_types) = inner.as_ref() {
                assert_eq!(map_types.len(), 1);
                assert_eq!(map_types.get(&TEXT_WILDCARD_KEY), Some(&FieldType::U64));
            } else {
                panic!("Expected Map");
            }
        } else {
            panic!("Expected Option<Map>");
        }
    }

    #[test]
    fn test_schema_with_id_field() {
        let schema = TestUserWithId::schema().unwrap();

        assert_eq!(schema.len(), 3);

        // 验证 username 字段
        let username_field = schema.get_field("username").unwrap();
        assert_eq!(username_field.r#type(), &FieldType::Text);

        // 验证 email 字段
        let email_field = schema.get_field("email").unwrap();
        assert_eq!(email_field.r#type(), &FieldType::Text);

        // 确认 _id 字段在 schema 中
        assert!(schema.get_field("_id").is_some());
    }

    #[test]
    fn test_all_data_types() {
        let schema = TestAllTypes::schema().unwrap();

        // 验证数字类型
        assert_eq!(
            schema.get_field("byte_val").unwrap().r#type(),
            &FieldType::U64
        );
        assert_eq!(
            schema.get_field("short_val").unwrap().r#type(),
            &FieldType::U64
        );
        assert_eq!(
            schema.get_field("int_val").unwrap().r#type(),
            &FieldType::U64
        );
        assert_eq!(
            schema.get_field("long_val").unwrap().r#type(),
            &FieldType::U64
        );

        assert_eq!(
            schema.get_field("signed_byte").unwrap().r#type(),
            &FieldType::I64
        );
        assert_eq!(
            schema.get_field("signed_short").unwrap().r#type(),
            &FieldType::I64
        );
        assert_eq!(
            schema.get_field("signed_int").unwrap().r#type(),
            &FieldType::I64
        );
        assert_eq!(
            schema.get_field("signed_long").unwrap().r#type(),
            &FieldType::I64
        );

        assert_eq!(
            schema.get_field("float_val").unwrap().r#type(),
            &FieldType::F32
        );
        assert_eq!(
            schema.get_field("double_val").unwrap().r#type(),
            &FieldType::F64
        );

        // 文本类型
        assert_eq!(schema.get_field("text").unwrap().r#type(), &FieldType::Text);

        // 布尔类型
        assert_eq!(schema.get_field("flag").unwrap().r#type(), &FieldType::Bool);

        // 字节数组
        assert_eq!(
            schema.get_field("data").unwrap().r#type(),
            &FieldType::Bytes
        );
        assert_eq!(
            schema.get_field("array").unwrap().r#type(),
            &FieldType::Bytes
        );
        if let FieldType::Option(inner) = schema.get_field("opt_array").unwrap().r#type() {
            assert_eq!(inner.as_ref(), &FieldType::Bytes);
        } else {
            panic!("Expected Option<Bytes>");
        }

        // 数组类型
        if let FieldType::Array(types) = schema.get_field("numbers").unwrap().r#type() {
            assert_eq!(types, &vec![FieldType::I64]);
        } else {
            panic!("Expected Array<I64>");
        }
        if let FieldType::Array(types) = schema.get_field("strings").unwrap().r#type() {
            assert_eq!(types, &vec![FieldType::Text]);
        } else {
            panic!("Expected Array<Text>");
        }

        // 可选类型
        if let FieldType::Option(inner) = schema.get_field("optional_text").unwrap().r#type() {
            assert_eq!(inner.as_ref(), &FieldType::Text);
        } else {
            panic!("Expected Option<Text>");
        }
        if let FieldType::Option(inner) = schema.get_field("optional_number").unwrap().r#type() {
            assert_eq!(inner.as_ref(), &FieldType::I64);
        } else {
            panic!("Expected Option<I64>");
        }

        // Map 类型
        if let FieldType::Map(map_types) = schema.get_field("string_map").unwrap().r#type() {
            assert_eq!(map_types.get(&TEXT_WILDCARD_KEY), Some(&FieldType::Text));
        } else {
            panic!("Expected Map<Text>");
        }
        if let FieldType::Map(map_types) = schema.get_field("number_map").unwrap().r#type() {
            assert_eq!(map_types.get(&TEXT_WILDCARD_KEY), Some(&FieldType::I64));
        } else {
            panic!("Expected Map<I64>");
        }
        if let FieldType::Map(map_types) = schema.get_field("json_map").unwrap().r#type() {
            assert_eq!(map_types.get(&TEXT_WILDCARD_KEY), Some(&FieldType::Json));
        } else {
            panic!("Expected Map<Json>");
        }
        if let FieldType::Map(map_types) = schema.get_field("json_map2").unwrap().r#type() {
            assert_eq!(map_types.get(&TEXT_WILDCARD_KEY), Some(&FieldType::Json));
        } else {
            panic!("Expected Map<Json>");
        }
    }

    #[test]
    fn test_custom_field_type_attributes() {
        let schema = TestCustomFieldType::schema().unwrap();

        assert_eq!(
            schema.get_field("custom_field").unwrap().r#type(),
            &FieldType::Json
        );
        assert_eq!(
            schema.get_field("binary_data").unwrap().r#type(),
            &FieldType::Bytes
        );
        assert_eq!(
            schema.get_field("embedding").unwrap().r#type(),
            &FieldType::Vector
        );
    }

    #[test]
    fn test_constraints_and_rename() {
        let schema = TestConstraints::schema().unwrap();

        // 字段名应被重命名为 user_id
        let id_field = schema.get_field("user_id").unwrap();
        assert_eq!(id_field.r#type(), &FieldType::Text);
        assert!(id_field.unique());

        let email_field = schema.get_field("email").unwrap();
        assert_eq!(email_field.r#type(), &FieldType::Text);
        assert!(email_field.unique());

        let name_field = schema.get_field("full_name").unwrap();
        assert_eq!(name_field.r#type(), &FieldType::Text);
        assert!(!name_field.unique());

        let bio_field = schema.get_field("bio").unwrap();
        if let FieldType::Option(inner) = bio_field.r#type() {
            assert_eq!(inner.as_ref(), &FieldType::Text);
        } else {
            panic!("Expected Option<Text>");
        }
    }

    #[test]
    fn test_rename_all_and_skip_match_serialization() {
        let schema = TestRenameAll::schema().unwrap();

        // schema 字段名跟随序列化名(rename_all / 显式 rename)
        assert!(schema.get_field("created_at").is_some());
        assert!(schema.get_field("explicit_name").is_some());
        assert!(schema.get_field("some_field").is_none());

        // skip 字段不进入 schema
        assert!(schema.get_field("runtime_cache").is_none());

        // 端到端:serde 序列化出的文档能通过 schema 校验,skip 字段不出现
        let value = TestRenameAll {
            _id: 1,
            created_at: 42,
            runtime_cache: Some("not stored".into()),
            some_field: "hello".into(),
        };
        let schema = std::sync::Arc::new(schema);
        let doc = anda_db_schema::Document::try_from(schema, &value).unwrap();
        assert_eq!(
            doc.get_field("created_at"),
            Some(&anda_db_schema::Fv::U64(42))
        );
        assert_eq!(
            doc.get_field("explicit_name"),
            Some(&anda_db_schema::Fv::Text("hello".into()))
        );
        assert!(doc.get_field("runtime_cache").is_none());
    }

    #[test]
    fn test_smart_pointer_inference() {
        let schema = TestSmartPointers::schema().unwrap();

        assert_eq!(
            schema.get_field("boxed_text").unwrap().r#type(),
            &FieldType::Text
        );
        assert_eq!(
            schema.get_field("shared_text").unwrap().r#type(),
            &FieldType::Text
        );
        assert_eq!(
            schema.get_field("cow_text").unwrap().r#type(),
            &FieldType::Text
        );
        assert_eq!(
            schema.get_field("boxed_bytes").unwrap().r#type(),
            &FieldType::Bytes
        );
    }

    #[test]
    fn test_schema_error() {
        // Schema 至少包含 `_id` 字段（builder 默认注入），这里验证查找不存在字段的行为
        let schema = Schema::builder().build().unwrap();

        assert!(schema.get_field("non_existent").is_none());
        assert!(schema.get_field_or_err("non_existent").is_err());
    }

    #[test]
    fn test_qualified_paths_generics_and_directional_serde_rename() {
        let schema = TestQualifiedPathSchema::<Json>::schema().unwrap();

        assert_eq!(
            schema.get_field("title").unwrap().r#type(),
            &FieldType::Text
        );
        if let FieldType::Option(inner) = schema.get_field("tags").unwrap().r#type() {
            assert_eq!(inner.as_ref(), &FieldType::Array(vec![FieldType::Text]));
        } else {
            panic!("Expected Option<Array<Text>>");
        }
        if let FieldType::Map(map_types) = schema.get_field("lookup").unwrap().r#type() {
            assert_eq!(map_types.get(&TEXT_WILDCARD_KEY), Some(&FieldType::U64));
        } else {
            panic!("Expected Map<U64>");
        }
        assert_eq!(
            schema.get_field("payload").unwrap().r#type(),
            &FieldType::Json
        );
        assert!(schema.get_field("public_name").is_some());
        assert!(schema.get_field("input_name").is_none());
        assert!(schema.get_field("deserialize_only").is_some());
        assert!(schema.get_field("input_only").is_none());
    }

    #[test]
    fn cbor_claims_can_be_used_as_nested_schema_value_type() {
        let claims_type = FieldType::Map(BTreeMap::from([
            (
                FieldKey::from(1_i64),
                FieldType::Option(Box::new(FieldType::Text)),
            ),
            (
                FieldKey::from(4_i64),
                FieldType::Option(Box::new(FieldType::U64)),
            ),
            (
                FieldKey::from(7_i64),
                FieldType::Option(Box::new(FieldType::Bytes)),
            ),
        ]));
        assert_eq!(SimplifiedClaims::field_type(), claims_type);

        let schema = TestClaimsAsValue::schema().unwrap();
        assert_eq!(schema.get_field("claims").unwrap().r#type(), &claims_type);

        let value = TestClaimsAsValue {
            _id: 9,
            claims: SimplifiedClaims {
                issuer: Some("coap://as.example.com".into()),
                expiration: Some(1_444_064_944),
                cwt_id: Some(vec![0x0b, 0x71]),
            },
        };

        let doc = Document::try_from(std::sync::Arc::new(schema), &value).unwrap();
        assert_eq!(
            doc.get_field("claims"),
            Some(&Fv::Map(BTreeMap::from([
                (
                    FieldKey::from(1_i64),
                    Fv::Text("coap://as.example.com".into()),
                ),
                (FieldKey::from(4_i64), Fv::U64(1_444_064_944)),
                (FieldKey::from(7_i64), Fv::Bytes(vec![0x0b, 0x71])),
            ])))
        );

        let decoded: TestClaimsAsValue = doc.try_into().unwrap();
        assert_eq!(decoded, value);
    }
}
