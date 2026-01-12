use anda_db_derive::FieldTyped;
use anda_db_schema::{FieldKey, FieldType, Json};
use half::bf16;
use ic_auth_types::Xid;
use serde::{Deserialize, Serialize};
use serde_bytes::{ByteArray, ByteBuf};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Serialize, Deserialize, FieldTyped)]
struct User {
    name: String,
    age: u32,
    tags: HashMap<String, String>,         // 会被正确映射为 Map
    properties: BTreeMap<String, Vec<u8>>, // 会被正确映射为 Map 包含 Bytes

    attributes: Map<String, serde_json::Value>, // 会被正确映射为 Map 包含 Json

    #[field_type = "Map<String, Json>"]
    attributes2: Map<String, Value>, // 会被正确映射为 Map 包含 Json
    metadata: Map<String, Json>, // 会被正确映射为 Map 包含 Json

    #[field_type = "Option<Map<Bytes, F64>>"]
    optional_data: Option<HashMap<Xid, f64>>, // 会被正确映射为 Option<Map>
    vector1: Vec<bf16>, // 会被正确映射为 Vector

    #[serde(rename = "b1")]
    blob1: ByteArray<64>, // 会被正确映射为 Bytes
    blob2: ByteBuf, // 会被正确映射为 Bytes
}

#[derive(Debug, Serialize, Deserialize, FieldTyped)]
struct Doc {
    #[field_type = "Bytes"] // 将 Xid 类型映射为 FieldType::Bytes
    id: Xid,

    #[field_type = "Option<Array<Bytes>>"]
    #[serde(rename = "ids")]
    user_ids: Option<Vec<Xid>>,
    user: User,
}

#[test]
fn field_typed_derive_works() {
    let user_ft = User::field_type();
    assert_eq!(
        user_ft,
        FieldType::Map(
            vec![
                ("name".into(), FieldType::Text),
                ("age".into(), FieldType::U64),
                (
                    "tags".into(),
                    FieldType::Map(std::collections::BTreeMap::from([(
                        "*".into(),
                        FieldType::Text
                    )]))
                ),
                (
                    "properties".into(),
                    FieldType::Map(std::collections::BTreeMap::from([(
                        "*".into(),
                        FieldType::Bytes
                    )]))
                ),
                (
                    "attributes".into(),
                    FieldType::Map(std::collections::BTreeMap::from([(
                        "*".into(),
                        FieldType::Json
                    )]))
                ),
                (
                    "attributes2".into(),
                    FieldType::Map(std::collections::BTreeMap::from([(
                        "*".into(),
                        FieldType::Json
                    )]))
                ),
                (
                    "metadata".into(),
                    FieldType::Map(std::collections::BTreeMap::from([(
                        "*".into(),
                        FieldType::Json
                    )]))
                ),
                (
                    "optional_data".into(),
                    FieldType::Option(Box::new(FieldType::Map(std::collections::BTreeMap::from(
                        [(b"*".into(), FieldType::F64)]
                    ))))
                ),
                ("vector1".into(), FieldType::Vector),
                ("b1".into(), FieldType::Bytes),
                ("blob2".into(), FieldType::Bytes),
            ]
            .into_iter()
            .collect()
        )
    );

    let doc_ft = Doc::field_type();
    assert_eq!(
        doc_ft,
        FieldType::Map(
            vec![
                ("id".into(), FieldType::Bytes),
                (
                    "ids".into(),
                    FieldType::Option(Box::new(FieldType::Array(vec![FieldType::Bytes])))
                ),
                ("user".into(), user_ft),
            ]
            .into_iter()
            .collect()
        )
    );
}
