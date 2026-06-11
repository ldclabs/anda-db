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

#[derive(Debug, Serialize, Deserialize, FieldTyped)]
struct MapKeyAliases {
    // 通过 field_type 字符串使用 Text 关键字声明字符串键 Map(等价于 String)
    #[field_type = "Map<Text, Text>"]
    by_text: HashMap<String, String>,
    // 也允许带额外空格 / 嵌套类型
    #[field_type = "Map< Text , Array<U64> >"]
    nested: HashMap<String, Vec<u64>>,
    // 空白可以出现在 DSL 的任意位置。
    #[field_type = " Map < Text , Option < Array < Bytes > > > "]
    spaced: HashMap<String, Option<Vec<Xid>>>,
}

#[test]
fn map_text_alias_works() {
    let ft = MapKeyAliases::field_type();
    let expected = FieldType::Map(
        vec![
            (
                "by_text".into(),
                FieldType::Map(std::collections::BTreeMap::from([(
                    "*".into(),
                    FieldType::Text,
                )])),
            ),
            (
                "nested".into(),
                FieldType::Map(std::collections::BTreeMap::from([(
                    "*".into(),
                    FieldType::Array(vec![FieldType::U64]),
                )])),
            ),
            (
                "spaced".into(),
                FieldType::Map(std::collections::BTreeMap::from([(
                    "*".into(),
                    FieldType::Option(Box::new(FieldType::Array(vec![FieldType::Bytes]))),
                )])),
            ),
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(ft, expected);
}

// serde 容器级 rename_all 与字段级 skip:field_type() 必须描述序列化后的形态。
// 嵌套 Map 的键不受 AndaDB 顶层字段命名规则限制,camelCase 是合法的。
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, FieldTyped)]
#[serde(rename_all = "camelCase")]
struct RenamedNested {
    display_name: String,
    #[serde(skip)]
    local_state: u64,
    #[serde(skip_serializing)]
    more_state: u64,
}

#[test]
fn rename_all_and_skip_work() {
    assert_eq!(
        RenamedNested::field_type(),
        FieldType::Map(
            vec![("displayName".into(), FieldType::Text)]
                .into_iter()
                .collect()
        )
    );
}

// 泛型自定义类型作为嵌套字段:生成代码必须在表达式位置合法(<Wrapper<T>>::field_type())
#[allow(dead_code)]
#[derive(Debug, FieldTyped)]
struct Wrapper<T> {
    #[field_type = "Json"]
    value: T,
    label: String,
}

#[allow(dead_code)]
#[allow(clippy::box_collection)]
#[derive(Debug, FieldTyped)]
struct UsesGenericNested {
    wrapped: Wrapper<u64>,
    boxed: Box<String>,
}

#[test]
fn generic_nested_and_smart_pointers_work() {
    assert_eq!(
        UsesGenericNested::field_type(),
        FieldType::Map(
            vec![
                ("wrapped".into(), Wrapper::<u64>::field_type()),
                ("boxed".into(), FieldType::Text),
            ]
            .into_iter()
            .collect()
        )
    );
}

#[allow(dead_code)]
mod qualified_path_models {
    use super::*;

    pub mod nested {
        use super::*;

        #[derive(Debug, FieldTyped)]
        pub struct Profile {
            pub nickname: std::string::String,
        }
    }

    #[derive(Debug, FieldTyped)]
    pub struct UsesQualifiedPaths<'a, T> {
        pub profile: nested::Profile,
        pub profiles: std::vec::Vec<nested::Profile>,
        pub by_name:
            std::collections::BTreeMap<std::string::String, std::vec::Vec<std::primitive::u8>>,
        pub json: serde_json::Value,
        pub bytes: serde_bytes::ByteBuf,
        pub borrowed_text: &'a str,
        pub borrowed_bytes: &'a [u8],
        #[field_type = "Json"]
        pub payload: T,
    }
}

#[test]
fn qualified_paths_and_generic_structs_work() {
    let profile_ft = qualified_path_models::nested::Profile::field_type();
    assert_eq!(
        profile_ft,
        FieldType::Map(
            vec![("nickname".into(), FieldType::Text)]
                .into_iter()
                .collect()
        )
    );

    let ft = qualified_path_models::UsesQualifiedPaths::<Json>::field_type();
    assert_eq!(
        ft,
        FieldType::Map(
            vec![
                ("profile".into(), profile_ft.clone()),
                ("profiles".into(), FieldType::Array(vec![profile_ft])),
                (
                    "by_name".into(),
                    FieldType::Map(std::collections::BTreeMap::from([(
                        "*".into(),
                        FieldType::Bytes,
                    )]))
                ),
                ("json".into(), FieldType::Json),
                ("bytes".into(), FieldType::Bytes),
                ("borrowed_text".into(), FieldType::Text),
                ("borrowed_bytes".into(), FieldType::Bytes),
                ("payload".into(), FieldType::Json),
            ]
            .into_iter()
            .collect()
        )
    );
}
