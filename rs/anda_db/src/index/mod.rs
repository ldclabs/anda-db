use anda_db_schema::{Document, Fv, Json, Vector, bf16};
use cbor2::to_canonical_vec;
use std::borrow::Cow;

mod bm25;
mod btree;
mod hnsw;

pub use bm25::*;
pub use btree::*;
pub use hnsw::*;

/// Customization point for deriving indexable values from stored documents.
///
/// The default implementation indexes physical fields directly. Applications
/// that need virtual composite keys, normalized full-text content, or alternate
/// vector encodings can provide their own hook implementation and install it
/// with `Collection::set_index_hooks`.
pub trait IndexHooks: Send + Sync {
    /// Returns the value to insert into a B-tree index for `doc`.
    ///
    /// The default implementation returns a borrowed single-field value or a
    /// deterministic CBOR byte key for multi-field virtual indexes.
    fn btree_index_value<'a>(&self, index: &BTree, doc: &'a Document) -> Option<Cow<'a, Fv>> {
        let fields = index.virtual_field();
        match fields {
            [] => None,
            [name] => doc.get_field(name).map(Cow::Borrowed),
            _ => {
                let mut vals: Vec<Option<&Fv>> = Vec::with_capacity(fields.len());
                for name in fields {
                    vals.push(doc.get_field(name));
                }

                virtual_field_value(&vals).map(Cow::Owned)
            }
        }
    }

    /// Returns searchable text to insert into a BM25 index for `doc`.
    ///
    /// The default implementation extracts text from all configured fields and
    /// joins multiple text fragments with newline separators.
    fn bm25_index_value<'a>(&self, index: &BM25, doc: &'a Document) -> Option<Cow<'a, str>> {
        let fields = index.virtual_field();
        let mut vals: Vec<Option<&Fv>> = Vec::with_capacity(fields.len());
        for name in fields {
            vals.push(doc.get_field(name));
        }

        virtual_searchable_text(&vals)
    }

    /// Returns the vector to insert into an HNSW index for `doc`.
    ///
    /// The default implementation accepts native vector fields and the compact
    /// array-of-bf16-bits representation used by serialized documents.
    fn hnsw_index_value<'a>(&self, index: &Hnsw, doc: &'a Document) -> Option<Cow<'a, Vector>> {
        match doc.get_field(index.field_name()) {
            Some(Fv::Vector(vector)) => Some(Cow::Borrowed(vector)),
            Some(Fv::Array(values)) => {
                let vector = values
                    .iter()
                    .map(|value| match value {
                        Fv::U64(bits) => u16::try_from(*bits).ok().map(bf16::from_bits),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(Cow::Owned(vector))
            }
            _ => None,
        }
    }
}

/// Builds the stable index name for a single-field or multi-field index.
pub fn virtual_field_name(fields: &[&str]) -> String {
    fields.join("-")
}

/// Splits an index name back into the field list it represents.
pub fn from_virtual_field_name(name: &str) -> Vec<String> {
    name.split('-').map(String::from).collect()
}

/// Builds a deterministic byte key for a multi-field B-tree index.
///
/// Each field value, including `None`, is encoded as deterministic CBOR and
/// concatenated so the resulting key has stable ordering and equality.
pub fn virtual_field_value(vals: &[Option<&Fv>]) -> Option<Fv> {
    if vals.is_empty() {
        return None;
    }
    let mut data = Vec::new();
    for val in vals {
        data.extend(to_canonical_vec(val).ok()?);
    }
    Some(Fv::Bytes(data))
}

/// Extracts searchable text from one or more field values.
///
/// Text inside arrays, maps, and JSON values is recursively collected. A single
/// text fragment is borrowed; multiple fragments are joined into an owned
/// newline-separated string.
pub fn virtual_searchable_text<'a>(vals: &[Option<&'a Fv>]) -> Option<Cow<'a, str>> {
    let mut texts: Vec<&str> = Vec::new();
    for val in vals.iter().flatten() {
        extract_text(&mut texts, val)
    }

    match texts.len() {
        0 => None,
        1 => Some(Cow::Borrowed(texts[0])),
        _ => Some(Cow::Owned(texts.join("\n"))),
    }
}

fn extract_text<'a>(texts: &mut Vec<&'a str>, val: &'a Fv) {
    match val {
        Fv::Text(text) => texts.push(text),
        Fv::Array(vals) => {
            for val in vals {
                extract_text(texts, val);
            }
        }
        Fv::Map(vals) => {
            for val in vals.values() {
                extract_text(texts, val);
            }
        }
        Fv::Json(json) => extract_json_text(texts, json),
        _ => {}
    }
}

/// Recursively appends text values found inside JSON data.
///
/// Arrays whose first element is not a string or object are treated as scalar
/// arrays and skipped to avoid indexing arbitrary numeric/vector payloads.
pub fn extract_json_text<'a>(texts: &mut Vec<&'a str>, val: &'a Json) {
    match val {
        Json::String(s) => texts.push(s),
        Json::Object(obj) => {
            for val in obj.values() {
                extract_json_text(texts, val);
            }
        }
        Json::Array(arr) => {
            if !arr.is_empty() && !matches!(arr[0], Json::String(_) | Json::Object(_)) {
                return;
            }

            for val in arr {
                extract_json_text(texts, val);
            }
        }
        _ => {}
    }
}

/// Default physical-field indexing behavior.
pub struct DefaultIndexHooks;

impl IndexHooks for DefaultIndexHooks {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Storage, StorageConfig};
    use anda_db_schema::{Fe, Ft, Fv, Schema};
    use object_store::memory::InMemory;
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Arc};

    #[test]
    fn test_virtual_searchable_text_empty() {
        // 测试空输入
        let result = virtual_searchable_text(&[]);
        assert_eq!(result, None);

        // 测试全为 None 的输入
        let result = virtual_searchable_text(&[None, None, None]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_virtual_searchable_text_single_text() {
        // 测试单个文本字段
        let text_val = Fv::Text("Hello World".to_string());
        let result = virtual_searchable_text(&[Some(&text_val)]);
        assert_eq!(result, Some(Cow::Borrowed("Hello World")));
    }

    #[test]
    fn test_virtual_searchable_text_multiple_texts() {
        // 测试多个文本字段
        let text1 = Fv::Text("Hello".to_string());
        let text2 = Fv::Text("World".to_string());
        let text3 = Fv::Text("Test".to_string());

        let result = virtual_searchable_text(&[Some(&text1), Some(&text2), Some(&text3)]);
        assert_eq!(result, Some(Cow::Owned("Hello\nWorld\nTest".to_string())));
    }

    #[test]
    fn test_virtual_searchable_text_with_array() {
        // 测试包含数组的字段
        let array_val = Fv::Array(vec![
            Fv::Text("item1".to_string()),
            Fv::Text("item2".to_string()),
            Fv::I64(123), // 非文本类型应该被忽略
        ]);

        let result = virtual_searchable_text(&[Some(&array_val)]);
        assert_eq!(result, Some(Cow::Owned("item1\nitem2".to_string())));
    }

    #[test]
    fn test_virtual_searchable_text_with_map() {
        // 测试包含 Map 的字段
        let mut map = BTreeMap::new();
        map.insert("key1".into(), Fv::Text("value1".to_string()));
        map.insert("key2".into(), Fv::Text("value2".to_string()));
        map.insert("key3".into(), Fv::I64(456)); // 非文本类型应该被忽略

        let map_val = Fv::Map(map);
        let result = virtual_searchable_text(&[Some(&map_val)]);

        // 由于 BTreeMap 的顺序是确定的，我们可以预期结果
        assert_eq!(result, Some(Cow::Owned("value1\nvalue2".to_string())));
    }

    #[test]
    fn test_virtual_searchable_text_with_json() {
        // 测试包含 JSON 的字段
        let json_val = Fv::Json(json!({
            "name": "John",
            "age": 30,
            "city": "New York",
            "hobbies": ["reading", "swimming"]
        }));

        let result = virtual_searchable_text(&[Some(&json_val)]);
        assert!(result.is_some());
        let text = result.unwrap();

        // JSON 中的字符串应该被提取
        assert!(text.contains("John"));
        assert!(text.contains("New York"));
        assert!(text.contains("reading"));
        assert!(text.contains("swimming"));
    }

    #[test]
    fn test_virtual_searchable_text_json_array_mixed_types() {
        // 测试 JSON 数组包含混合类型（应该被忽略）
        let json_val = Fv::Json(json!([1, 2, 3, true, false]));
        let result = virtual_searchable_text(&[Some(&json_val)]);
        assert_eq!(result, None);
    }

    #[test]
    fn test_virtual_searchable_text_json_array_strings() {
        // 测试 JSON 数组只包含字符串
        let json_val = Fv::Json(json!(["apple", "banana", "cherry"]));
        let result = virtual_searchable_text(&[Some(&json_val)]);
        assert_eq!(
            result,
            Some(Cow::Owned("apple\nbanana\ncherry".to_string()))
        );
    }

    #[test]
    fn test_virtual_searchable_text_json_array_objects() {
        // 测试 JSON 数组包含对象
        let json_val = Fv::Json(json!([
            {"name": "Alice", "role": "admin"},
            {"name": "Bob", "role": "user"}
        ]));

        let result = virtual_searchable_text(&[Some(&json_val)]);
        assert!(result.is_some());
        let text = result.unwrap();

        assert!(text.contains("Alice"));
        assert!(text.contains("admin"));
        assert!(text.contains("Bob"));
        assert!(text.contains("user"));
    }

    #[test]
    fn test_virtual_searchable_text_mixed_types() {
        // 测试混合不同类型的字段
        let text_val = Fv::Text("Direct text".to_string());
        let array_val = Fv::Array(vec![Fv::Text("array text".to_string())]);
        let json_val = Fv::Json(json!({"message": "json text"}));
        let number_val = Fv::I64(123); // 应该被忽略

        let result = virtual_searchable_text(&[
            Some(&text_val),
            Some(&array_val),
            Some(&json_val),
            Some(&number_val),
        ]);

        assert!(result.is_some());
        let text = result.unwrap();

        assert!(text.contains("Direct text"));
        assert!(text.contains("array text"));
        assert!(text.contains("json text"));
    }

    #[test]
    fn test_virtual_searchable_text_nested_structures() {
        // 测试嵌套结构
        let nested_array = Fv::Array(vec![
            Fv::Array(vec![
                Fv::Text("nested1".to_string()),
                Fv::Text("nested2".to_string()),
            ]),
            Fv::Text("top level".to_string()),
        ]);

        let result = virtual_searchable_text(&[Some(&nested_array)]);
        assert!(result.is_some());
        let text = result.unwrap();

        assert!(text.contains("nested1"));
        assert!(text.contains("nested2"));
        assert!(text.contains("top level"));
    }

    #[test]
    fn test_virtual_searchable_text_with_none_values() {
        // 测试包含 None 值的混合输入
        let text_val = Fv::Text("Valid text".to_string());

        let result = virtual_searchable_text(&[None, Some(&text_val), None]);

        assert_eq!(result, Some(Cow::Borrowed("Valid text")));
    }

    #[test]
    fn test_extract_json_text_edge_cases() {
        // 测试 extract_json_text 的边界情况
        let mut texts = Vec::new();

        // 测试空对象
        let empty_obj = json!({});
        extract_json_text(&mut texts, &empty_obj);
        assert!(texts.is_empty());

        // 测试空数组
        let empty_arr = json!([]);
        extract_json_text(&mut texts, &empty_arr);
        assert!(texts.is_empty());

        // 测试 null 值
        let null_val = json!(null);
        extract_json_text(&mut texts, &null_val);
        assert!(texts.is_empty());

        // 测试数字
        let number_val = json!(42);
        extract_json_text(&mut texts, &number_val);
        assert!(texts.is_empty());

        // 测试布尔值
        let bool_val = json!(true);
        extract_json_text(&mut texts, &bool_val);
        assert!(texts.is_empty());
    }

    #[tokio::test]
    async fn test_default_hnsw_hook_converts_u64_bit_arrays() {
        assert!(virtual_field_value(&[]).is_none());

        let storage = Storage::connect(
            "hook_db".to_string(),
            Arc::new(InMemory::new()),
            StorageConfig::default(),
        )
        .await
        .unwrap();
        let hnsw_field = Fe::new("embedding".to_string(), Ft::Vector).unwrap();
        let hnsw = Hnsw::new(
            &hnsw_field,
            HnswConfig {
                dimension: 2,
                ..Default::default()
            },
            storage,
            1,
        )
        .await
        .unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("embedding".to_string(), Ft::Array(vec![Ft::U64])).unwrap())
            .unwrap();
        let mut doc = Document::new(Arc::new(schema.build().unwrap()));
        doc.set_field(
            "embedding",
            Fv::Array(vec![
                Fv::U64(u64::from(bf16::from_f32(0.25).to_bits())),
                Fv::U64(u64::from(bf16::from_f32(0.5).to_bits())),
            ]),
        )
        .unwrap();

        let vector = DefaultIndexHooks
            .hnsw_index_value(&hnsw, &doc)
            .expect("u64 bf16 bits should be converted");
        assert_eq!(vector.len(), 2);
        assert_eq!(vector[0], bf16::from_f32(0.25));

        let mut text_schema = Schema::builder();
        text_schema
            .add_field(Fe::new("embedding".to_string(), Ft::Array(vec![Ft::Text])).unwrap())
            .unwrap();
        let mut text_doc = Document::new(Arc::new(text_schema.build().unwrap()));
        text_doc
            .set_field(
                "embedding",
                Fv::Array(vec![Fv::Text("not-bits".to_string())]),
            )
            .unwrap();
        assert!(
            DefaultIndexHooks
                .hnsw_index_value(&hnsw, &text_doc)
                .is_none()
        );

        let mut scalar_schema = Schema::builder();
        scalar_schema
            .add_field(Fe::new("embedding".to_string(), Ft::Text).unwrap())
            .unwrap();
        let mut scalar_doc = Document::new(Arc::new(scalar_schema.build().unwrap()));
        scalar_doc
            .set_field("embedding", Fv::Text("not a vector".to_string()))
            .unwrap();
        assert!(
            DefaultIndexHooks
                .hnsw_index_value(&hnsw, &scalar_doc)
                .is_none()
        );
    }
}
