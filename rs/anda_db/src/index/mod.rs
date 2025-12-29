use anda_db_schema::{Document, Fv, Json, Vector};
use ic_auth_types::deterministic_cbor_into_vec;
use std::borrow::Cow;

mod bm25;
mod btree;
mod hnsw;

pub use bm25::*;
pub use btree::*;
pub use hnsw::*;

pub trait IndexHooks: Send + Sync {
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

    fn bm25_index_value<'a>(&self, index: &BM25, doc: &'a Document) -> Option<Cow<'a, str>> {
        let fields = index.virtual_field();
        let mut vals: Vec<Option<&Fv>> = Vec::with_capacity(fields.len());
        for name in fields {
            vals.push(doc.get_field(name));
        }

        virtual_searchable_text(&vals)
    }

    fn hnsw_index_value<'a>(&self, index: &Hnsw, doc: &'a Document) -> Option<Cow<'a, Vector>> {
        if let Some(Fv::Vector(vector)) = doc.get_field(index.field_name()) {
            return Some(Cow::Borrowed(vector));
        }
        None
    }
}

pub fn virtual_field_name(fields: &[&str]) -> String {
    fields.join("-")
}

pub fn from_virtual_field_name(name: &str) -> Vec<String> {
    name.split('-').map(String::from).collect()
}

pub fn virtual_field_value(vals: &[Option<&Fv>]) -> Option<Fv> {
    if vals.is_empty() {
        return None;
    }
    let mut data = Vec::new();
    for val in vals {
        data.extend(deterministic_cbor_into_vec(val).ok()?);
    }
    Some(Fv::Bytes(data))
}

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

pub struct DefaultIndexHooks;

impl IndexHooks for DefaultIndexHooks {}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db_schema::Fv;
    use serde_json::json;
    use std::collections::BTreeMap;

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
}
