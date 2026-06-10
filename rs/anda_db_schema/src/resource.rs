//! The [`Resource`] type — a reusable schema describing a binary asset or
//! external reference attached to a document.
use serde::{Deserialize, Serialize};

use crate::{
    AndaDBSchema, ByteArrayB64, ByteBufB64, FieldEntry, FieldKey, FieldType, FieldTyped, Json, Map,
    Schema, SchemaError,
};

/// Represents a resource for AI Agents.
///
/// A `Resource` is a generic descriptor for any external piece of data an
/// agent may need to reference: an inline blob, a remote file, a URL, etc.
/// Every field except `_id`, `tags` and `name` is optional, so the same
/// type can describe both lightweight references (`uri` only) and fully
/// inlined assets (`blob` plus `mime_type` and `hash`).
///
/// The struct derives [`AndaDBSchema`] and [`FieldTyped`], so it can be
/// embedded as a sub-document in any other Anda DB schema by simply using
/// `Resource` (or `Option<Resource>`) as a field type.
#[derive(Debug, Default, Clone, Serialize, Deserialize, FieldTyped, PartialEq, AndaDBSchema)]
pub struct Resource {
    /// The unique identifier for this resource in the Anda DB collection.
    pub _id: u64,

    /// A list of tags that identifies the type of this resource.
    /// It is recommended to use the primary type of the file MIME type and the file extension as tags, for example:
    /// "text", "image", "audio", "video", "txt", "md", "png", etc.
    pub tags: Vec<String>,

    /// A human-readable name for this resource.
    pub name: String,

    /// A description of what this resource represents.
    /// This can be used by clients to improve the LLM's understanding of available resources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The URI of this resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,

    /// MIME type, <https://developer.mozilla.org/zh-CN/docs/Web/HTTP/MIME_types/Common_types>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,

    /// The binary data of this resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<ByteBufB64>,

    /// The size of the resource in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,

    /// The SHA3-256 hash of the resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[unique]
    pub hash: Option<ByteArrayB64<32>>,

    /// Metadata associated with this resource.
    /// This can include additional information such as creation date, author, etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Json>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Document, FieldValue};
    use std::sync::Arc;

    #[test]
    fn resource_schema_marks_hash_unique() {
        let schema = Resource::schema().unwrap();

        let id = schema.get_field("_id").unwrap();
        assert_eq!(id.r#type(), &FieldType::U64);
        assert!(id.unique());

        let hash = schema.get_field("hash").unwrap();
        assert!(hash.unique());
        assert_eq!(
            hash.r#type(),
            &FieldType::Option(Box::new(FieldType::Bytes))
        );
        assert!(!hash.required());

        let name = schema.get_field("name").unwrap();
        assert_eq!(name.r#type(), &FieldType::Text);
        assert!(name.required());
        assert!(!name.unique());
    }

    #[test]
    fn resource_with_blob_and_hash_roundtrips_through_document() {
        let schema = Arc::new(Resource::schema().unwrap());
        let resource = Resource {
            _id: 7,
            tags: vec!["image".to_string(), "png".to_string()],
            name: "avatar.png".to_string(),
            description: Some("profile picture".to_string()),
            uri: None,
            mime_type: Some("image/png".to_string()),
            blob: Some(vec![0x89, 0x50, 0x4E, 0x47].into()),
            size: Some(4),
            hash: Some([7u8; 32].into()),
            metadata: None,
        };

        let doc = Document::try_from(schema.clone(), &resource).unwrap();
        // Binary fields must be stored as CBOR bytes, not Base64 text.
        assert_eq!(
            doc.get_field("blob").unwrap(),
            &FieldValue::Bytes(vec![0x89, 0x50, 0x4E, 0x47])
        );
        assert_eq!(
            doc.get_field("hash").unwrap(),
            &FieldValue::Bytes(vec![7u8; 32])
        );

        let decoded: Resource = doc.try_into().unwrap();
        assert_eq!(decoded, resource);
    }
}
