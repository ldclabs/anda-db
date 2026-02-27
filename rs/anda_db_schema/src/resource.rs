use serde::{Deserialize, Serialize};

use crate::{
    AndaDBSchema, ByteArrayB64, ByteBufB64, FieldEntry, FieldKey, FieldType, FieldTyped, Json, Map,
    Schema, SchemaError,
};

/// Represents a resource for AI Agents.
/// It can be a file, a URL, or any other type of resource.
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

    /// MIME type, https://developer.mozilla.org/zh-CN/docs/Web/HTTP/MIME_types/Common_types
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
mod tests {}
