//! Internal helpers shared by the `FieldTyped` and `AndaDBSchema` derive macros.
//!
//! The functions in this module work on `syn` AST nodes and emit
//! `proc_macro2::TokenStream` fragments that reference items from
//! `anda_db_schema` (`FieldType`, `FieldKey`, ...). Generated code therefore
//! requires those names to be in scope at the call site.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Expr, GenericArgument, Lit, Meta, PathArguments, Type, ext::IdentExt};

/// Extract the value of a `#[serde(rename = "...")]` attribute, if any.
///
/// Only the first `rename` encountered is returned; other serde options are
/// ignored. Attributes that fail to parse are skipped silently so that
/// unrelated serde syntax does not break schema generation.
pub fn find_rename_attr(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let args = match attr
            .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        {
            Ok(args) => args,
            Err(_) => continue,
        };

        // Walk all serde meta items and pick out `rename = "..."`.
        for meta in args {
            if let Meta::NameValue(name_value) = meta
                && name_value.path.is_ident("rename")
                && let Expr::Lit(expr_lit) = &name_value.value
                && let Lit::Str(s) = &expr_lit.lit
            {
                return Some(s.value());
            }
        }
    }
    None
}

/// Locate a `#[field_type = "..."]` attribute and parse its string payload
/// into a `FieldType` token stream.
///
/// Returns `None` when no such attribute is present, in which case callers
/// should fall back to [`determine_field_type`].
pub fn find_field_type_attr(attrs: &[Attribute]) -> Option<TokenStream> {
    for attr in attrs {
        if attr.path().is_ident("field_type") {
            if let Ok(meta_name_value) = attr.meta.require_name_value()
                && let Expr::Lit(expr_lit) = &meta_name_value.value
                && let Lit::Str(lit_str) = &expr_lit.lit
            {
                return Some(parse_field_type_str(&lit_str.value()));
            }
        }
    }
    None
}

/// Parse the textual DSL used inside `#[field_type = "..."]` and emit the
/// corresponding `FieldType` constructor as a token stream.
///
/// Grammar (whitespace is ignored everywhere):
///
/// ```text
/// type        := primitive | array | option | map
/// primitive   := "Bytes" | "Text" | "U64" | "I64"
///              | "F64"   | "F32"  | "Bool" | "Json" | "Vector"
/// array       := "Array<" type ">"
/// option      := "Option<" type ">"
/// map         := "Map<" map_key "," type ">"
/// map_key     := "String" | "Text" | "Bytes"
/// ```
///
/// `String` and `Text` are accepted as synonymous map keys: `FieldType` only
/// has a `Text` variant, but `Map<String, T>` reads more naturally and is
/// kept for backwards compatibility.
///
/// Unrecognised input expands to a `compile_error!(...)` invocation so that
/// the user gets a precise diagnostic at the original macro call site.
pub fn parse_field_type_str(type_str: &str) -> TokenStream {
    let trimmed = type_str.trim();
    match trimmed {
        // Primitive types.
        "Bytes" => quote! { FieldType::Bytes },
        "Text" => quote! { FieldType::Text },
        "U64" => quote! { FieldType::U64 },
        "I64" => quote! { FieldType::I64 },
        "F64" => quote! { FieldType::F64 },
        "F32" => quote! { FieldType::F32 },
        "Bool" => quote! { FieldType::Bool },
        "Json" => quote! { FieldType::Json },
        "Vector" => quote! { FieldType::Vector },

        // Compound wrappers: Array<T>, Option<T>.
        s if s.starts_with("Array<") && s.ends_with(">") => {
            let inner = s[6..s.len() - 1].trim();
            let inner_type = parse_field_type_str(inner);
            quote! { FieldType::Array(vec![#inner_type]) }
        }
        s if s.starts_with("Option<") && s.ends_with(">") => {
            let inner = s[7..s.len() - 1].trim();
            let inner_type = parse_field_type_str(inner);
            quote! { FieldType::Option(Box::new(#inner_type)) }
        }

        // Map<String, T> / Map<Text, T> / Map<Bytes, T>.
        //
        // `FieldType` represents string keys as `Text`, but `String` is
        // accepted as well so that the DSL can mirror plain Rust signatures.
        s if s.starts_with("Map<") && s.ends_with(">") => {
            let inner = s[4..s.len() - 1].trim();
            // Find the first top-level comma, skipping commas nested inside
            // angle brackets so that types like `Map<Text, Array<U64>>` parse
            // correctly.
            let mut depth: i32 = 0;
            let mut split_at: Option<usize> = None;
            for (i, ch) in inner.char_indices() {
                match ch {
                    '<' => depth += 1,
                    '>' => depth -= 1,
                    ',' if depth == 0 => {
                        split_at = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            let Some(idx) = split_at else {
                let error_msg = format!(
                    "Invalid Map field type: '{}'. Expected 'Map<KeyType, ValueType>'.",
                    type_str
                );
                return quote! { compile_error!(#error_msg) };
            };
            let key = inner[..idx].trim();
            let value = inner[idx + 1..].trim();
            let value_type = parse_field_type_str(value);
            match key {
                "String" | "Text" => quote! {
                    FieldType::Map(std::collections::BTreeMap::from([(
                        FieldKey::from("*"),
                        #value_type
                    )]))
                },
                "Bytes" => quote! {
                    FieldType::Map(std::collections::BTreeMap::from([(
                        FieldKey::from(b"*"),
                        #value_type
                    )]))
                },
                other => {
                    let error_msg = format!(
                        "Unsupported Map key type: '{}'. Expected 'String', 'Text' or 'Bytes'.",
                        other
                    );
                    quote! { compile_error!(#error_msg) }
                }
            }
        }

        // Anything else is rejected at compile time.
        _ => {
            let error_msg = format!(
                "Unsupported field type: '{}'. Supported types: Bytes, Text, U64, I64, F64, F32, Bool, Json, Vector, Array<T>, Option<T>, Map<String, T>, Map<Text, T>, Map<Bytes, T>",
                type_str
            );
            quote! { compile_error!(#error_msg) }
        }
    }
}

/// Infer the AndaDB [`FieldType`] for a Rust type.
///
/// This drives automatic schema generation when no explicit
/// `#[field_type = "..."]` override is provided. The mapping mirrors
/// `parse_field_type_str` for primitives and adds idiomatic Rust patterns:
///
/// - `Vec<u8>` / `[u8; N]` / `Bytes` / `ByteArray` / `ByteBuf` -> `Bytes`
/// - `Vec<bf16>` / `[bf16; N]` -> `Vector`
/// - `Vec<T>` / `HashSet<T>` / `BTreeSet<T>` -> `Array(T)`
/// - `HashMap<K, V>` / `BTreeMap<K, V>` -> `Map({*: V})` (key must be a
///   string- or bytes-like type)
/// - `Option<T>` -> `Option(T)`
/// - `serde_json::Value`, `serde_bytes::*` recognised by full path
/// - Any other path type is treated as a user-defined struct and resolved by
///   calling its `field_type()` associated function (i.e. it must derive
///   [`crate::FieldTyped`])
///
/// On failure, a human-readable message is returned so the caller can emit a
/// `compile_error!` at the original span.
pub fn determine_field_type(ty: &Type) -> Result<TokenStream, String> {
    match ty {
        Type::Path(type_path) if !type_path.path.segments.is_empty() => {
            let path = &type_path.path;
            let segment = &path.segments[0];
            let type_name = segment.ident.unraw().to_string();

            match type_name.as_str() {
                "Option" => {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments
                        && let Some(GenericArgument::Type(inner_type)) = args.args.first()
                    {
                        let inner_field_type = determine_field_type(inner_type)?;
                        return Ok(quote! { FieldType::Option(Box::new(#inner_field_type)) });
                    }
                    Ok(quote! { FieldType::Option(Box::new(FieldType::Json)) })
                }
                "String" | "str" => Ok(quote! { FieldType::Text }),
                "Vec" | "HashSet" | "BTreeSet" => {
                    if let PathArguments::AngleBracketed(args) = &segment.arguments
                        && let Some(GenericArgument::Type(inner_type)) = args.args.first()
                    {
                        if is_u8_type(inner_type) {
                            return Ok(quote! { FieldType::Bytes });
                        } else if is_bf16_type(inner_type) {
                            return Ok(quote! { FieldType::Vector });
                        } else {
                            let inner_field_type = determine_field_type(inner_type)?;
                            return Ok(quote! { FieldType::Array(vec![#inner_field_type]) });
                        }
                    }
                    Err(format!(
                        "Unable to determine Vec element type for: {}",
                        type_name
                    ))
                }
                "bool" => Ok(quote! { FieldType::Bool }),
                "i8" | "i16" | "i32" | "i64" | "isize" => Ok(quote! { FieldType::I64 }),
                "u8" | "u16" | "u32" | "u64" | "usize" => Ok(quote! { FieldType::U64 }),
                "f32" => Ok(quote! { FieldType::F32 }),
                "f64" => Ok(quote! { FieldType::F64 }),
                "Bytes" | "ByteArray" | "ByteBuf" | "BytesB64" | "ByteArrayB64" | "ByteBufB64" => {
                    Ok(quote! { FieldType::Bytes })
                }
                "Json" => Ok(quote! { FieldType::Json }),
                "Vector" => Ok(quote! { FieldType::Vector }),
                "HashMap" | "BTreeMap" | "Map" => {
                    // Handle HashMap / BTreeMap / serde_json::Map.
                    if let PathArguments::AngleBracketed(args) = &segment.arguments
                        && args.args.len() >= 2
                    {
                        let key_type = &args.args[0];
                        let value_type = &args.args[1];
                        let key_ty = match key_type {
                            GenericArgument::Type(ty) => ty,
                            _ => {
                                return Err(format!(
                                    "Map key type must be a type, found: {:?}",
                                    key_type
                                ));
                            }
                        };
                        if is_string_type(key_ty) {
                            if let GenericArgument::Type(value_type) = value_type {
                                let value_field_type = determine_field_type(value_type)?;
                                return Ok(quote! {
                                    FieldType::Map(std::collections::BTreeMap::from([(
                                        FieldKey::from("*"),
                                        #value_field_type
                                    )]))
                                });
                            }
                        } else if is_bytes_type(key_ty) {
                            if let GenericArgument::Type(value_type) = value_type {
                                let value_field_type = determine_field_type(value_type)?;
                                return Ok(quote! {
                                    FieldType::Map(std::collections::BTreeMap::from([(
                                        FieldKey::from(b"*"),
                                        #value_field_type
                                    )]))
                                });
                            }
                        } else {
                            return Err(format!(
                                "Map key type must be String or bytes (e.g., Vec<u8>, [u8; N]), found: {:?}",
                                key_ty
                            ));
                        }
                    }
                    Err(format!("Invalid map type: {}", type_name))
                }
                _ => {
                    if path.segments.len() > 1 {
                        // Multi-segment paths: match a few well-known fully
                        // qualified types from external crates.
                        let full_path = path
                            .segments
                            .iter()
                            .map(|seg| seg.ident.unraw().to_string())
                            .collect::<Vec<_>>()
                            .join("::");

                        match full_path.as_str() {
                            "serde_bytes::ByteArray"
                            | "serde_bytes::ByteBuf"
                            | "serde_bytes::Bytes" => {
                                return Ok(quote! { FieldType::Bytes });
                            }
                            "serde_json::Value" => return Ok(quote! { FieldType::Json }),
                            "half::bf16" => {
                                return Err(
                                    "Standalone `half::bf16` is not supported as a field type. \
                                     Use `Vec<bf16>` (mapped to FieldType::Vector), \
                                     or annotate with `#[field_type = \"F32\"]`."
                                        .to_string(),
                                );
                            }
                            _ => {}
                        }
                    }

                    // Fallback: assume a user-defined struct that derives
                    // `FieldTyped`, and call its `field_type()` accessor.
                    let type_ident =
                        proc_macro2::Ident::new(&type_name, proc_macro2::Span::call_site());
                    Ok(quote! {
                        #type_ident::field_type()
                    })
                }
            }
        }
        Type::Array(array) if is_u8_type(&array.elem) => Ok(quote! { FieldType::Bytes }),
        Type::Array(array) if is_bf16_type(&array.elem) => Ok(quote! { FieldType::Vector }),
        Type::Array(array) => {
            let inner_type = determine_field_type(&array.elem)?;
            Ok(quote! { FieldType::Array(vec![#inner_type]) })
        }
        _ => {
            // Reference, tuple, trait object, etc. -- not representable.
            let error_msg = format!(
                "Unsupported type: '{:?}'. Consider:\n1. Using a supported primitive type\n2. Adding #[field_type = \"SupportedType\"] attribute\n3. Implementing FieldTyped for this type",
                ty
            );
            Err(error_msg)
        }
    }
}

/// Returns `true` if `ty` is the primitive `u8`.
pub fn is_u8_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        return segment.ident == "u8";
    }
    false
}

/// Returns `true` if `ty` is `String` or `str`.
pub fn is_string_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        return segment.ident == "String" || segment.ident == "str";
    }
    false
}

/// Returns `true` if `ty` is one of the supported byte container types.
///
/// Recognised types: `Vec<u8>`, `Bytes`, `ByteBuf`, `ByteArray`,
/// `BytesB64`, `ByteBufB64`, `ByteArrayB64`.
///
/// Note: bare `[u8; N]` arrays are intentionally **not** treated as bytes
/// here for `Map` keys -- prefer `ByteArray` / `ByteArrayB64` instead.
pub fn is_bytes_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        // Vec<u8>
        if segment.ident == "Vec"
            && let PathArguments::AngleBracketed(args) = &segment.arguments
            && let Some(GenericArgument::Type(inner_ty)) = args.args.first()
        {
            return is_u8_type(inner_ty);
        }

        // Other byte/buffer wrappers.
        return segment.ident == "Bytes"
            || segment.ident == "ByteBuf"
            || segment.ident == "ByteArray"
            || segment.ident == "ByteBufB64"
            || segment.ident == "BytesB64"
            || segment.ident == "ByteArrayB64";
    }
    false
}

/// Returns `true` if `ty` is `bf16` (the `half::bf16` short name).
pub fn is_bf16_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        return segment.ident == "bf16";
    }
    false
}

/// Returns `true` if `ty` is the primitive `u64`. Used to validate the
/// mandatory `_id: u64` field on structs deriving [`crate::AndaDBSchema`].
pub fn is_u64_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.first()
    {
        return segment.ident == "u64";
    }
    false
}
