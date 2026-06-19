//! Internal helpers shared by the `FieldTyped` and `AndaDBSchema` derive macros.
//!
//! The functions in this module work on `syn` AST nodes and emit
//! `proc_macro2::TokenStream` fragments that reference items from
//! `anda_db_schema` (`FieldType`, `FieldKey`, ...). Generated code therefore
//! requires those names to be in scope at the call site.
//!
//! All fallible helpers return [`syn::Result`] with errors spanned at the
//! offending field, type or attribute so that diagnostics point at the user's
//! code instead of the `#[derive(...)]` invocation.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::{
    Attribute, Data, DeriveInput, Expr, Field, Fields, GenericArgument, Lit, LitStr, Meta, Path,
    PathArguments, PathSegment, Type, ext::IdentExt, punctuated::Punctuated, token::Comma,
};

/// Extract the named fields of a struct, or report a spanned error for any
/// other input shape (tuple/unit structs, enums, unions).
pub fn named_fields<'a>(
    input: &'a DeriveInput,
    macro_name: &str,
) -> syn::Result<&'a Punctuated<Field, Comma>> {
    match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => Ok(&fields_named.named),
            _ => Err(syn::Error::new_spanned(
                &input.ident,
                format!("{macro_name} only supports structs with named fields"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            &input.ident,
            format!("{macro_name} only supports structs"),
        )),
    }
}

/// The case-conversion rules accepted by `#[serde(rename_all = "...")]`.
///
/// The conversion algorithms mirror `serde_derive`'s `RenameRule::apply_to_field`
/// exactly, so the generated schema always matches the names serde writes.
/// As in serde, field identifiers are assumed to be snake_case.
// Variant names deliberately mirror serde_derive's `RenameRule` one-to-one.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenameRule {
    /// `lowercase`
    LowerCase,
    /// `UPPERCASE`
    UpperCase,
    /// `PascalCase`
    PascalCase,
    /// `camelCase`
    CamelCase,
    /// `snake_case`
    SnakeCase,
    /// `SCREAMING_SNAKE_CASE`
    ScreamingSnakeCase,
    /// `kebab-case`
    KebabCase,
    /// `SCREAMING-KEBAB-CASE`
    ScreamingKebabCase,
}

impl RenameRule {
    fn from_lit(lit: &LitStr) -> syn::Result<Self> {
        match lit.value().as_str() {
            "lowercase" => Ok(Self::LowerCase),
            "UPPERCASE" => Ok(Self::UpperCase),
            "PascalCase" => Ok(Self::PascalCase),
            "camelCase" => Ok(Self::CamelCase),
            "snake_case" => Ok(Self::SnakeCase),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnakeCase),
            "kebab-case" => Ok(Self::KebabCase),
            "SCREAMING-KEBAB-CASE" => Ok(Self::ScreamingKebabCase),
            other => Err(syn::Error::new(
                lit.span(),
                format!(
                    "unknown #[serde(rename_all = {other:?})] rule; expected one of \
                     \"lowercase\", \"UPPERCASE\", \"PascalCase\", \"camelCase\", \
                     \"snake_case\", \"SCREAMING_SNAKE_CASE\", \"kebab-case\", \
                     \"SCREAMING-KEBAB-CASE\""
                ),
            )),
        }
    }

    /// Apply this rule to a (snake_case) field identifier, mirroring serde.
    pub fn apply_to_field(self, field: &str) -> String {
        match self {
            Self::LowerCase | Self::SnakeCase => field.to_string(),
            Self::UpperCase | Self::ScreamingSnakeCase => field.to_ascii_uppercase(),
            Self::PascalCase => {
                let mut pascal = String::with_capacity(field.len());
                let mut capitalize = true;
                for ch in field.chars() {
                    if ch == '_' {
                        capitalize = true;
                    } else if capitalize {
                        pascal.push(ch.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        pascal.push(ch);
                    }
                }
                pascal
            }
            Self::CamelCase => {
                let pascal = Self::PascalCase.apply_to_field(field);
                let mut chars = pascal.chars();
                match chars.next() {
                    Some(first) => first.to_ascii_lowercase().to_string() + chars.as_str(),
                    None => pascal,
                }
            }
            Self::KebabCase => field.replace('_', "-"),
            Self::ScreamingKebabCase => Self::ScreamingSnakeCase
                .apply_to_field(field)
                .replace('_', "-"),
        }
    }
}

/// Container-level serde options that affect schema generation.
#[derive(Debug, Default)]
pub struct ContainerSerdeAttrs {
    /// The rule declared by `#[serde(rename_all = "...")]`, if any. When the
    /// directional form is used, only the `serialize` rule is honoured
    /// because AndaDB stores the serialized representation.
    pub rename_all: Option<RenameRule>,
    /// `#[serde(transparent)]` -- the struct serializes as its single field,
    /// not as a map, so a map-shaped schema can never match it.
    pub transparent: bool,
}

/// Parse the container-level serde attributes relevant to schema generation.
///
/// Unknown serde options are ignored; attributes that fail to parse are
/// skipped silently so that unrelated serde syntax does not break schema
/// generation. An unknown `rename_all` rule is an error, because silently
/// ignoring it would produce a schema that cannot match the serialized data.
pub fn parse_container_serde_attrs(attrs: &[Attribute]) -> syn::Result<ContainerSerdeAttrs> {
    let mut out = ContainerSerdeAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let Ok(args) = attr.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        else {
            continue;
        };

        for meta in args {
            match &meta {
                Meta::Path(path) if path.is_ident("transparent") => out.transparent = true,
                Meta::NameValue(name_value) if name_value.path.is_ident("rename_all") => {
                    if let Expr::Lit(expr_lit) = &name_value.value
                        && let Lit::Str(lit) = &expr_lit.lit
                        && out.rename_all.is_none()
                    {
                        out.rename_all = Some(RenameRule::from_lit(lit)?);
                    }
                }
                // Directional form: rename_all(serialize = "...", deserialize = "...").
                Meta::List(list) if list.path.is_ident("rename_all") => {
                    if let Ok(rename_args) =
                        list.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
                    {
                        for rename_meta in rename_args {
                            if let Meta::NameValue(name_value) = rename_meta
                                && name_value.path.is_ident("serialize")
                                && let Expr::Lit(expr_lit) = &name_value.value
                                && let Lit::Str(lit) = &expr_lit.lit
                                && out.rename_all.is_none()
                            {
                                out.rename_all = Some(RenameRule::from_lit(lit)?);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(out)
}

/// Field-level serde options that affect schema generation.
#[derive(Debug, Default)]
pub struct FieldSerdeAttrs {
    /// `#[serde(rename = "...")]`, or the `serialize` half of the directional
    /// form. The schema follows the serialized field name because that is
    /// what AndaDB stores.
    pub rename: Option<String>,
    /// `#[serde(skip)]` / `#[serde(skip_serializing)]` -- the field never
    /// appears in serialized output and must not appear in the schema.
    pub skip_serializing: bool,
    /// `#[serde(flatten)]` -- the field's keys are inlined into the parent
    /// map, which a per-field schema entry cannot describe.
    pub flatten: bool,
}

/// Parse the field-level serde attributes relevant to schema generation.
///
/// Only the first `rename` encountered is returned; other serde options are
/// ignored. Attributes that fail to parse are skipped silently so that
/// unrelated serde syntax does not break schema generation.
pub fn parse_field_serde_attrs(attrs: &[Attribute]) -> FieldSerdeAttrs {
    let mut out = FieldSerdeAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let Ok(args) = attr.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        else {
            continue;
        };

        for meta in args {
            match &meta {
                Meta::Path(path) if path.is_ident("skip") || path.is_ident("skip_serializing") => {
                    out.skip_serializing = true;
                }
                Meta::Path(path) if path.is_ident("flatten") => out.flatten = true,
                Meta::NameValue(name_value) if name_value.path.is_ident("rename") => {
                    if let Expr::Lit(expr_lit) = &name_value.value
                        && let Lit::Str(lit) = &expr_lit.lit
                        && out.rename.is_none()
                    {
                        out.rename = Some(lit.value());
                    }
                }
                // Directional form: rename(serialize = "...", deserialize = "...").
                Meta::List(list) if list.path.is_ident("rename") => {
                    if let Ok(rename_args) =
                        list.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
                    {
                        for rename_meta in rename_args {
                            if let Meta::NameValue(name_value) = rename_meta
                                && name_value.path.is_ident("serialize")
                                && let Expr::Lit(expr_lit) = &name_value.value
                                && let Lit::Str(lit) = &expr_lit.lit
                                && out.rename.is_none()
                            {
                                out.rename = Some(lit.value());
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    out
}

/// Field-level `cbor2::Cbor` options that affect nested `FieldTyped` maps.
#[derive(Debug, Default)]
pub struct FieldCborAttrs {
    /// `#[cbor(key = N)]` -- the field serializes with an integer CBOR map
    /// key instead of its serde text name.
    pub key: Option<i64>,
}

/// Parse the field-level `#[cbor(...)]` attributes relevant to nested
/// `FieldTyped` map generation.
///
/// Only `key = <integer>` is consumed. Other `cbor2::Cbor` options are left to
/// cbor2 itself.
pub fn parse_field_cbor_attrs(attrs: &[Attribute]) -> syn::Result<FieldCborAttrs> {
    let mut out = FieldCborAttrs::default();
    for attr in attrs {
        if !attr.path().is_ident("cbor") {
            continue;
        }
        let Ok(args) = attr.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        else {
            continue;
        };

        for meta in args {
            if let Meta::NameValue(name_value) = meta
                && name_value.path.is_ident("key")
            {
                if out.key.is_some() {
                    return Err(syn::Error::new_spanned(
                        name_value,
                        "duplicate #[cbor(key = ...)] attribute",
                    ));
                }
                out.key = Some(parse_cbor_i64_key(&name_value.value)?);
            }
        }
    }
    Ok(out)
}

fn parse_cbor_i64_key(expr: &Expr) -> syn::Result<i64> {
    match expr {
        Expr::Lit(expr_lit) => match &expr_lit.lit {
            Lit::Int(lit) => {
                let value = lit.base10_parse::<i128>()?;
                i64::try_from(value).map_err(|_| {
                    syn::Error::new_spanned(
                        expr,
                        "#[cbor(key = ...)] must fit in an i64 for AndaDB FieldKey",
                    )
                })
            }
            _ => Err(syn::Error::new_spanned(
                expr,
                "#[cbor(key = ...)] must be an integer literal",
            )),
        },
        Expr::Unary(expr_unary) if matches!(expr_unary.op, syn::UnOp::Neg(_)) => {
            if let Expr::Lit(expr_lit) = expr_unary.expr.as_ref()
                && let Lit::Int(lit) = &expr_lit.lit
            {
                let magnitude = lit.base10_parse::<i128>()?;
                let value = magnitude.checked_neg().ok_or_else(|| {
                    syn::Error::new_spanned(
                        expr,
                        "#[cbor(key = ...)] must fit in an i64 for AndaDB FieldKey",
                    )
                })?;
                return i64::try_from(value).map_err(|_| {
                    syn::Error::new_spanned(
                        expr,
                        "#[cbor(key = ...)] must fit in an i64 for AndaDB FieldKey",
                    )
                });
            }
            Err(syn::Error::new_spanned(
                expr,
                "#[cbor(key = ...)] must be an integer literal",
            ))
        }
        _ => Err(syn::Error::new_spanned(
            expr,
            "#[cbor(key = ...)] must be an integer literal",
        )),
    }
}

/// Resolve the schema field name for a field: an explicit serde `rename`
/// wins; otherwise the container-level `rename_all` rule (if any) is applied
/// to the Rust identifier, mirroring serde's own precedence.
pub fn effective_field_name(
    rust_name: &str,
    serde_attrs: &FieldSerdeAttrs,
    rename_all: Option<RenameRule>,
) -> String {
    if let Some(rename) = &serde_attrs.rename {
        return rename.clone();
    }
    match rename_all {
        Some(rule) => rule.apply_to_field(rust_name),
        None => rust_name.to_string(),
    }
}

/// Validate a top-level schema field name against AndaDB's naming rules
/// (mirrors `anda_db_schema::validate_field_name`): non-empty, at most 64
/// bytes, and only ASCII lowercase letters, digits and underscores.
///
/// Only `Schema` field names are restricted; keys of nested
/// `FieldType::Map`s (as generated by `FieldTyped`) are free-form.
pub fn validate_schema_field_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("empty string".to_string());
    }
    if name.len() > 64 {
        return Err(format!("length {} exceeds the limit 64", name.len()));
    }
    for &b in name.as_bytes() {
        if !matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_') {
            return Err(format!("invalid character {:?}", char::from(b)));
        }
    }
    Ok(())
}

/// Resolve a field's `FieldType` tokens: an explicit `#[field_type = "..."]`
/// override wins; otherwise the type is inferred from the Rust type.
pub fn resolve_field_type(field: &Field) -> syn::Result<TokenStream> {
    match find_field_type_attr(&field.attrs)? {
        Some(field_type) => Ok(field_type),
        None => determine_field_type(&field.ty),
    }
}

/// Locate a `#[field_type = "..."]` attribute and parse its string payload
/// into a `FieldType` token stream.
///
/// Returns `Ok(None)` when no such attribute is present, in which case
/// callers should fall back to [`determine_field_type`].
pub fn find_field_type_attr(attrs: &[Attribute]) -> syn::Result<Option<TokenStream>> {
    for attr in attrs {
        if attr.path().is_ident("field_type") {
            let meta_name_value = attr.meta.require_name_value().map_err(|_| {
                syn::Error::new_spanned(
                    attr,
                    "`field_type` attribute must use the form #[field_type = \"Type\"]",
                )
            })?;

            if let Expr::Lit(expr_lit) = &meta_name_value.value
                && let Lit::Str(lit_str) = &expr_lit.lit
            {
                return parse_field_type_str(&lit_str.value(), lit_str.span()).map(Some);
            }

            return Err(syn::Error::new_spanned(
                &meta_name_value.value,
                "`field_type` attribute value must be a string literal, e.g. #[field_type = \"Text\"]",
            ));
        }
    }
    Ok(None)
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
/// Unrecognised input produces an error spanned at `span` (the attribute's
/// string literal) so that the user gets a precise diagnostic.
pub fn parse_field_type_str(type_str: &str, span: Span) -> syn::Result<TokenStream> {
    let normalized: String = type_str.chars().filter(|ch| !ch.is_whitespace()).collect();
    match normalized.as_str() {
        // Primitive types.
        "Bytes" => Ok(quote! { FieldType::Bytes }),
        "Text" => Ok(quote! { FieldType::Text }),
        "U64" => Ok(quote! { FieldType::U64 }),
        "I64" => Ok(quote! { FieldType::I64 }),
        "F64" => Ok(quote! { FieldType::F64 }),
        "F32" => Ok(quote! { FieldType::F32 }),
        "Bool" => Ok(quote! { FieldType::Bool }),
        "Json" => Ok(quote! { FieldType::Json }),
        "Vector" => Ok(quote! { FieldType::Vector }),

        // Compound wrappers: Array<T>, Option<T>.
        s if s.starts_with("Array<") && s.ends_with('>') => {
            let inner_type = parse_field_type_str(&s[6..s.len() - 1], span)?;
            Ok(quote! { FieldType::Array(vec![#inner_type]) })
        }
        s if s.starts_with("Option<") && s.ends_with('>') => {
            let inner_type = parse_field_type_str(&s[7..s.len() - 1], span)?;
            Ok(quote! { FieldType::Option(Box::new(#inner_type)) })
        }

        // Map<String, T> / Map<Text, T> / Map<I64, T> / Map<Bytes, T>.
        //
        // `FieldType` represents string keys as `Text`, but `String` is
        // accepted as well so that the DSL can mirror plain Rust signatures.
        s if s.starts_with("Map<") && s.ends_with('>') => {
            let inner = &s[4..s.len() - 1];
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
                return Err(syn::Error::new(
                    span,
                    format!(
                        "Invalid Map field type: '{type_str}'. Expected 'Map<KeyType, ValueType>'."
                    ),
                ));
            };
            let key_token = match &inner[..idx] {
                "String" | "Text" => quote! { FieldKey::from("*") },
                "I64" | "i8" | "i16" | "i32" | "i64" | "isize" => {
                    quote! { FieldKey::from(i64::MIN) }
                }
                "Bytes" => quote! { FieldKey::from(b"*") },
                other => {
                    return Err(syn::Error::new(
                        span,
                        format!(
                            "Unsupported Map key type: '{other}'. Expected 'String', 'Text', 'I64' or 'Bytes'."
                        ),
                    ));
                }
            };
            let value_type = parse_field_type_str(&inner[idx + 1..], span)?;
            Ok(quote! {
                FieldType::Map(std::collections::BTreeMap::from([(
                    #key_token,
                    #value_type
                )]))
            })
        }

        // Anything else is rejected at compile time.
        _ => Err(syn::Error::new(
            span,
            format!(
                "Unsupported field type: '{type_str}'. Supported types: Bytes, Text, U64, I64, F64, F32, Bool, Json, Vector, Array<T>, Option<T>, Map<String, T>, Map<Text, T>, Map<I64, T>, Map<Bytes, T>"
            ),
        )),
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
///   string-, signed integer-, or bytes-like type)
/// - `Option<T>` -> `Option(T)`
/// - `Box<T>` / `Arc<T>` / `Rc<T>` / `Cow<'_, T>` -> the inner `T` (serde
///   serializes these wrappers transparently)
/// - `serde_json::Value`, `serde_bytes::*` recognised by full path
/// - Any other path type is treated as a user-defined struct and resolved by
///   calling its `field_type()` associated function (i.e. it must derive
///   [`crate::FieldTyped`])
///
/// Parenthesized and macro-generated (invisibly grouped) types are unwrapped
/// transparently. On failure, an error spanned at the offending type is
/// returned so the compiler points at the user's code.
pub fn determine_field_type(ty: &Type) -> syn::Result<TokenStream> {
    let ty = peel_type(ty);
    match ty {
        Type::Path(type_path) if !type_path.path.segments.is_empty() => {
            let path = &type_path.path;
            let segment = path.segments.last().expect("checked non-empty");
            let type_name = segment.ident.unraw().to_string();
            let full_path = path_to_string(path);

            match full_path.as_str() {
                "serde_json::Value" => return Ok(quote! { FieldType::Json }),
                "serde_bytes::ByteArray" | "serde_bytes::ByteBuf" | "serde_bytes::Bytes" => {
                    return Ok(quote! { FieldType::Bytes });
                }
                "half::bf16" => return unsupported_scalar_bf16(ty),
                _ => {}
            }

            match type_name.as_str() {
                "Option" => {
                    if let Some(inner_type) = first_type_argument(segment) {
                        let inner_field_type = determine_field_type(inner_type)?;
                        return Ok(quote! { FieldType::Option(Box::new(#inner_field_type)) });
                    }
                    Err(syn::Error::new_spanned(
                        ty,
                        "Unable to determine Option element type",
                    ))
                }
                "String" | "str" => Ok(quote! { FieldType::Text }),
                "Vec" | "HashSet" | "BTreeSet" => {
                    if let Some(inner_type) = first_type_argument(segment) {
                        if is_u8_type(inner_type) {
                            return Ok(quote! { FieldType::Bytes });
                        } else if is_bf16_type(inner_type) {
                            return Ok(quote! { FieldType::Vector });
                        }
                        let inner_field_type = determine_field_type(inner_type)?;
                        return Ok(quote! { FieldType::Array(vec![#inner_field_type]) });
                    }
                    Err(syn::Error::new_spanned(
                        ty,
                        format!("Unable to determine Vec element type for: {type_name}"),
                    ))
                }
                // serde serializes smart pointers transparently, so the
                // schema type is the inner type's.
                "Box" | "Arc" | "Rc" | "Cow" => {
                    if let Some(inner_type) = first_type_argument(segment) {
                        return determine_field_type(inner_type);
                    }
                    Err(syn::Error::new_spanned(
                        ty,
                        format!("Unable to determine the inner type of: {type_name}"),
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
                "bf16" => unsupported_scalar_bf16(ty),
                "HashMap" | "BTreeMap" | "Map" => {
                    // Handle HashMap / BTreeMap / serde_json::Map. Extra
                    // generic arguments (e.g. a custom hasher) are ignored.
                    if let PathArguments::AngleBracketed(args) = &segment.arguments
                        && args.args.len() >= 2
                        && let (GenericArgument::Type(key_ty), GenericArgument::Type(value_ty)) =
                            (&args.args[0], &args.args[1])
                    {
                        let key_token = if is_string_type(key_ty) {
                            quote! { FieldKey::from("*") }
                        } else if is_signed_integer_type(key_ty) {
                            quote! { FieldKey::from(i64::MIN) }
                        } else if is_bytes_type(key_ty) {
                            quote! { FieldKey::from(b"*") }
                        } else {
                            return Err(syn::Error::new_spanned(
                                key_ty,
                                format!(
                                    "Map key type must be String, signed integer, or bytes (e.g., Vec<u8>, ByteArray, ByteBuf), found: {}",
                                    type_to_string(key_ty)
                                ),
                            ));
                        };
                        let value_field_type = determine_field_type(value_ty)?;
                        return Ok(quote! {
                            FieldType::Map(std::collections::BTreeMap::from([(
                                #key_token,
                                #value_field_type
                            )]))
                        });
                    }
                    Err(syn::Error::new_spanned(
                        ty,
                        format!("Invalid map type: {type_name}"),
                    ))
                }
                _ => {
                    // Fallback: assume a user-defined struct that derives
                    // `FieldTyped`, and call its `field_type()` accessor. The
                    // `<...>` form keeps generic types like `Wrapper<T>`
                    // valid in expression position.
                    Ok(quote! {
                        <#ty>::field_type()
                    })
                }
            }
        }
        Type::Reference(reference) => determine_field_type(&reference.elem),
        Type::Slice(slice) if is_u8_type(&slice.elem) => Ok(quote! { FieldType::Bytes }),
        Type::Slice(slice) if is_bf16_type(&slice.elem) => Ok(quote! { FieldType::Vector }),
        Type::Slice(slice) => {
            let inner_type = determine_field_type(&slice.elem)?;
            Ok(quote! { FieldType::Array(vec![#inner_type]) })
        }
        Type::Array(array) if is_u8_type(&array.elem) => Ok(quote! { FieldType::Bytes }),
        Type::Array(array) if is_bf16_type(&array.elem) => Ok(quote! { FieldType::Vector }),
        Type::Array(array) => {
            let inner_type = determine_field_type(&array.elem)?;
            Ok(quote! { FieldType::Array(vec![#inner_type]) })
        }
        _ => {
            // Tuple, trait object, bare function, etc. -- not representable.
            Err(syn::Error::new_spanned(
                ty,
                format!(
                    "Unsupported type: `{}`. Consider:\n1. Using a supported primitive type\n2. Adding #[field_type = \"SupportedType\"] attribute\n3. Implementing FieldTyped for this type",
                    type_to_string(ty)
                ),
            ))
        }
    }
}

/// Strip parentheses and invisible groups (inserted by `macro_rules!`
/// expansion) so that types produced by macros infer the same way as
/// hand-written ones.
fn peel_type(mut ty: &Type) -> &Type {
    loop {
        match ty {
            Type::Group(group) => ty = &group.elem,
            Type::Paren(paren) => ty = &paren.elem,
            _ => return ty,
        }
    }
}

/// Return the first generic *type* argument of a path segment, skipping
/// lifetimes and const arguments (e.g. the `'a` in `Cow<'a, str>`).
fn first_type_argument(segment: &PathSegment) -> Option<&Type> {
    if let PathArguments::AngleBracketed(args) = &segment.arguments {
        args.args.iter().find_map(|arg| match arg {
            GenericArgument::Type(ty) => Some(ty),
            _ => None,
        })
    } else {
        None
    }
}

fn path_to_string(path: &Path) -> String {
    path.segments
        .iter()
        .map(|seg| seg.ident.unraw().to_string())
        .collect::<Vec<_>>()
        .join("::")
}

/// Render a type roughly as the Rust source the user wrote. Used in error
/// messages instead of the unreadable AST `Debug` output.
fn type_to_string(ty: &Type) -> String {
    ty.to_token_stream().to_string()
}

fn unsupported_scalar_bf16(ty: &Type) -> syn::Result<TokenStream> {
    Err(syn::Error::new_spanned(
        ty,
        "Standalone `bf16` is not supported as a field type. \
         Use `Vec<bf16>` (mapped to FieldType::Vector), \
         or annotate with `#[field_type = \"F32\"]`.",
    ))
}

/// Returns `true` if `ty` is the primitive `u8`.
pub fn is_u8_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = peel_type(ty)
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "u8";
    }
    false
}

/// Returns `true` if `ty` is `String` or `str`.
pub fn is_string_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = peel_type(ty)
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "String" || segment.ident == "str";
    }
    false
}

/// Returns `true` if `ty` is one of the signed integer types represented as
/// `FieldType::I64`.
pub fn is_signed_integer_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = peel_type(ty)
        && let Some(segment) = type_path.path.segments.last()
    {
        return matches!(
            segment.ident.to_string().as_str(),
            "i8" | "i16" | "i32" | "i64" | "isize"
        );
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
    if let Type::Path(type_path) = peel_type(ty)
        && let Some(segment) = type_path.path.segments.last()
    {
        // Vec<u8>
        if segment.ident == "Vec"
            && let Some(inner_ty) = first_type_argument(segment)
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
    if let Type::Path(type_path) = peel_type(ty)
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "bf16";
    }
    false
}

/// Returns `true` if `ty` is the primitive `u64`. Used to validate the
/// `_id: u64` field on structs deriving [`crate::AndaDBSchema`].
pub fn is_u64_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = peel_type(ty)
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident == "u64";
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn tokens(value: TokenStream) -> String {
        value.to_string()
    }

    fn parse_ft(input: &str) -> syn::Result<TokenStream> {
        parse_field_type_str(input, Span::call_site())
    }

    #[test]
    fn named_fields_accepts_named_structs_and_rejects_other_shapes() {
        let input: DeriveInput = parse_quote! {
            struct Named { value: u64 }
        };
        assert_eq!(named_fields(&input, "FieldTyped").unwrap().len(), 1);

        let input: DeriveInput = parse_quote!(
            struct Tuple(u64);
        );
        assert!(
            named_fields(&input, "FieldTyped")
                .err()
                .unwrap()
                .to_string()
                .contains("only supports structs with named fields")
        );

        let input: DeriveInput = parse_quote!(
            enum Choice {
                A,
            }
        );
        assert!(
            named_fields(&input, "AndaDBSchema")
                .err()
                .unwrap()
                .to_string()
                .contains("only supports structs")
        );
    }

    #[test]
    fn parse_field_serde_attrs_handles_rename_skip_flatten_and_absent() {
        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(default)])];
        let parsed = parse_field_serde_attrs(&attrs);
        assert_eq!(parsed.rename, None);
        assert!(!parsed.skip_serializing);
        assert!(!parsed.flatten);

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(rename = "wire_name")])];
        assert_eq!(
            parse_field_serde_attrs(&attrs).rename,
            Some("wire_name".to_string())
        );

        let attrs: Vec<Attribute> =
            vec![parse_quote!(#[serde(rename(serialize = "out", deserialize = "in"))])];
        assert_eq!(
            parse_field_serde_attrs(&attrs).rename,
            Some("out".to_string())
        );

        // A deserialize-only rename does not change the serialized name.
        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(rename(deserialize = "in"))])];
        assert_eq!(parse_field_serde_attrs(&attrs).rename, None);

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(skip)])];
        assert!(parse_field_serde_attrs(&attrs).skip_serializing);

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(skip_serializing)])];
        assert!(parse_field_serde_attrs(&attrs).skip_serializing);

        // skip_serializing_if is conditional: the field can still appear.
        let attrs: Vec<Attribute> =
            vec![parse_quote!(#[serde(skip_serializing_if = "Option::is_none")])];
        assert!(!parse_field_serde_attrs(&attrs).skip_serializing);

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(flatten)])];
        assert!(parse_field_serde_attrs(&attrs).flatten);

        let attrs: Vec<Attribute> = vec![parse_quote!(#[allow(dead_code)])];
        assert_eq!(parse_field_serde_attrs(&attrs).rename, None);
    }

    #[test]
    fn parse_field_cbor_attrs_reads_integer_keys_and_rejects_bad_forms() {
        let attrs: Vec<Attribute> = vec![parse_quote!(#[cbor(key = 4)])];
        assert_eq!(parse_field_cbor_attrs(&attrs).unwrap().key, Some(4));

        let attrs: Vec<Attribute> = vec![parse_quote!(#[cbor(key = -8)])];
        assert_eq!(parse_field_cbor_attrs(&attrs).unwrap().key, Some(-8));

        let attrs: Vec<Attribute> = vec![
            parse_quote!(#[cbor(key = 1)]),
            parse_quote!(#[cbor(key = 2)]),
        ];
        assert!(
            parse_field_cbor_attrs(&attrs)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );

        let attrs: Vec<Attribute> = vec![parse_quote!(#[cbor(key = "iss")])];
        assert!(
            parse_field_cbor_attrs(&attrs)
                .unwrap_err()
                .to_string()
                .contains("integer literal")
        );
    }

    #[test]
    fn parse_container_serde_attrs_handles_rename_all_and_transparent() {
        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(rename_all = "camelCase")])];
        let parsed = parse_container_serde_attrs(&attrs).unwrap();
        assert_eq!(parsed.rename_all, Some(RenameRule::CamelCase));
        assert!(!parsed.transparent);

        let attrs: Vec<Attribute> =
            vec![parse_quote!(#[serde(rename_all(serialize = "kebab-case"))])];
        assert_eq!(
            parse_container_serde_attrs(&attrs).unwrap().rename_all,
            Some(RenameRule::KebabCase)
        );

        // A deserialize-only rule does not change serialized names.
        let attrs: Vec<Attribute> =
            vec![parse_quote!(#[serde(rename_all(deserialize = "camelCase"))])];
        assert_eq!(
            parse_container_serde_attrs(&attrs).unwrap().rename_all,
            None
        );

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(transparent)])];
        assert!(parse_container_serde_attrs(&attrs).unwrap().transparent);

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(rename_all = "weirdCase")])];
        assert!(
            parse_container_serde_attrs(&attrs)
                .unwrap_err()
                .to_string()
                .contains("unknown #[serde(rename_all")
        );
    }

    #[test]
    fn rename_rules_match_serde_behavior() {
        let cases = [
            (RenameRule::LowerCase, "field_name", "field_name"),
            (RenameRule::UpperCase, "field_name", "FIELD_NAME"),
            (RenameRule::PascalCase, "field_name", "FieldName"),
            (RenameRule::CamelCase, "field_name", "fieldName"),
            (RenameRule::SnakeCase, "field_name", "field_name"),
            (RenameRule::ScreamingSnakeCase, "field_name", "FIELD_NAME"),
            (RenameRule::KebabCase, "field_name", "field-name"),
            (RenameRule::ScreamingKebabCase, "field_name", "FIELD-NAME"),
            // serde drops the leading underscore for PascalCase/camelCase.
            (RenameRule::CamelCase, "_id", "id"),
            (RenameRule::PascalCase, "_id", "Id"),
        ];
        for (rule, input, expected) in cases {
            assert_eq!(rule.apply_to_field(input), expected, "{rule:?}({input:?})");
        }
    }

    #[test]
    fn validate_schema_field_name_enforces_anda_db_rules() {
        assert!(validate_schema_field_name("created_at").is_ok());
        assert!(validate_schema_field_name("_id").is_ok());
        assert!(validate_schema_field_name("a1").is_ok());

        assert!(
            validate_schema_field_name("")
                .unwrap_err()
                .contains("empty")
        );
        assert!(
            validate_schema_field_name("createdAt")
                .unwrap_err()
                .contains("invalid character 'A'")
        );
        assert!(
            validate_schema_field_name("created-at")
                .unwrap_err()
                .contains("invalid character '-'")
        );
        assert!(
            validate_schema_field_name(&"x".repeat(65))
                .unwrap_err()
                .contains("exceeds the limit")
        );
    }

    #[test]
    fn effective_field_name_prefers_explicit_rename_over_rename_all() {
        let renamed = FieldSerdeAttrs {
            rename: Some("explicit".to_string()),
            ..Default::default()
        };
        assert_eq!(
            effective_field_name("field_name", &renamed, Some(RenameRule::CamelCase)),
            "explicit"
        );

        let plain = FieldSerdeAttrs::default();
        assert_eq!(
            effective_field_name("field_name", &plain, Some(RenameRule::CamelCase)),
            "fieldName"
        );
        assert_eq!(
            effective_field_name("field_name", &plain, None),
            "field_name"
        );
    }

    #[test]
    fn find_field_type_attr_accepts_string_and_rejects_bad_forms() {
        let attrs: Vec<Attribute> = vec![parse_quote!(#[field_type = "Map<Bytes, U64>"])];
        let parsed = tokens(find_field_type_attr(&attrs).unwrap().unwrap());
        assert!(parsed.contains("FieldType :: Map"));
        assert!(parsed.contains("FieldKey :: from (b\"*\")"));

        let attrs: Vec<Attribute> = vec![parse_quote!(#[serde(default)])];
        assert!(find_field_type_attr(&attrs).unwrap().is_none());

        let attrs: Vec<Attribute> = vec![parse_quote!(#[field_type("Text")])];
        assert!(
            find_field_type_attr(&attrs)
                .unwrap_err()
                .to_string()
                .contains("must use the form")
        );

        let attrs: Vec<Attribute> = vec![parse_quote!(#[field_type = 7])];
        assert!(
            find_field_type_attr(&attrs)
                .unwrap_err()
                .to_string()
                .contains("must be a string literal")
        );
    }

    #[test]
    fn parse_field_type_str_covers_primitives_nested_maps_and_errors() {
        for (input, expected) in [
            ("Bytes", "FieldType :: Bytes"),
            ("Text", "FieldType :: Text"),
            ("U64", "FieldType :: U64"),
            ("I64", "FieldType :: I64"),
            ("F64", "FieldType :: F64"),
            ("F32", "FieldType :: F32"),
            ("Bool", "FieldType :: Bool"),
            ("Json", "FieldType :: Json"),
            ("Vector", "FieldType :: Vector"),
        ] {
            assert_eq!(tokens(parse_ft(input).unwrap()), expected);
        }

        let array = tokens(parse_ft("Array<Option<Text>>").unwrap());
        assert!(array.contains("FieldType :: Array"));
        assert!(array.contains("FieldType :: Option"));

        let map = tokens(parse_ft("Map<Text, Array<U64>>").unwrap());
        assert!(map.contains("FieldKey :: from (\"*\")"));
        assert!(map.contains("FieldType :: Array"));

        let i64_map = tokens(parse_ft("Map<I64, Text>").unwrap());
        assert!(i64_map.contains("FieldKey :: from (i64 :: MIN)"));
        assert!(i64_map.contains("FieldType :: Text"));
        assert!(
            tokens(parse_ft("Map<isize, Text>").unwrap()).contains("FieldKey :: from (i64 :: MIN)")
        );

        assert!(
            parse_ft("Map<Text>")
                .unwrap_err()
                .to_string()
                .contains("Invalid Map field type")
        );
        assert!(
            parse_ft("Map<U64, Text>")
                .unwrap_err()
                .to_string()
                .contains("Unsupported Map key type")
        );
        assert!(
            parse_ft("Unsupported")
                .unwrap_err()
                .to_string()
                .contains("Unsupported field type")
        );
        assert!(
            parse_ft("Array<Junk>")
                .unwrap_err()
                .to_string()
                .contains("Unsupported field type")
        );
    }

    #[test]
    fn determine_field_type_covers_paths_collections_maps_and_errors() {
        let ty: Type = parse_quote!(serde_json::Value);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Json"
        );

        // A bare `Option` (no generic argument) cannot be inferred.
        let ty: Type = parse_quote!(Option);
        assert!(
            determine_field_type(&ty)
                .unwrap_err()
                .to_string()
                .contains("Option element type")
        );

        let ty: Type = parse_quote!(Option<Vec<String>>);
        let inferred = tokens(determine_field_type(&ty).unwrap());
        assert!(inferred.contains("FieldType :: Option"));
        assert!(inferred.contains("FieldType :: Array"));

        for input in ["String", "str", "bool", "i32", "u32", "f32", "f64"] {
            let ty: Type = syn::parse_str(input).unwrap();
            assert!(tokens(determine_field_type(&ty).unwrap()).contains("FieldType"));
        }

        let ty: Type = parse_quote!(Vec<u8>);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Bytes"
        );

        let ty: Type = parse_quote!(Vec<bf16>);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Vector"
        );

        let ty: Type = parse_quote!(Vec<String>);
        assert!(tokens(determine_field_type(&ty).unwrap()).contains("FieldType :: Array"));

        let ty: Type = parse_quote!(Vec);
        assert!(
            determine_field_type(&ty)
                .unwrap_err()
                .to_string()
                .contains("Unable to determine Vec element type")
        );

        let ty: Type = parse_quote!(BTreeMap<String, Vec<u8>>);
        let inferred = tokens(determine_field_type(&ty).unwrap());
        assert!(inferred.contains("FieldKey :: from (\"*\")"));
        assert!(inferred.contains("FieldType :: Bytes"));

        let ty: Type = parse_quote!(HashMap<Vec<u8>, String>);
        assert!(tokens(determine_field_type(&ty).unwrap()).contains("FieldKey :: from (b\"*\")"));

        let ty: Type = parse_quote!(BTreeMap<i64, String>);
        assert!(
            tokens(determine_field_type(&ty).unwrap()).contains("FieldKey :: from (i64 :: MIN)")
        );

        // HashMap with a custom hasher still infers from the first two args.
        let ty: Type = parse_quote!(HashMap<String, u64, RandomState>);
        assert!(tokens(determine_field_type(&ty).unwrap()).contains("FieldType :: U64"));

        let ty: Type = parse_quote!(HashMap<[u8; 4], String>);
        let err = determine_field_type(&ty).unwrap_err().to_string();
        assert!(err.contains("Map key type must be String, signed integer, or bytes"));
        assert!(err.contains("[u8 ; 4]"));

        let ty: Type = parse_quote!(HashMap<String>);
        assert!(
            determine_field_type(&ty)
                .unwrap_err()
                .to_string()
                .contains("Invalid map type")
        );

        let ty: Type = parse_quote!(serde_bytes::ByteBuf);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Bytes"
        );

        let ty: Type = parse_quote!(CustomType);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "< CustomType > :: field_type ()"
        );

        // Generic user-defined types stay valid in expression position.
        let ty: Type = parse_quote!(Wrapper<Inner>);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "< Wrapper < Inner > > :: field_type ()"
        );

        let ty: Type = parse_quote!(half::bf16);
        assert!(
            determine_field_type(&ty)
                .unwrap_err()
                .to_string()
                .contains("Standalone `bf16`")
        );

        let ty: Type = parse_quote!(bf16);
        assert!(
            determine_field_type(&ty)
                .unwrap_err()
                .to_string()
                .contains("Standalone `bf16`")
        );
    }

    #[test]
    fn determine_field_type_unwraps_transparent_wrappers() {
        for (input, expected) in [
            ("Box<str>", "FieldType :: Text"),
            ("std::sync::Arc<String>", "FieldType :: Text"),
            ("Rc<Vec<u8>>", "FieldType :: Bytes"),
            ("Cow<'a, str>", "FieldType :: Text"),
            ("std::borrow::Cow<'static, str>", "FieldType :: Text"),
            ("Box<[u8]>", "FieldType :: Bytes"),
        ] {
            let ty: Type = syn::parse_str(input).unwrap();
            assert_eq!(
                tokens(determine_field_type(&ty).unwrap()),
                expected,
                "{input}"
            );
        }

        let ty: Type = parse_quote!(Box);
        assert!(
            determine_field_type(&ty)
                .unwrap_err()
                .to_string()
                .contains("inner type")
        );
    }

    #[test]
    fn determine_field_type_peels_parens_and_invisible_groups() {
        let ty: Type = parse_quote!((String));
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Text"
        );

        // `macro_rules!` substitution wraps types in invisible groups.
        let grouped = Type::Group(syn::TypeGroup {
            group_token: Default::default(),
            elem: Box::new(parse_quote!(Vec<u8>)),
        });
        assert_eq!(
            tokens(determine_field_type(&grouped).unwrap()),
            "FieldType :: Bytes"
        );
    }

    #[test]
    fn determine_field_type_covers_references_slices_arrays_and_unsupported() {
        let ty: Type = parse_quote!(&String);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Text"
        );

        let ty: Type = syn::parse_str("[u8]").unwrap();
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Bytes"
        );

        let ty: Type = syn::parse_str("[bf16]").unwrap();
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Vector"
        );

        let ty: Type = syn::parse_str("[String]").unwrap();
        assert!(tokens(determine_field_type(&ty).unwrap()).contains("FieldType :: Array"));

        let ty: Type = parse_quote!([u8; 16]);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Bytes"
        );

        let ty: Type = parse_quote!([bf16; 3]);
        assert_eq!(
            tokens(determine_field_type(&ty).unwrap()),
            "FieldType :: Vector"
        );

        let ty: Type = parse_quote!([String; 2]);
        assert!(tokens(determine_field_type(&ty).unwrap()).contains("FieldType :: Array"));

        let ty: Type = parse_quote!((u64, u64));
        let err = determine_field_type(&ty).unwrap_err().to_string();
        assert!(err.contains("Unsupported type"));
        // The message shows the Rust type, not an AST debug dump.
        assert!(err.contains("u64 , u64"));
    }

    #[test]
    fn primitive_type_helpers_cover_true_and_false_cases() {
        let u8_ty: Type = parse_quote!(u8);
        let u64_ty: Type = parse_quote!(u64);
        let string_ty: Type = parse_quote!(String);
        let i64_ty: Type = parse_quote!(i64);
        let isize_ty: Type = parse_quote!(isize);
        let vec_u8_ty: Type = parse_quote!(Vec<u8>);
        let bytes_ty: Type = parse_quote!(ByteBufB64);
        let bf16_ty: Type = parse_quote!(bf16);
        let tuple_ty: Type = parse_quote!((u8, u8));

        assert!(is_u8_type(&u8_ty));
        assert!(!is_u8_type(&u64_ty));
        assert!(is_u64_type(&u64_ty));
        assert!(!is_u64_type(&u8_ty));
        assert!(is_string_type(&string_ty));
        assert!(!is_string_type(&u8_ty));
        assert!(is_signed_integer_type(&i64_ty));
        assert!(is_signed_integer_type(&isize_ty));
        assert!(!is_signed_integer_type(&u64_ty));
        assert!(is_bytes_type(&vec_u8_ty));
        assert!(is_bytes_type(&bytes_ty));
        assert!(!is_bytes_type(&tuple_ty));
        assert!(is_bf16_type(&bf16_ty));
        assert!(!is_bf16_type(&string_ty));
    }
}
