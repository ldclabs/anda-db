use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Attribute, DeriveInput, Expr, Lit, ext::IdentExt, parse_macro_input};

use crate::common::{
    effective_field_name, is_u64_type, named_fields, parse_container_serde_attrs,
    parse_field_serde_attrs, resolve_field_type, validate_schema_field_name,
};

/// Implementation of `#[derive(AndaDBSchema)]`.
///
/// For each named field of the input struct this function produces an
/// `impl <Struct> { pub fn schema() -> Result<Schema, SchemaError> { ... } }`
/// block that builds an `anda_db_schema::Schema` via `Schema::builder()`.
///
/// The `_id: u64` field is recognised specially: when declared it must have
/// the correct type and serialize as `"_id"` (the schema builder injects the
/// entry automatically) and is otherwise skipped during code generation.
/// Unique constraints, doc-comment descriptions, custom `field_type`
/// overrides and serde renames/skips are all honoured here -- see the
/// crate-level docs for the full attribute list.
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);
    TokenStream::from(expand_anda_db_schema_derive(input))
}

pub(crate) fn expand_anda_db_schema_derive(input: DeriveInput) -> TokenStream2 {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Only structs with named fields are supported.
    let fields = match named_fields(&input, "AndaDBSchema") {
        Ok(fields) => fields,
        Err(err) => return err.to_compile_error(),
    };

    let container = match parse_container_serde_attrs(&input.attrs) {
        Ok(container) => container,
        Err(err) => return err.to_compile_error(),
    };
    if container.transparent {
        return syn::Error::new_spanned(
            &input.ident,
            "AndaDBSchema does not support #[serde(transparent)]: the struct serializes as its inner field, not as a map",
        )
        .to_compile_error();
    }

    // Build one `builder.add_field(...)` invocation per serialized field
    // (except `_id`). Errors are emitted in place so that every offending
    // field is reported at once.
    let mut seen_names = std::collections::BTreeSet::new();
    let mut field_entries = Vec::with_capacity(fields.len());
    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let rust_name = field_ident.unraw().to_string();
        let serde_attrs = parse_field_serde_attrs(&field.attrs);

        // The `_id` column is provided automatically by `SchemaBuilder`; the
        // user-declared field is validated and then skipped.
        if rust_name == "_id" {
            if !is_u64_type(&field.ty) {
                field_entries.push(
                    syn::Error::new_spanned(&field.ty, "The '_id' field must be of type u64")
                        .to_compile_error(),
                );
            } else if !serde_attrs.skip_serializing {
                // serde must keep serializing the primary key as "_id",
                // otherwise stored documents would not match the schema.
                let schema_name =
                    effective_field_name(&rust_name, &serde_attrs, container.rename_all);
                if schema_name != "_id" {
                    field_entries.push(
                        syn::Error::new_spanned(
                            field_ident,
                            format!(
                                "serde renames `_id` to {schema_name:?}, but the primary key must serialize as \"_id\"; add #[serde(rename = \"_id\")]"
                            ),
                        )
                        .to_compile_error(),
                    );
                }
            }
            continue;
        }

        // Fields serde never serializes must not appear in the schema.
        if serde_attrs.skip_serializing {
            continue;
        }
        if serde_attrs.flatten {
            field_entries.push(
                syn::Error::new_spanned(
                    field_ident,
                    "#[serde(flatten)] is not supported: flattened keys are inlined into the parent map and cannot be described by a single schema field",
                )
                .to_compile_error(),
            );
            continue;
        }

        // Schema field names follow the serialized names: serde renames and
        // container-level rename_all rules are honoured.
        let schema_name = effective_field_name(&rust_name, &serde_attrs, container.rename_all);

        // Reject names AndaDB would refuse at runtime (`FieldEntry::new`
        // accepts only `[a-z0-9_]{1,64}`): a document serialized with such a
        // key could never be stored, so fail at compile time instead.
        if let Err(reason) = validate_schema_field_name(&schema_name) {
            field_entries.push(
                syn::Error::new_spanned(
                    field_ident,
                    format!(
                        "schema field name {schema_name:?} is not a valid AndaDB field name ({reason}); \
                         field names must match [a-z0-9_]{{1,64}}. Adjust the field name or its #[serde(rename...)] attributes"
                    ),
                )
                .to_compile_error(),
            );
            continue;
        }

        if schema_name == "_id" {
            field_entries.push(
                syn::Error::new_spanned(
                    field_ident,
                    format!(
                        "field {rust_name:?} serializes as \"_id\", which collides with the auto-generated primary key"
                    ),
                )
                .to_compile_error(),
            );
            continue;
        }
        if !seen_names.insert(schema_name.clone()) {
            field_entries.push(
                syn::Error::new_spanned(
                    field_ident,
                    format!("duplicate schema field name {schema_name:?} (after serde renaming)"),
                )
                .to_compile_error(),
            );
            continue;
        }

        // `#[field_type = "..."]` wins over auto-inference.
        let field_type = match resolve_field_type(field) {
            Ok(field_type) => field_type,
            Err(err) => {
                field_entries.push(err.to_compile_error());
                continue;
            }
        };

        // Doc comments become the schema field description, and `#[unique]`
        // adds a unique constraint to the generated entry.
        let mut entry = quote! { FieldEntry::new(#schema_name.to_string(), #field_type)? };
        let description = extract_doc_comments(&field.attrs);
        if !description.is_empty() {
            entry = quote! { #entry.with_description(#description.to_string()) };
        }
        if has_unique_attr(&field.attrs) {
            entry = quote! { #entry.with_unique() };
        }

        field_entries.push(quote! {
            builder.add_field(#entry)?;
        });
    }

    // Avoid an `unused_mut` warning when the struct declares no field other
    // than `_id`.
    let builder_binding = if field_entries.is_empty() {
        quote! { let builder = Schema::builder(); }
    } else {
        quote! { let mut builder = Schema::builder(); }
    };

    // Generate the schema function implementation.
    quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            #[doc = "Returns the AndaDB `Schema` derived from this struct's serialized fields.\n\nThe `_id` primary-key column is injected automatically by the schema builder.\nGenerated by `#[derive(AndaDBSchema)]`."]
            pub fn schema() -> Result<Schema, SchemaError> {
                #builder_binding

                #(#field_entries)*

                builder.build()
            }
        }
    }
}

/// Returns `true` if any of the supplied attributes is `#[unique]`.
fn has_unique_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident("unique"))
}

/// Concatenate all `///` doc comments on a field into a single description
/// string, separated by spaces. Empty comments are dropped.
fn extract_doc_comments(attrs: &[Attribute]) -> String {
    let mut doc_comments = Vec::new();

    for attr in attrs {
        if attr.path().is_ident("doc")
            && let Ok(meta_name_value) = attr.meta.require_name_value()
            && let Expr::Lit(expr_lit) = &meta_name_value.value
            && let Lit::Str(lit_str) = &expr_lit.lit
        {
            let comment = lit_str.value().trim().to_string();
            if !comment.is_empty() {
                doc_comments.push(comment);
            }
        }
    }

    doc_comments.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn tokens(input: TokenStream2) -> String {
        input.to_string()
    }

    #[test]
    fn expand_schema_generates_builder_for_supported_fields() {
        let input: DeriveInput = parse_quote! {
            struct User<T>
            where
                T: Clone
            {
                /// managed id ignored by generated fields
                _id: u64,
                /// Display
                /// name
                #[serde(rename = "display_name")]
                #[unique]
                name: String,
                #[field_type = "Option<Array<Text>>"]
                tags: Vec<String>,
                nested: T,
            }
        };

        let expanded = tokens(expand_anda_db_schema_derive(input));
        assert!(expanded.contains("impl < T > User < T > where T : Clone"));
        assert!(expanded.contains("Schema :: builder"));
        assert!(expanded.contains("\"display_name\""));
        assert!(expanded.contains("with_description (\"Display name\""));
        assert!(expanded.contains("with_unique"));
        assert!(expanded.contains("FieldType :: Option"));
        assert!(expanded.contains("< T > :: field_type ()"));
        assert!(!expanded.contains("\"_id\" . to_string"));
        assert!(!expanded.contains("compile_error"));
    }

    #[test]
    fn expand_schema_honours_rename_all_and_skip() {
        // `snake_case` / `lowercase` rules keep names valid for AndaDB.
        let input: DeriveInput = parse_quote! {
            #[serde(rename_all = "snake_case")]
            struct Payload {
                _id: u64,
                created_at: u64,
                #[serde(rename = "explicit_name")]
                some_field: String,
                #[serde(skip)]
                local_cache: String,
                #[serde(skip_serializing)]
                more_cache: String,
            }
        };

        let expanded = tokens(expand_anda_db_schema_derive(input));
        assert!(expanded.contains("\"created_at\""));
        assert!(expanded.contains("\"explicit_name\""));
        assert!(!expanded.contains("some_field"));
        // Skipped fields never appear in the serialized form, so they are
        // excluded from the generated schema.
        assert!(!expanded.contains("local_cache"));
        assert!(!expanded.contains("more_cache"));
        assert!(!expanded.contains("compile_error"));
    }

    #[test]
    fn expand_schema_rejects_names_anda_db_cannot_store() {
        // camelCase produces "createdAt", which `FieldEntry::new` would
        // reject at runtime -- the macro must reject it at compile time.
        let input: DeriveInput = parse_quote! {
            #[serde(rename_all = "camelCase")]
            struct Payload {
                #[serde(rename = "_id")]
                _id: u64,
                created_at: u64,
            }
        };
        let expanded = tokens(expand_anda_db_schema_derive(input));
        assert!(expanded.contains("not a valid AndaDB field name"));
        assert!(expanded.contains("createdAt"));

        // Same for an explicit rename to an invalid name.
        let input: DeriveInput = parse_quote! {
            struct Payload {
                #[serde(rename = "Bad-Name")]
                value: u64,
            }
        };
        assert!(
            tokens(expand_anda_db_schema_derive(input)).contains("not a valid AndaDB field name")
        );
    }

    #[test]
    fn expand_schema_elides_mut_for_id_only_structs() {
        let input: DeriveInput = parse_quote! {
            struct OnlyId {
                _id: u64,
            }
        };

        let expanded = tokens(expand_anda_db_schema_derive(input));
        assert!(expanded.contains("let builder"));
        assert!(!expanded.contains("let mut builder"));
        assert!(!expanded.contains("compile_error"));
    }

    #[test]
    fn expand_schema_rejects_unsupported_shapes_and_bad_fields() {
        let tuple_struct: DeriveInput = parse_quote!(
            struct Tuple(u64);
        );
        assert!(
            tokens(expand_anda_db_schema_derive(tuple_struct))
                .contains("AndaDBSchema only supports structs with named fields")
        );

        let enum_input: DeriveInput = parse_quote!(
            enum Choice {
                A,
            }
        );
        assert!(
            tokens(expand_anda_db_schema_derive(enum_input))
                .contains("AndaDBSchema only supports structs")
        );

        let bad_id: DeriveInput = parse_quote! {
            struct BadId {
                _id: String,
            }
        };
        assert!(
            tokens(expand_anda_db_schema_derive(bad_id))
                .contains("The '_id' field must be of type u64")
        );

        let bad_attr: DeriveInput = parse_quote! {
            struct BadAttr {
                _id: u64,
                #[field_type(Text)]
                value: String,
            }
        };
        assert!(tokens(expand_anda_db_schema_derive(bad_attr)).contains("field_type"));

        let bad_type: DeriveInput = parse_quote! {
            struct BadType {
                _id: u64,
                value: (u64, u64),
            }
        };
        assert!(tokens(expand_anda_db_schema_derive(bad_type)).contains("Unsupported type"));
    }

    #[test]
    fn expand_schema_guards_the_reserved_id_column() {
        // rename_all would serialize `_id` as "id": stored documents could
        // never match the schema, so this must be rejected.
        let input: DeriveInput = parse_quote! {
            #[serde(rename_all = "camelCase")]
            struct BadId {
                _id: u64,
                created_at: u64,
            }
        };
        assert!(
            tokens(expand_anda_db_schema_derive(input)).contains("must serialize as \\\"_id\\\"")
        );

        // Renaming another field to `_id` collides with the primary key.
        let input: DeriveInput = parse_quote! {
            struct Collide {
                #[serde(rename = "_id")]
                key: u64,
            }
        };
        assert!(tokens(expand_anda_db_schema_derive(input)).contains("collides"));

        // Two fields serializing under one name are rejected.
        let input: DeriveInput = parse_quote! {
            struct Duplicate {
                #[serde(rename = "name")]
                a: String,
                name: String,
            }
        };
        assert!(
            tokens(expand_anda_db_schema_derive(input)).contains("duplicate schema field name")
        );

        // Flattened fields cannot be described by the schema.
        let input: DeriveInput = parse_quote! {
            struct WithFlatten {
                _id: u64,
                #[serde(flatten)]
                extra: std::collections::HashMap<String, String>,
            }
        };
        assert!(
            tokens(expand_anda_db_schema_derive(input))
                .contains("#[serde(flatten)] is not supported")
        );
    }

    #[test]
    fn extract_doc_comments_skips_empty_and_non_doc_attributes() {
        let field: syn::Field = parse_quote! {
            /// First
            #[serde(rename = "ignored")]
            ///
            /// Second
            value: String
        };

        assert_eq!(extract_doc_comments(&field.attrs), "First Second");
        assert!(!has_unique_attr(&field.attrs));
    }
}
