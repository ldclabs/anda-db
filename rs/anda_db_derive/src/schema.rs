use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Expr, Fields, Lit, ext::IdentExt, parse_macro_input};

use crate::common::{determine_field_type, find_field_type_attr, find_rename_attr, is_u64_type};

/// Implementation of `#[derive(AndaDBSchema)]`.
///
/// For each named field of the input struct this function produces an
/// `impl <Struct> { pub fn schema() -> Result<Schema, SchemaError> { ... } }`
/// block that builds an `anda_db_schema::Schema` via `Schema::builder()`.
///
/// The `_id: u64` field is recognised specially: it must exist with the
/// correct type (the schema builder injects the entry automatically) and is
/// otherwise skipped during code generation. Unique constraints, doc-comment
/// descriptions, custom `field_type` overrides and serde renames are all
/// honoured here -- see the crate-level docs for the full attribute list.
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident.unraw();

    // Only structs with named fields are supported.
    let fields = if let Data::Struct(data_struct) = &input.data {
        match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => {
                return TokenStream::from(quote! {
                    compile_error!("AndaDBSchema only supports structs with named fields");
                });
            }
        }
    } else {
        return TokenStream::from(quote! {
            compile_error!("AndaDBSchema only supports structs");
        });
    };

    // Build one `builder.add_field(...)` invocation per field (except `_id`).
    let field_entries = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref().unwrap().unraw();
        let field_name_str = field_name.to_string();

        // Honour `#[serde(rename = "...")]` for the schema field name.
        let rename_attr = find_rename_attr(&field.attrs).unwrap_or_else(|| field_name_str.clone());

        // Doc comments become the schema field description.
        let description = extract_doc_comments(&field.attrs);

        // `#[field_type = "..."]` wins over auto-inference.
        let custom_field_type = find_field_type_attr(&field.attrs);

        let field_type = if let Some(field_type) = custom_field_type {
            quote! { #field_type }
        } else {
            match determine_field_type(&field.ty) {
                Ok(field_type) => field_type,
                Err(err_msg) => {
                    // Surface the inference failure as a compile error.
                    return Some(quote! {
                        compile_error!(#err_msg);
                    });
                }
            }
        };

        // The `_id` column is provided automatically by `SchemaBuilder`.
        if field_name_str == "_id" {
            // ...but the user-declared type must still match `u64`.
            if !is_u64_type(&field.ty) {
                return Some(quote! {
                    compile_error!("The '_id' field must be of type u64");
                });
            }

            return None;
        }

        // `#[unique]` adds a unique constraint to the generated entry.
        let is_unique = has_unique_attr(&field.attrs);

        // Generate field entry creation
        let field_entry_creation = if description.is_empty() {
            if is_unique {
                quote! {
                    FieldEntry::new(#rename_attr.to_string(), #field_type)?.with_unique()
                }
            } else {
                quote! {
                    FieldEntry::new(#rename_attr.to_string(), #field_type)?
                }
            }
        } else if is_unique {
            quote! {
                FieldEntry::new(#rename_attr.to_string(), #field_type)?
                    .with_description(#description.to_string())
                    .with_unique()
            }
        } else {
            quote! {
                FieldEntry::new(#rename_attr.to_string(), #field_type)?
                    .with_description(#description.to_string())
            }
        };

        Some(quote! {
            builder.add_field(#field_entry_creation)?;
        })
    });

    // Generate the schema function implementation
    let expanded = quote! {
        impl #name {
            pub fn schema() -> Result<Schema, SchemaError> {
                let mut builder = Schema::builder();

                #(#field_entries)*

                builder.build()
            }
        }
    };

    TokenStream::from(expanded)
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
