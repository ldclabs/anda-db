use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Expr, Fields, Lit, ext::IdentExt, parse_macro_input};

use crate::common::{determine_field_type, find_field_type_attr, find_rename_attr, is_u64_type};

/// A derive macro that generates a `schema()` function for structs.
/// This generates an AndaDB Schema definition based on the struct fields.
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident.unraw();

    // Process struct fields
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

    // Generate field entries for schema builder
    let field_entries = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref().unwrap().unraw();
        let field_name_str = field_name.to_string();

        // Get renamed field from serde attribute if present
        let rename_attr = find_rename_attr(&field.attrs).unwrap_or_else(|| field_name_str.clone());

        // Check for field description from doc comments
        let description = extract_doc_comments(&field.attrs);

        // Check if there's a custom field_type attribute
        let custom_field_type = find_field_type_attr(&field.attrs);

        // Determine the field type
        let field_type = if let Some(field_type) = custom_field_type {
            quote! { #field_type }
        } else {
            match determine_field_type(&field.ty) {
                Ok(field_type) => field_type,
                Err(err_msg) => {
                    // Generate a compile error for unsupported types
                    return Some(quote! {
                        compile_error!(#err_msg);
                    });
                }
            }
        };

        // Skip the '_id' field as it's automatically added by SchemaBuilder
        if field_name_str == "_id" {
            // Check if _id field is u64 type by examining the actual Rust type
            if !is_u64_type(&field.ty) {
                return Some(quote! {
                    compile_error!("The '_id' field must be of type u64");
                });
            }

            return None;
        }

        // Check if field is unique (from unique attribute)
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

/// 检查是否有 unique 属性
fn has_unique_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident("unique"))
}

/// 提取文档注释作为描述
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
