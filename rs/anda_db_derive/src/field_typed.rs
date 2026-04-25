use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, ext::IdentExt, parse_macro_input};

use crate::common::{determine_field_type, find_field_type_attr, find_rename_attr};

/// Implementation of `#[derive(FieldTyped)]`.
///
/// Generates an inherent `pub fn field_type() -> FieldType` method that
/// returns a `FieldType::Map` whose keys are the (possibly serde-renamed)
/// field names and whose values are the inferred (or explicitly overridden)
/// `FieldType` for each field.
///
/// This is the workhorse for nested types: when [`super::schema::anda_db_schema_derive`]
/// or `determine_field_type` encounters a user-defined struct, it calls
/// `<Struct>::field_type()` to recover its schema fragment.
pub fn field_typed_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident.unraw();

    // Only structs with named fields are supported.
    let fields = if let Data::Struct(data_struct) = &input.data {
        match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            _ => {
                return TokenStream::from(quote! {
                    compile_error!("FieldTyped only supports structs with named fields");
                });
            }
        }
    } else {
        return TokenStream::from(quote! {
            compile_error!("FieldTyped only supports structs");
        });
    };

    // For each field, emit a `("name".into(), <FieldType>)` tuple that will be
    // collected into the resulting `FieldType::Map`.
    let field_type_mappings = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap().unraw();
        let field_name_str = field_name.to_string();

        // Honour `#[serde(rename = "...")]` for the map key.
        let rename_attr = find_rename_attr(&field.attrs).unwrap_or_else(|| field_name_str.clone());

        // `#[field_type = "..."]` overrides auto-inference.
        let custom_field_type = find_field_type_attr(&field.attrs);

        let field_type = if let Some(field_type) = custom_field_type {
            quote! { #field_type }
        } else {
            match determine_field_type(&field.ty) {
                Ok(field_type) => field_type,
                Err(err_msg) => {
                    // Emit a compile error in place of the tuple.
                    return quote! {
                        compile_error!(#err_msg)
                    };
                }
            }
        };

        quote! {
            (#rename_attr.into(), #field_type)
        }
    });

    // Stitch the tuples into the final `field_type()` accessor.
    let expanded = quote! {
        impl #name {
            pub fn field_type() -> FieldType {
                FieldType::Map(
                    vec![
                        #(#field_type_mappings),*
                    ]
                    .into_iter()
                    .collect(),
                )
            }
        }
    };

    TokenStream::from(expanded)
}
