use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, ext::IdentExt, parse_macro_input};

use crate::common::{
    effective_field_name, named_fields, parse_container_serde_attrs, parse_field_cbor_attrs,
    parse_field_serde_attrs, resolve_field_type, schema_crate_path,
};

/// Implementation of `#[derive(FieldTyped)]`.
///
/// Generates an inherent `pub fn field_type() -> FieldType` method that
/// returns a `FieldType::Map` whose keys are the serialized field names
/// (honouring serde renames) and whose values are the inferred (or
/// explicitly overridden) `FieldType` for each serialized field.
///
/// This is the workhorse for nested types: when [`super::schema::anda_db_schema_derive`]
/// or `determine_field_type` encounters a user-defined struct, it calls
/// `<Struct>::field_type()` to recover its schema fragment.
pub fn field_typed_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree.
    let input = parse_macro_input!(input as DeriveInput);
    TokenStream::from(expand_field_typed_derive(input))
}

pub(crate) fn expand_field_typed_derive(input: DeriveInput) -> TokenStream2 {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let root = schema_crate_path();
    let type_params: std::collections::BTreeSet<String> = input
        .generics
        .type_params()
        .map(|p| p.ident.to_string())
        .collect();

    // Only structs with named fields are supported.
    let fields = match named_fields(&input, "FieldTyped") {
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
            "FieldTyped does not support #[serde(transparent)]: the struct serializes as its inner field, not as a map",
        )
        .to_compile_error();
    }

    // For each serialized field, emit a `("name".into(), <FieldType>)` tuple
    // that will be collected into the resulting `FieldType::Map`. Errors are
    // emitted in place so that every offending field is reported at once.
    let mut seen_keys = std::collections::BTreeSet::new();
    let mut field_type_mappings = Vec::with_capacity(fields.len());
    for field in fields {
        let field_ident = field.ident.as_ref().unwrap();
        let serde_attrs = parse_field_serde_attrs(&field.attrs);

        // Fields serde never serializes must not appear in the type map.
        if serde_attrs.skip_serializing {
            continue;
        }
        if serde_attrs.flatten {
            field_type_mappings.push(
                syn::Error::new_spanned(
                    field_ident,
                    "#[serde(flatten)] is not supported: flattened keys are inlined into the parent map and cannot be described by a single schema field",
                )
                .to_compile_error(),
            );
            continue;
        }
        let cbor_attrs = match parse_field_cbor_attrs(&field.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                field_type_mappings.push(err.to_compile_error());
                continue;
            }
        };

        // Schema field names follow the serialized names: serde renames and
        // container-level rename_all rules are honoured unless cbor2 provides
        // an integer map key for the CBOR serialized shape.
        let schema_name = effective_field_name(
            &field_ident.unraw().to_string(),
            &serde_attrs,
            container.rename_all,
        );
        let (field_key, duplicate_key) = if let Some(key) = cbor_attrs.key {
            (quote! { #root::FieldKey::from(#key) }, format!("i64:{key}"))
        } else {
            (
                quote! { #root::FieldKey::from(#schema_name) },
                format!("text:{schema_name}"),
            )
        };
        if !seen_keys.insert(duplicate_key.clone()) {
            field_type_mappings.push(
                syn::Error::new_spanned(
                    field_ident,
                    format!(
                        "duplicate schema field key {duplicate_key:?} (after serde/cbor renaming)"
                    ),
                )
                .to_compile_error(),
            );
            continue;
        }

        // `#[field_type = "..."]` overrides auto-inference.
        match resolve_field_type(field, &root, &type_params) {
            Ok(field_type) => field_type_mappings.push(quote! {
                (#field_key, #field_type)
            }),
            Err(err) => field_type_mappings.push(err.to_compile_error()),
        }
    }

    // Stitch the tuples into the final `field_type()` accessor. Every schema
    // item is referenced through the resolved crate path (never imported into
    // the generated scope) so that user types named `FieldType` / `FieldKey`
    // are not shadowed.
    quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            #[doc = "Returns the `FieldType` map describing this struct's serialized fields.\n\nGenerated by `#[derive(FieldTyped)]`."]
            pub fn field_type() -> #root::FieldType {
                #root::FieldType::Map(
                    ::std::vec![
                        #(#field_type_mappings),*
                    ]
                    .into_iter()
                    .collect(),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn tokens(input: TokenStream2) -> String {
        input.to_string()
    }

    #[test]
    fn expand_field_typed_generates_map_for_named_structs() {
        let input: DeriveInput = parse_quote! {
            struct User<T>
            where
                T: Clone
            {
                #[serde(rename = "displayName")]
                name: String,
                #[field_type = "Option<Array<Text>>"]
                tags: Vec<String>,
                #[field_type = "Json"]
                nested: T,
                wrapped: Wrapper<T>,
            }
        };

        let expanded = tokens(expand_field_typed_derive(input));
        assert!(expanded.contains("impl < T > User < T > where T : Clone"));
        assert!(expanded.contains("\"displayName\""));
        assert!(expanded.contains(":: anda_db_schema :: FieldType :: Option"));
        assert!(expanded.contains(":: anda_db_schema :: FieldType :: Array"));
        // Generic *user* types keep working through their own field_type().
        assert!(expanded.contains("< Wrapper < T > > :: field_type ()"));
        // The generated body must not import bare schema names into scope;
        // every schema item is referenced through the resolved crate path.
        assert!(!expanded.contains("use ::"));
        assert!(!expanded.contains("use anda_db_schema"));
        assert!(expanded.contains(":: anda_db_schema :: FieldType :: Map"));
        assert!(!expanded.contains("compile_error"));
    }

    #[test]
    fn expand_field_typed_rejects_bare_generic_fields_with_clear_error() {
        // A bare `T` has no `field_type()`; the old fallback emitted
        // `<T>::field_type()`, which failed with a misleading E0599 pointing
        // at the derive. Now it is a targeted compile error.
        let input: DeriveInput = parse_quote! {
            struct Wrapper<T> {
                inner: T,
            }
        };

        let expanded = tokens(expand_field_typed_derive(input));
        assert!(expanded.contains("compile_error"));
        assert!(expanded.contains("generic type parameter `T`"));
        assert!(expanded.contains("field_type"));
    }

    #[test]
    fn expand_field_typed_uses_cbor_integer_keys_when_present() {
        let input: DeriveInput = parse_quote! {
            struct Claims {
                #[cbor(key = 1)]
                #[serde(rename = "iss")]
                issuer: Option<String>,
                #[cbor(key = 4)]
                #[serde(rename = "exp")]
                expiration: Option<u64>,
            }
        };

        let expanded = tokens(expand_field_typed_derive(input));
        assert!(expanded.contains(":: anda_db_schema :: FieldKey :: from (1i64)"));
        assert!(expanded.contains(":: anda_db_schema :: FieldKey :: from (4i64)"));
        assert!(!expanded.contains("\"iss\" . into"));
        assert!(!expanded.contains("\"exp\" . into"));
        assert!(!expanded.contains("compile_error"));
    }

    #[test]
    fn expand_field_typed_honours_rename_all_and_skip() {
        let input: DeriveInput = parse_quote! {
            #[serde(rename_all = "camelCase")]
            struct Payload {
                created_at: u64,
                #[serde(skip)]
                local_cache: String,
                #[serde(skip_serializing)]
                more_cache: String,
                #[serde(rename = "explicit")]
                renamed_field: bool,
            }
        };

        let expanded = tokens(expand_field_typed_derive(input));
        assert!(expanded.contains("\"createdAt\""));
        // Skipped fields never appear in the serialized form, so they are
        // excluded from the generated map.
        assert!(!expanded.contains("localCache"));
        assert!(!expanded.contains("moreCache"));
        // An explicit rename wins over the container rule.
        assert!(expanded.contains("\"explicit\""));
        assert!(!expanded.contains("renamedField"));
        assert!(!expanded.contains("compile_error"));
    }

    #[test]
    fn expand_field_typed_rejects_unsupported_inputs_and_bad_fields() {
        let tuple_struct: DeriveInput = parse_quote!(
            struct Tuple(String);
        );
        assert!(
            tokens(expand_field_typed_derive(tuple_struct))
                .contains("FieldTyped only supports structs with named fields")
        );

        let enum_input: DeriveInput = parse_quote!(
            enum Choice {
                A,
            }
        );
        assert!(
            tokens(expand_field_typed_derive(enum_input))
                .contains("FieldTyped only supports structs")
        );

        let bad_attr: DeriveInput = parse_quote! {
            struct BadAttr {
                #[field_type(Text)]
                value: String,
            }
        };
        assert!(tokens(expand_field_typed_derive(bad_attr)).contains("field_type"));

        let bad_type: DeriveInput = parse_quote! {
            struct BadType {
                value: (u64, u64),
            }
        };
        assert!(tokens(expand_field_typed_derive(bad_type)).contains("Unsupported type"));
    }

    #[test]
    fn expand_field_typed_rejects_flatten_transparent_and_duplicates() {
        let flatten: DeriveInput = parse_quote! {
            struct WithFlatten {
                #[serde(flatten)]
                extra: std::collections::HashMap<String, String>,
            }
        };
        assert!(
            tokens(expand_field_typed_derive(flatten))
                .contains("#[serde(flatten)] is not supported")
        );

        let transparent: DeriveInput = parse_quote! {
            #[serde(transparent)]
            struct Wrapper {
                inner: String,
            }
        };
        assert!(
            tokens(expand_field_typed_derive(transparent))
                .contains("does not support #[serde(transparent)]")
        );

        let duplicate: DeriveInput = parse_quote! {
            struct Duplicate {
                #[serde(rename = "name")]
                a: String,
                name: String,
            }
        };
        assert!(
            tokens(expand_field_typed_derive(duplicate)).contains("duplicate schema field key")
        );
    }
}
