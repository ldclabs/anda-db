//! Test the actual vendored schemas, including their transitive references.
//! Resources are local and pinned; no network lookup is permitted.
#![allow(dead_code)]
use jsonschema::{Retrieve, Uri, Validator};
use serde_json::Value;

/// The crate's own closed resource set (`anda_kip::vendored_schemas`), so a
/// test resolves exactly the references a runtime validator would.
struct Vendored;
impl Retrieve for Vendored {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        anda_kip::vendored_schemas()
            .get(uri.as_str())
            .cloned()
            .ok_or_else(|| format!("unregistered schema {uri}").into())
    }
}

pub struct Schema(Validator);
impl Schema {
    pub fn new(source: &str) -> Self {
        let schema: Value = serde_json::from_str(source).expect("valid schema JSON");
        Self(
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .with_retriever(Vendored)
                .should_validate_formats(true)
                .with_format("date-time", |value| {
                    anda_kip::timestamp::parse(value, "timestamp").is_ok()
                })
                .with_format("timestamp", |value| {
                    anda_kip::timestamp::parse(value, "timestamp").is_ok()
                })
                .build(&schema)
                .expect("vendored schema compiles"),
        )
    }
    pub fn validate(&self, instance: &Value) -> Vec<String> {
        self.0
            .iter_errors(instance)
            .map(|error| error.to_string())
            .collect()
    }
    pub fn assert_valid(&self, what: &str, instance: &Value) {
        let errors = self.validate(instance);
        assert!(
            errors.is_empty(),
            "{what}: {}\n{instance}",
            errors.join("; ")
        );
    }
}
