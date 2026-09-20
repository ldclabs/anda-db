//! A JSON Schema (draft 2020-12) subset validator, used by `wire_schema.rs`.
//!
//! The vendored wire schemas are the normative description of what goes on the
//! wire, and until something checks the Rust types against them the two can
//! drift silently — which is exactly what happened: a field defined in
//! `kip-response.schema.json` had no Rust counterpart, and two `Default`
//! values serialized to shapes the schema forbids.
//!
//! Pulling in a full validator is not worth a new dependency here. The two
//! schemas use a closed set of keywords, so this implements exactly that set
//! and **panics on anything it does not recognize** rather than passing it.
//! A schema update that introduces a new keyword fails loudly instead of
//! quietly validating nothing.
//!
//! KIP §6.5 requires `date-time`/`timestamp` format assertions. Other format
//! names retain draft 2020-12's default annotation behavior.

#![allow(dead_code)]

use regex::Regex;
use serde_json::Value;

/// Keywords that carry no assertion and are skipped.
const ANNOTATIONS: &[&str] = &[
    "$schema",
    "$id",
    "title",
    "description",
    "$comment",
    "examples",
    "default",
    "deprecated",
    "readOnly",
    "writeOnly",
    "$defs",
];

/// A compiled schema document.
pub struct Schema {
    root: Value,
}

impl Schema {
    /// Parses a schema document.
    pub fn new(source: &str) -> Self {
        Self {
            root: serde_json::from_str(source).expect("schema is valid JSON"),
        }
    }

    /// Validates `instance`, returning every violation found.
    pub fn validate(&self, instance: &Value) -> Vec<String> {
        let mut errors = Vec::new();
        self.check(&self.root, instance, "", &mut errors);
        errors
    }

    /// Validates `instance` and panics with a readable report if it fails.
    pub fn assert_valid(&self, what: &str, instance: &Value) {
        let errors = self.validate(instance);
        assert!(
            errors.is_empty(),
            "{what} does not match its wire schema:\n  {}\n\ninstance:\n{}",
            errors.join("\n  "),
            serde_json::to_string_pretty(instance).unwrap(),
        );
    }

    /// Whether `instance` satisfies `schema`, with no error detail kept.
    fn matches(&self, schema: &Value, instance: &Value) -> bool {
        let mut errors = Vec::new();
        self.check(schema, instance, "", &mut errors);
        errors.is_empty()
    }

    fn resolve<'a>(&'a self, reference: &str) -> &'a Value {
        let name = reference.strip_prefix("#/$defs/").unwrap_or_else(|| {
            panic!("only local #/$defs/... references are supported, found {reference:?}")
        });
        self.root
            .get("$defs")
            .and_then(|defs| defs.get(name))
            .unwrap_or_else(|| panic!("unresolved schema reference {reference:?}"))
    }

    fn check(&self, schema: &Value, instance: &Value, path: &str, errors: &mut Vec<String>) {
        let object = match schema {
            // A boolean schema: `true` accepts everything, `false` nothing.
            Value::Bool(true) => return,
            Value::Bool(false) => {
                errors.push(format!("{}: schema forbids any value", at(path)));
                return;
            }
            Value::Object(object) => object,
            other => panic!("a schema must be an object or a boolean, found {other}"),
        };

        for (keyword, value) in object {
            if ANNOTATIONS.contains(&keyword.as_str()) {
                continue;
            }
            match keyword.as_str() {
                "$ref" => {
                    let target = self.resolve(value.as_str().expect("$ref is a string"));
                    self.check(target, instance, path, errors);
                }
                "type" => self.check_type(value, instance, path, errors),
                "format" => {
                    if matches!(value.as_str(), Some("date-time" | "timestamp"))
                        && let Some(text) = instance.as_str()
                        && let Err(err) = anda_kip::timestamp::parse(text, at(path))
                    {
                        errors.push(err.message);
                    }
                }
                "const" => {
                    if instance != value {
                        errors.push(format!("{}: must equal {value}", at(path)));
                    }
                }
                "enum" => {
                    let allowed = value.as_array().expect("enum is an array");
                    if !allowed.contains(instance) {
                        errors.push(format!("{}: {instance} is not one of {value}", at(path)));
                    }
                }
                "properties" => self.check_properties(value, instance, path, errors),
                "required" => self.check_required(value, instance, path, errors),
                "additionalProperties" => {
                    self.check_additional(object, value, instance, path, errors)
                }
                "propertyNames" => self.check_property_names(value, instance, path, errors),
                "minProperties" => {
                    if let Value::Object(map) = instance {
                        let min = value.as_u64().expect("minProperties is an integer") as usize;
                        if map.len() < min {
                            errors.push(format!(
                                "{}: needs at least {min} properties, has {}",
                                at(path),
                                map.len()
                            ));
                        }
                    }
                }
                "items" => {
                    if let Value::Array(items) = instance {
                        for (index, item) in items.iter().enumerate() {
                            self.check(value, item, &format!("{path}/{index}"), errors);
                        }
                    }
                }
                "minItems" => {
                    if let Value::Array(items) = instance {
                        let min = value.as_u64().expect("minItems is an integer") as usize;
                        if items.len() < min {
                            errors.push(format!(
                                "{}: needs at least {min} items, has {}",
                                at(path),
                                items.len()
                            ));
                        }
                    }
                }
                "contains" => self.check_contains(object, value, instance, path, errors),
                // Counted by `contains`; on its own it asserts nothing.
                "minContains" => {}
                "minLength" | "maxLength" => {
                    self.check_length(keyword, value, instance, path, errors)
                }
                "minimum" | "maximum" => self.check_bound(keyword, value, instance, path, errors),
                "pattern" => self.check_pattern(value, instance, path, errors),
                "allOf" => {
                    for branch in value.as_array().expect("allOf is an array") {
                        self.check(branch, instance, path, errors);
                    }
                }
                "anyOf" => {
                    let branches = value.as_array().expect("anyOf is an array");
                    if !branches.iter().any(|b| self.matches(b, instance)) {
                        errors.push(format!("{}: matches no anyOf branch", at(path)));
                    }
                }
                "oneOf" => {
                    let branches = value.as_array().expect("oneOf is an array");
                    let matched = branches
                        .iter()
                        .filter(|b| self.matches(b, instance))
                        .count();
                    if matched != 1 {
                        errors.push(format!(
                            "{}: must match exactly one oneOf branch, matched {matched}",
                            at(path)
                        ));
                    }
                }
                "not" => {
                    if self.matches(value, instance) {
                        errors.push(format!("{}: must not match this schema", at(path)));
                    }
                }
                "if" => {
                    // `then`/`else` are only meaningful next to `if`, and are
                    // handled here so the branch is evaluated exactly once.
                    let branch = if self.matches(value, instance) {
                        object.get("then")
                    } else {
                        object.get("else")
                    };
                    if let Some(branch) = branch {
                        self.check(branch, instance, path, errors);
                    }
                }
                "then" | "else" => {}
                other => panic!(
                    "this validator does not implement the {other:?} keyword; extend it rather \
                     than letting the schema go unchecked"
                ),
            }
        }
    }

    fn check_type(&self, expected: &Value, instance: &Value, path: &str, errors: &mut Vec<String>) {
        let names: Vec<&str> = match expected {
            Value::String(name) => vec![name.as_str()],
            Value::Array(names) => names
                .iter()
                .map(|n| n.as_str().expect("type name is a string"))
                .collect(),
            other => panic!("type must be a string or an array, found {other}"),
        };
        let ok = names.iter().any(|name| match *name {
            "object" => instance.is_object(),
            "array" => instance.is_array(),
            "string" => instance.is_string(),
            "boolean" => instance.is_boolean(),
            "null" => instance.is_null(),
            // JSON Schema's "integer" accepts a whole number however it is
            // spelled, so 1.0 is an integer and 1.5 is not.
            "integer" => {
                instance.is_i64()
                    || instance.is_u64()
                    || instance.as_f64().is_some_and(|f| f.fract() == 0.0)
            }
            "number" => instance.is_number(),
            other => panic!("unknown type name {other:?}"),
        });
        if !ok {
            errors.push(format!(
                "{}: expected type {expected}, found {}",
                at(path),
                type_of(instance)
            ));
        }
    }

    fn check_properties(
        &self,
        properties: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Value::Object(map) = instance else {
            return;
        };
        for (name, subschema) in properties.as_object().expect("properties is an object") {
            if let Some(value) = map.get(name) {
                self.check(subschema, value, &format!("{path}/{name}"), errors);
            }
        }
    }

    fn check_required(
        &self,
        required: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Value::Object(map) = instance else {
            return;
        };
        for name in required.as_array().expect("required is an array") {
            let name = name.as_str().expect("a required name is a string");
            if !map.contains_key(name) {
                errors.push(format!("{}: missing required property {name:?}", at(path)));
            }
        }
    }

    fn check_additional(
        &self,
        object: &serde_json::Map<String, Value>,
        additional: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Value::Object(map) = instance else {
            return;
        };
        let declared = object.get("properties").and_then(Value::as_object);
        for (name, value) in map {
            if declared.is_some_and(|d| d.contains_key(name)) {
                continue;
            }
            match additional {
                Value::Bool(true) => {}
                Value::Bool(false) => errors.push(format!(
                    "{}: property {name:?} is not allowed here",
                    at(path)
                )),
                subschema => self.check(subschema, value, &format!("{path}/{name}"), errors),
            }
        }
    }

    fn check_property_names(
        &self,
        subschema: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Value::Object(map) = instance else {
            return;
        };
        for name in map.keys() {
            let as_string = Value::String(name.clone());
            let mut inner = Vec::new();
            self.check(subschema, &as_string, path, &mut inner);
            if !inner.is_empty() {
                errors.push(format!(
                    "{}: property name {name:?} is not allowed ({})",
                    at(path),
                    inner.join("; ")
                ));
            }
        }
    }

    fn check_contains(
        &self,
        object: &serde_json::Map<String, Value>,
        subschema: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Value::Array(items) = instance else {
            return;
        };
        let found = items.iter().filter(|i| self.matches(subschema, i)).count();
        let min = object
            .get("minContains")
            .and_then(Value::as_u64)
            .unwrap_or(1) as usize;
        if found < min {
            errors.push(format!(
                "{}: needs at least {min} matching items, found {found}",
                at(path)
            ));
        }
    }

    fn check_length(
        &self,
        keyword: &str,
        value: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Value::String(text) = instance else {
            return;
        };
        // JSON Schema counts characters, not bytes.
        let length = text.chars().count() as u64;
        let bound = value.as_u64().expect("a length bound is an integer");
        let violated = match keyword {
            "minLength" => length < bound,
            _ => length > bound,
        };
        if violated {
            errors.push(format!(
                "{}: {keyword} is {bound}, length is {length}",
                at(path)
            ));
        }
    }

    fn check_bound(
        &self,
        keyword: &str,
        value: &Value,
        instance: &Value,
        path: &str,
        errors: &mut Vec<String>,
    ) {
        let Some(number) = instance.as_f64() else {
            return;
        };
        let bound = value.as_f64().expect("a numeric bound is a number");
        let violated = match keyword {
            "minimum" => number < bound,
            _ => number > bound,
        };
        if violated {
            errors.push(format!(
                "{}: {keyword} is {bound}, value is {number}",
                at(path)
            ));
        }
    }

    fn check_pattern(&self, value: &Value, instance: &Value, path: &str, errors: &mut Vec<String>) {
        let Value::String(text) = instance else {
            return;
        };
        let pattern = value.as_str().expect("pattern is a string");
        let regex = Regex::new(pattern).expect("pattern is a valid regex");
        if !regex.is_match(text) {
            errors.push(format!("{}: {text:?} does not match /{pattern}/", at(path)));
        }
    }
}

fn at(path: &str) -> &str {
    if path.is_empty() { "(root)" } else { path }
}

fn type_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
