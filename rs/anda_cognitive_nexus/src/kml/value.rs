//! # Evaluating KML right-hand sides
//!
//! Every value a mutation writes arrives as one of four things: a literal, a
//! `:parameter` bound from the request envelope, a `?handle` naming an element
//! this same transaction creates, or an arithmetic expression over the target's
//! own fields.
//!
//! Parameters are **structurally bound, never string-spliced** (Spec §77.2).
//! The parser already refused to turn `:name` into text, and this module
//! finishes the job by substituting a JSON value into a JSON slot. There is no
//! point at which a parameter's contents could be reparsed as KML, which is the
//! whole reason the protocol has parameters instead of interpolation.

use anda_kip::{
    BoundValue, ElementRef, Json, KipError, Map, MutationValue, Number, Scalar, Term, UpdateExpr,
    UpdateFunction,
};
use std::collections::BTreeMap;

use crate::id::ElementId;
use crate::term::Endpoint;

/// What a mutation may substitute into its values.
pub struct Bindings<'a> {
    /// Request-level parameters.
    pub request: Option<&'a Map<String, Json>>,
    /// Operation-level parameters, which shadow request-level ones.
    pub operation: Option<&'a Map<String, Json>>,
    /// Handles bound by this mutation plan.
    pub handles: &'a BTreeMap<String, ElementId>,
    /// The Schema Environment, for the one thing a right-hand side resolves
    /// symbols for: a Facet named by its local name inside an update
    /// expression, `?m.facets["MnemonicState"].memory_strength`. A read that
    /// did not resolve it would find nothing and silently decay `null`.
    pub env: Option<&'a crate::schema::SchemaEnvironment>,
}

impl Bindings<'_> {
    /// Looks up a `:parameter`.
    ///
    /// Operation-level bindings shadow request-level ones: the narrower scope
    /// is the one the caller wrote closest to the command.
    pub fn param(&self, name: &str) -> Result<Json, KipError> {
        self.operation
            .and_then(|map| map.get(name))
            .or_else(|| self.request.and_then(|map| map.get(name)))
            .cloned()
            .ok_or_else(|| {
                KipError::invalid_request_envelope(format!(
                    "the command uses the parameter :{name}, which the request does not bind"
                ))
            })
    }

    /// Resolves a `?handle` to the element it names.
    pub fn handle(&self, name: &str) -> Result<ElementId, KipError> {
        self.handles.get(name).copied().ok_or_else(|| {
            KipError::invalid_syntax(format!(
                "?{name} is not bound by this mutation; a handle must be declared by a clause in \
                 the same MUTATE block"
            ))
        })
    }

    /// Evaluates a `parameter | literal` slot.
    pub fn scalar(&self, scalar: &Scalar) -> Result<Json, KipError> {
        Ok(match scalar {
            Scalar::Literal(value) => Json::from(value.clone()),
            Scalar::Param(name) => self.param(name)?,
        })
    }

    /// Evaluates a scalar that must be a string.
    pub fn scalar_str(&self, scalar: &Scalar, what: &str) -> Result<String, KipError> {
        match self.scalar(scalar)? {
            Json::String(text) => Ok(text),
            other => Err(KipError::type_mismatch(format!(
                "{what} must be a string, got {other}"
            ))),
        }
    }

    /// Evaluates a scalar that must be a non-negative integer.
    pub fn scalar_u64(&self, scalar: &Scalar, what: &str) -> Result<u64, KipError> {
        match self.scalar(scalar)? {
            Json::Number(n) => n.as_u64().ok_or_else(|| {
                KipError::type_mismatch(format!("{what} must be a non-negative integer, got {n}"))
            }),
            other => Err(KipError::type_mismatch(format!(
                "{what} must be a non-negative integer, got {other}"
            ))),
        }
    }

    /// Evaluates a `data_value`, which may still contain unbound parameters.
    pub fn bound(&self, value: &BoundValue, target: Option<&Json>) -> Result<Json, KipError> {
        Ok(match value {
            BoundValue::Value(literal) => Json::from(literal.clone()),
            BoundValue::Param(name) => self.param(name)?,
            BoundValue::Handle(name) => reference(self.handle(name)?),
            BoundValue::Variable(path) => self.own_field(target, path)?,
            BoundValue::Array(items) => Json::Array(
                items
                    .iter()
                    .map(|item| self.bound(item, target))
                    .collect::<Result<_, _>>()?,
            ),
            BoundValue::Object(fields) => {
                let mut map = Map::new();
                for (key, item) in fields {
                    map.insert(key.clone(), self.bound(item, target)?);
                }
                Json::Object(map)
            }
        })
    }

    /// Evaluates a KML assignment's right-hand side.
    ///
    /// `target` is the element being written, and is the *only* element a
    /// value may read from: the parser rejected references to any other
    /// variable, which is what lets each matched element be updated from its
    /// own row without a join.
    pub fn value(&self, value: &MutationValue, target: Option<&Json>) -> Result<Json, KipError> {
        Ok(match value {
            MutationValue::Value(literal) => Json::from(literal.clone()),
            MutationValue::Param(name) => self.param(name)?,
            MutationValue::Handle(name) => reference(self.handle(name)?),
            MutationValue::Variable(path) => self.own_field(target, path)?,
            MutationValue::Array(items) => Json::Array(
                items
                    .iter()
                    .map(|item| self.bound(item, target))
                    .collect::<Result<_, _>>()?,
            ),
            MutationValue::Object(fields) => {
                let mut map = Map::new();
                for (key, item) in fields {
                    map.insert(key.clone(), self.bound(item, target)?);
                }
                Json::Object(map)
            }
            MutationValue::Expr(expr) => self.expr(expr, target)?,
        })
    }

    /// Evaluates a deterministic update expression (Spec §59).
    ///
    /// Deterministic is the operative word: the registered functions are total
    /// over numbers, so replaying a transaction produces the same state. That
    /// is why there is no general expression language here.
    pub fn expr(&self, expr: &UpdateExpr, target: Option<&Json>) -> Result<Json, KipError> {
        Ok(match expr {
            UpdateExpr::Number(n) => Json::Number(n.clone()),
            UpdateExpr::Param(name) => self.param(name)?,
            UpdateExpr::Variable(path) => self.own_field(target, path)?,
            UpdateExpr::Function { func, args } => {
                if args.len() != func.arity() {
                    return Err(KipError::invalid_syntax(format!(
                        "{func:?} takes {} argument(s), got {}",
                        func.arity(),
                        args.len()
                    )));
                }
                let values: Vec<Json> = args
                    .iter()
                    .map(|arg| self.expr(arg, target))
                    .collect::<Result<_, _>>()?;
                apply_function(*func, &values)?
            }
        })
    }

    /// Resolves a mutation target to the element it names.
    pub fn element_ref(&self, target: &ElementRef) -> Result<ElementId, KipError> {
        match target {
            ElementRef::Handle(name) => self.handle(name),
            ElementRef::Id(id) => id.parse(),
            ElementRef::Param(name) => match self.param(name)? {
                Json::String(id) => id.parse(),
                other => Err(KipError::type_mismatch(format!(
                    "the parameter :{name} must carry an element id string, got {other}"
                ))),
            },
        }
    }

    /// Resolves a tuple endpoint.
    ///
    /// A `Term::Match` — an inline `{field: value}` matcher — is a *query*, and
    /// resolving one needs the pattern matcher that KQL owns. `ENSURE
    /// PROPOSITION` therefore accepts only endpoints that already name
    /// something.
    pub fn term(&self, term: &Term) -> Result<Endpoint, KipError> {
        match term {
            Term::Variable(name) => Ok(Endpoint::Local(self.handle(name)?)),
            Term::Param(name) => endpoint_from_json(&self.param(name)?),
            Term::Literal(value) => endpoint_from_json(&Json::from(value.clone())),
            Term::Match(_) | Term::Proposition(_) => Err(KipError::unsupported_capability(
                "this engine resolves a mutation endpoint only from a handle, a parameter or a \
                 literal; matching an endpoint by pattern needs the KQL solver, which is not \
                 wired into the mutation path yet",
            )),
        }
    }
}

impl Bindings<'_> {
    /// Reads a dot path off the element being written, resolving Facet names
    /// through the environment exactly as a KQL read would.
    fn own_field(
        &self,
        target: Option<&Json>,
        path: &anda_kip::DotPathVar,
    ) -> Result<Json, KipError> {
        let Some(env) = self.env else {
            return read_own_field(target, path);
        };
        let target = target.ok_or_else(|| missing_target(path))?;
        Ok(crate::view::read_path_in(env, target, &path.path))
    }
}

/// The refusal for an expression that reads a target no clause provides.
fn missing_target(path: &anda_kip::DotPathVar) -> KipError {
    KipError::invalid_syntax(format!(
        "{path} reads a field of the element being written, and this clause has no such element \
         to read from"
    ))
}

/// Reads a value out of the element being written, without an environment to
/// resolve Facet names through.
fn read_own_field(target: Option<&Json>, path: &anda_kip::DotPathVar) -> Result<Json, KipError> {
    let target = target.ok_or_else(|| missing_target(path))?;
    let mut cursor = target;
    for step in &path.path {
        let key = match step {
            anda_kip::PathStep::Field(name) => name,
            anda_kip::PathStep::Key(key) => key,
        };
        // A missing member reads as null rather than failing: `COALESCE` exists
        // precisely so a clause can supply a value when one was never set.
        cursor = match cursor.get(key) {
            Some(value) => value,
            None => return Ok(Json::Null),
        };
    }
    Ok(cursor.clone())
}

/// Normalizes a value written into a structural field.
///
/// A structural field holds references, and a reference is persisted as
/// `{"id": "C-3"}`. A `?handle` already arrives in that shape; a `:parameter`
/// carrying the same id arrives as the bare string `"C-3"`. Storing them
/// differently would make the edge written one way traversable and the edge
/// written the other way a literal that no `STRUCTURAL (…)` pattern can follow
/// — a difference nothing in the command hints at.
pub fn structural_value(value: Json) -> Json {
    match value {
        Json::String(text) => match text.parse::<ElementId>() {
            Ok(id) => reference(id),
            Err(_) => Json::String(text),
        },
        other => other,
    }
}

/// The persisted form of a reference to a local element.
pub fn reference(id: ElementId) -> Json {
    let mut map = Map::new();
    map.insert("id".into(), Json::String(id.to_string()));
    Json::Object(map)
}

fn endpoint_from_json(value: &Json) -> Result<Endpoint, KipError> {
    // A bare string that parses as an element id is a reference; anything else
    // is a Literal. This is the one place the two spellings meet, and getting
    // it backwards would turn `"C-1"` the string into a graph edge.
    if let Json::String(text) = value
        && let Ok(id) = text.parse::<ElementId>()
    {
        return Ok(Endpoint::Local(id));
    }
    Endpoint::from_json(value)
}

fn apply_function(func: UpdateFunction, args: &[Json]) -> Result<Json, KipError> {
    let number = |value: &Json, position: usize| -> Result<f64, KipError> {
        value.as_f64().ok_or_else(|| {
            KipError::type_mismatch(format!(
                "{func:?} takes numbers; argument {} is {value}",
                position + 1
            ))
        })
    };
    Ok(match func {
        UpdateFunction::Coalesce => {
            // The only function that is not arithmetic: it picks the first
            // value that exists, which is how a clause supplies a default
            // without reading the row twice.
            if args[0].is_null() {
                args[1].clone()
            } else {
                args[0].clone()
            }
        }
        UpdateFunction::Add => finite(number(&args[0], 0)? + number(&args[1], 1)?)?,
        UpdateFunction::Mul => finite(number(&args[0], 0)? * number(&args[1], 1)?)?,
        UpdateFunction::Clamp => {
            let (value, low, high) = (
                number(&args[0], 0)?,
                number(&args[1], 1)?,
                number(&args[2], 2)?,
            );
            if low > high {
                return Err(KipError::invalid_syntax(format!(
                    "CLAMP was given the empty range [{low}, {high}]"
                )));
            }
            finite(value.clamp(low, high))?
        }
    })
}

/// Rejects a result that is not a valid Core JSON number (Spec §9.4).
fn finite(value: f64) -> Result<Json, KipError> {
    Number::from_f64(value)
        .map(Json::Number)
        .ok_or_else(|| KipError::type_mismatch(format!("{value} is not a finite KIP number")))
}

/// Converts a [`KipValue`] assignment map into plain JSON.
pub fn assignments_to_json(
    bindings: &Bindings<'_>,
    assignments: &[(String, MutationValue)],
    target: Option<&Json>,
) -> Result<Map<String, Json>, KipError> {
    let mut map = Map::new();
    for (name, value) in assignments {
        map.insert(name.clone(), bindings.value(value, target)?);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::{DotPathVar, ElementKind, KipValue, PathStep};
    use serde_json::json;

    fn map(value: Json) -> Map<String, Json> {
        value.as_object().cloned().unwrap()
    }

    fn handles() -> BTreeMap<String, ElementId> {
        BTreeMap::from([("e".to_string(), ElementId::new(ElementKind::Evidence, 7))])
    }

    fn bindings<'a>(
        request: &'a Map<String, Json>,
        operation: &'a Map<String, Json>,
        handles: &'a BTreeMap<String, ElementId>,
    ) -> Bindings<'a> {
        Bindings {
            request: Some(request),
            operation: Some(operation),
            handles,
            env: None,
        }
    }

    #[test]
    fn an_operation_parameter_shadows_a_request_one() {
        let request = map(json!({"who": "request", "only": 1}));
        let operation = map(json!({"who": "operation"}));
        let handles = handles();
        let b = bindings(&request, &operation, &handles);
        assert_eq!(b.param("who").unwrap(), json!("operation"));
        assert_eq!(b.param("only").unwrap(), json!(1));
        // An unbound parameter is an envelope error, not an empty value: a
        // silently-null parameter would write nonsense instead of failing.
        assert_eq!(
            b.param("missing").unwrap_err().name(),
            "InvalidRequestEnvelope"
        );
    }

    #[test]
    fn a_parameter_is_substituted_as_a_value_never_as_text() {
        // Spec §77.2. A parameter carrying something that looks like KML is
        // just a string; there is no stage at which it could be reparsed.
        let request = map(json!({"payload": "ARCHIVE :everything"}));
        let operation = Map::new();
        let handles = handles();
        let b = bindings(&request, &operation, &handles);
        assert_eq!(
            b.value(&MutationValue::Param("payload".into()), None)
                .unwrap(),
            json!("ARCHIVE :everything")
        );

        // Structure survives too, at any depth.
        let nested = MutationValue::Object(vec![(
            "outer".to_string(),
            BoundValue::Array(vec![BoundValue::Param("payload".into())]),
        )]);
        assert_eq!(
            b.value(&nested, None).unwrap(),
            json!({"outer": ["ARCHIVE :everything"]})
        );
    }

    #[test]
    fn a_handle_becomes_an_element_reference() {
        let request = Map::new();
        let operation = Map::new();
        let handles = handles();
        let b = bindings(&request, &operation, &handles);
        assert_eq!(
            b.value(&MutationValue::Handle("e".into()), None).unwrap(),
            json!({"id": "E-7"})
        );
        assert!(
            b.value(&MutationValue::Handle("nope".into()), None)
                .is_err()
        );
    }

    #[test]
    fn update_functions_are_total_and_deterministic() {
        let request = Map::new();
        let operation = Map::new();
        let handles = handles();
        let b = bindings(&request, &operation, &handles);
        let target = json!({"facets": {"MnemonicState": {"memory_strength": 0.9}}});
        let path = DotPathVar {
            var: "x".into(),
            path: vec![
                PathStep::Field("facets".into()),
                PathStep::Key("MnemonicState".into()),
                PathStep::Field("memory_strength".into()),
            ],
        };

        let decay = UpdateExpr::Function {
            func: UpdateFunction::Clamp,
            args: vec![
                UpdateExpr::Function {
                    func: UpdateFunction::Mul,
                    args: vec![
                        UpdateExpr::Variable(path.clone()),
                        UpdateExpr::Number(Number::from_f64(0.5).unwrap()),
                    ],
                },
                UpdateExpr::Number(Number::from_f64(0.0).unwrap()),
                UpdateExpr::Number(Number::from_f64(1.0).unwrap()),
            ],
        };
        assert_eq!(b.expr(&decay, Some(&target)).unwrap(), json!(0.45));

        // A field that was never set reads as null, which is what COALESCE is
        // for — not an error that would make a decay job fail on new rows.
        let missing = DotPathVar {
            var: "x".into(),
            path: vec![PathStep::Field("nothing".into())],
        };
        let defaulted = UpdateExpr::Function {
            func: UpdateFunction::Coalesce,
            args: vec![
                UpdateExpr::Variable(missing),
                UpdateExpr::Number(Number::from(1)),
            ],
        };
        assert_eq!(b.expr(&defaulted, Some(&target)).unwrap(), json!(1));
    }

    #[test]
    fn arithmetic_that_leaves_the_number_line_is_refused() {
        let request = map(json!({"huge": 1e308}));
        let operation = Map::new();
        let handles = handles();
        let b = bindings(&request, &operation, &handles);
        let overflow = UpdateExpr::Function {
            func: UpdateFunction::Mul,
            args: vec![
                UpdateExpr::Param("huge".into()),
                UpdateExpr::Number(Number::from(1000)),
            ],
        };
        let err = b.expr(&overflow, None).unwrap_err();
        assert_eq!(err.name(), "TypeMismatch");

        let empty_range = UpdateExpr::Function {
            func: UpdateFunction::Clamp,
            args: vec![
                UpdateExpr::Number(Number::from(5)),
                UpdateExpr::Number(Number::from(10)),
                UpdateExpr::Number(Number::from(1)),
            ],
        };
        assert!(b.expr(&empty_range, None).is_err());
    }

    #[test]
    fn an_endpoint_tells_an_id_apart_from_a_string() {
        let request = map(json!({"who": "C-3", "what": "hello"}));
        let operation = Map::new();
        let handles = handles();
        let b = bindings(&request, &operation, &handles);

        assert_eq!(
            b.term(&Term::Param("who".into())).unwrap(),
            Endpoint::Local(ElementId::new(ElementKind::Concept, 3))
        );
        // Anything that is not an element id is a Literal, so a Proposition
        // object of `"hello"` is a value rather than a dangling edge.
        assert!(matches!(
            b.term(&Term::Param("what".into())).unwrap(),
            Endpoint::Literal(_)
        ));
        assert!(matches!(
            b.term(&Term::Literal(KipValue::Number(Number::from(3))))
                .unwrap(),
            Endpoint::Literal(_)
        ));
    }

    #[test]
    fn a_pattern_endpoint_is_refused_rather_than_guessed() {
        let request = Map::new();
        let operation = Map::new();
        let handles = handles();
        let b = bindings(&request, &operation, &handles);
        let err = b.term(&Term::Match(Default::default())).unwrap_err();
        assert_eq!(err.name(), "UnsupportedCapability");
    }
}
