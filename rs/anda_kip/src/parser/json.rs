// https://github.com/rust-bakery/nom/blob/main/examples/json2.rs

use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take},
    character::{
        anychar,
        complete::{alpha1, alphanumeric1, char, none_of},
    },
    combinator::{cut, map, map_opt, map_res, opt, recognize, value, verify},
    error::context,
    multi::{fold, many0, separated_list0},
    number::complete::recognize_float,
    sequence::{delimited, pair, preceded, separated_pair, terminated},
};
use nom_language::error::{VerboseError, VerboseErrorKind};
use std::collections::HashSet;
use std::str::FromStr;

use crate::{Json, Map, Number};

/// Parse a non-standard JSON:
/// - Allow identifier as map key (starts with a letter or underscore, followed by any combination of letters, digits, or underscores)
/// - Allow line comment (starting with //).
/// - Allow trailing comma
pub fn json_value<'a>() -> impl Parser<&'a str, Output = Json, Error = VerboseError<&'a str>> {
    JsonParser
}

/// Parses a double-quoted string, handling escaped quotes.
pub fn quoted_string(input: &str) -> IResult<&str, String, VerboseError<&str>> {
    string().parse(input)
}

pub fn parse_number(input: &str) -> IResult<&str, Number, VerboseError<&str>> {
    map_res(recognize_float, |literal: &str| {
        let number = Number::from_str(literal).map_err(|err| err.to_string())?;
        if !is_integer_literal(literal) || number.is_i64() || number.is_u64() {
            return Ok(number);
        }

        // `Number::from_str` (serde_json without `arbitrary_precision`) converts
        // an out-of-range integer literal to `f64`, storing a *different* value
        // than the one written — `18446744073709551617` becomes
        // `1.8446744073709552e19` and an EXPORT capsule no longer round-trips.
        // Recover the exact value when it still fits (this is also what turns
        // `-0` into the integer `0` instead of the float `-0.0`), otherwise
        // reject, matching how an overflowing float literal is already handled.
        literal
            .parse::<i64>()
            .map(Number::from)
            .or_else(|_| literal.parse::<u64>().map(Number::from))
            .map_err(|_| {
                format!(
                    "integer literal {literal} is out of range: \
                     KIP integers must be representable as i64 or u64"
                )
            })
    })
    .parse(input)
}

/// True when a `recognize_float` literal has no fraction and no exponent, i.e.
/// the author wrote an integer and expects an integer back.
fn is_integer_literal(literal: &str) -> bool {
    !literal.contains(['.', 'e', 'E'])
}

pub fn ws<'a, O, F>(f: F) -> impl Parser<&'a str, Output = O, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    delimited(skip_ws_and_comments, f, skip_ws_and_comments)
}

/// Skips whitespace and line comments.
pub(super) fn skip_ws_and_comments(input: &str) -> IResult<&str, (), VerboseError<&str>> {
    let mut remaining = input;

    loop {
        let start_len = remaining.len();

        // Skip whitespace characters
        let trimmed = remaining.trim_start_matches(|c: char| c.is_whitespace());
        remaining = trimmed;

        // Skip line comments
        if remaining.starts_with("//") {
            if let Some(newline_pos) = remaining.find('\n') {
                remaining = &remaining[newline_pos + 1..];
            } else {
                // Comment extends to end of file
                remaining = "";
            }
        }

        // If nothing was skipped, exit loop
        if remaining.len() == start_len {
            break;
        }
    }

    Ok((remaining, ()))
}

fn string<'a>() -> impl Parser<&'a str, Output = String, Error = VerboseError<&'a str>> {
    context(
        "JSON string \"...\"",
        preceded(
            char('"'),
            cut(terminated(
                fold(0.., character(), String::new, |mut string, c| {
                    string.push(c);
                    string
                }),
                char('"'),
            )),
        ),
    )
}

// It is not a standard JSON:
// - Allow trailing comma
fn array<'a>() -> impl Parser<&'a str, Output = Vec<Json>, Error = VerboseError<&'a str>> {
    context(
        "JSON array [value, ...]",
        delimited(
            char('['),
            cut(ws(terminated(
                separated_list0(ws(char(',')), json_value()),
                opt(ws(char(','))),
            ))),
            cut(char(']')),
        ),
    )
}

// An identifier starts with a letter or underscore, followed by any combination of letters, digits, or underscores.
// Uses the `complete` tag so a trailing identifier at end of input parses
// instead of returning `Incomplete` (KIP inputs are always complete strings).
pub(super) fn identifier<'a>()
-> impl Parser<&'a str, Output = &'a str, Error = VerboseError<&'a str>> {
    recognize(pair(
        alt((alpha1, nom::bytes::complete::tag("_"))),
        many0(alt((alphanumeric1, nom::bytes::complete::tag("_")))),
    ))
}

/// An object key paired with the input position where it starts, so that
/// duplicate-key errors can point at the offending key instead of at the
/// start of the enclosing object.
pub(super) type SpannedKey<'a> = (&'a str, String);

/// Wraps a key parser so that it also captures the input slice at which the
/// key starts. Used with [`ensure_unique_keys`] to anchor duplicate-key
/// errors at the duplicated key itself.
pub(super) fn spanned<'a, O, F>(
    mut f: F,
) -> impl Parser<&'a str, Output = (&'a str, O), Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    move |input: &'a str| {
        let (rest, value) = f.parse(input)?;
        Ok((rest, (input, value)))
    }
}

/// Rejects duplicate keys in an object literal.
///
/// In an LLM-facing protocol, a duplicate key is almost always a generation
/// error; silently keeping the last value would mask it, so parsing fails
/// (`nom::Err::Failure` anchored at the first duplicated key occurrence).
pub(super) fn ensure_unique_keys<'a, V>(
    entries: &[(SpannedKey<'a>, V)],
) -> Result<(), nom::Err<VerboseError<&'a str>>> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(entries.len());
    for ((position, key), _) in entries {
        if !seen.insert(key.as_str()) {
            return Err(nom::Err::Failure(VerboseError {
                errors: vec![(
                    *position,
                    VerboseErrorKind::Context("duplicate key in object (keys must be unique)"),
                )],
            }));
        }
    }
    Ok(())
}

fn object<'a>() -> impl Parser<&'a str, Output = Map<String, Json>, Error = VerboseError<&'a str>> {
    context("JSON object { key: value, ... }", |input: &'a str| {
        let (remaining, key_values) = delimited(
            char('{'),
            cut(ws(terminated(
                separated_list0(
                    ws(char(',')),
                    context(
                        "JSON key-value pair: key: value",
                        separated_pair(
                            spanned(alt((string(), map(identifier(), |s| s.to_string())))),
                            cut(ws(char(':'))),
                            cut(json_value()),
                        ),
                    ),
                ),
                opt(ws(char(','))),
            ))),
            cut(char('}')),
        )
        .parse(input)?;
        ensure_unique_keys(&key_values)?;
        Ok((
            remaining,
            key_values.into_iter().map(|((_, k), v)| (k, v)).collect(),
        ))
    })
}

/// Parses the four-hex-digit payload of a `\uXXXX` escape.
///
/// The digits are verified to be ASCII hex before conversion: `u16::from_str_radix`
/// on its own accepts a leading sign, so `"\u+041"` used to decode to `A`.
/// `take`/`tag` here are the `complete` variants for the same reason as
/// [`identifier`]: a truncated escape must be a located parse error, not
/// `Incomplete` (which `format_nom_error` reports with no line or column).
fn u16_hex<'a>() -> impl Parser<&'a str, Output = u16, Error = VerboseError<&'a str>> {
    map_res(
        verify(take(4usize), |s: &str| {
            s.chars().all(|c| c.is_ascii_hexdigit())
        }),
        |s: &str| u16::from_str_radix(s, 16),
    )
}

fn unicode_escape<'a>() -> impl Parser<&'a str, Output = char, Error = VerboseError<&'a str>> {
    map_opt(
        alt((
            // Not a surrogate
            map(
                verify(u16_hex(), |cp| !(0xD800..0xE000).contains(cp)),
                |cp| cp as u32,
            ),
            // See https://en.wikipedia.org/wiki/UTF-16#Code_points_from_U+010000_to_U+10FFFF for details
            map(
                verify(
                    separated_pair(u16_hex(), tag("\\u"), u16_hex()),
                    |(high, low)| (0xD800..0xDC00).contains(high) && (0xDC00..0xE000).contains(low),
                ),
                |(high, low)| {
                    let high_ten = (high as u32) - 0xD800;
                    let low_ten = (low as u32) - 0xDC00;
                    (high_ten << 10) + low_ten + 0x10000
                },
            ),
        )),
        // Could probably be replaced with .unwrap() or _unchecked due to the verify checks
        std::char::from_u32,
    )
}

pub fn character<'a>() -> impl Parser<&'a str, Output = char, Error = VerboseError<&'a str>> {
    context(
        "JSON string character",
        alt((
            preceded(
                char('\\'),
                alt((
                    map_res(anychar, |c| {
                        Ok(match c {
                            '"' | '\\' | '/' => c,
                            'b' => '\x08',
                            'f' => '\x0C',
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            _ => return Err(()),
                        })
                    }),
                    preceded(char('u'), unicode_escape()),
                )),
            ),
            verify(none_of("\"\\"), |c: &char| *c >= '\u{20}'),
        )),
    )
}

struct JsonParser;

impl<'a> Parser<&'a str> for JsonParser {
    type Output = Json;
    type Error = VerboseError<&'a str>;

    fn process<OM: nom::OutputMode>(
        &mut self,
        input: &'a str,
    ) -> nom::PResult<OM, &'a str, Self::Output, Self::Error> {
        // The KIP protocol is case-sensitive (§2.8.2), so `TRUE` / `NULL` /
        // `FaLsE` are not JSON literals; they are rejected instead of being
        // silently normalized.
        let mut parser = alt((
            value(Json::Null, tag("null")),
            value(Json::Bool(true), tag("true")),
            value(Json::Bool(false), tag("false")),
            map(string(), Json::String),
            map(parse_number, Json::Number),
            map(array(), Json::Array),
            map(object(), Json::Object),
        ));

        parser.process::<OM>(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_non_standard_json() {
        let input = r#"
        {
            description: "Defines a class or category of Concept Nodes. It acts as a template for creating new concept instances. Every concept node in the graph must have a 'type' that points to a concept of this type.",
            display_hint: "📦",
            "instance_schema": { // line comments
                "description": {
                    type: "string",
                    is_required: true,
                    description: "A human-readable explanation of what this concept type represents."
                },
                "display_hint": {
                    type: "string",
                    is_required: false,
                    description: "A suggested icon or visual cue for user interfaces (e.g., an emoji or icon name)."
                },
                "instance_schema": {
                    type: "object",
                    is_required: false,
                    description: "A recommended schema defining the common and core attributes for instances of this concept type. It serves as a 'best practice' guideline for knowledge creation, not a rigid constraint. Keys are attribute names, values are objects defining 'type', 'is_required', and 'description'. Instances SHOULD include required attributes but MAY also include any other attribute not defined in this schema, allowing for knowledge to emerge and evolve freely."
                },
                "key_instances": {
                    type: "array",
                    item_type: "string",
                    is_required: false,
                    description: "A list of names of the most important or representative instances of this type, to help LLMs ground their queries.",
                },
            },
            key_instances: [ "$ConceptType", "$PropositionType", "Domain", ],
        }
        "#;

        let result = json_value().parse(input.trim()).unwrap();
        println!("{:?}", result);
    }

    #[test]
    fn test_duplicate_object_keys_rejected() {
        // Duplicate keys are almost always an LLM generation error; failing
        // loudly beats silently keeping the last value.
        assert!(json_value().parse(r#"{ a: 1, a: 2 }"#).is_err());
        assert!(json_value().parse(r#"{ "a": 1, a: 2 }"#).is_err());
        // Nested objects are checked as well.
        assert!(json_value().parse(r#"{ a: { b: 1, b: 2 } }"#).is_err());
        // Same key at different nesting levels is fine.
        assert!(
            json_value()
                .parse(r#"{ a: { a: 1 }, b: { a: 2 } }"#)
                .is_ok()
        );
    }

    /// Extracts the deepest duplicate-key error position from a parse error.
    fn duplicate_key_position(err: nom::Err<VerboseError<&str>>) -> &str {
        match err {
            nom::Err::Failure(ve) => {
                let (position, kind) = ve.errors.into_iter().next().expect("non-empty errors");
                assert!(
                    matches!(&kind, VerboseErrorKind::Context(ctx) if ctx.contains("duplicate key")),
                    "expected a duplicate-key context, got {kind:?}"
                );
                position
            }
            other => panic!("expected Failure, got {other:?}"),
        }
    }

    #[test]
    fn test_duplicate_object_key_error_points_at_the_duplicate_key() {
        // The error is anchored at the first *duplicated* occurrence of the
        // key, not at the start of the object.
        let err = json_value().parse(r#"{ a: 1, a: 2 }"#).unwrap_err();
        assert!(duplicate_key_position(err).starts_with("a: 2"));

        // Quoted and identifier spellings of the same key collide; the
        // position covers the duplicate's own spelling.
        let err = json_value().parse(r#"{ "a": 1, a: 2 }"#).unwrap_err();
        assert!(duplicate_key_position(err).starts_with("a: 2"));

        // Nested objects anchor at the nested duplicate.
        let err = json_value().parse(r#"{ a: { b: 1, b: 2 } }"#).unwrap_err();
        assert!(duplicate_key_position(err).starts_with("b: 2"));

        // With three occurrences, the second one (the first duplicate) is
        // reported.
        let err = json_value().parse(r#"{ k: 1, k: 2, k: 3 }"#).unwrap_err();
        assert!(duplicate_key_position(err).starts_with("k: 2,"));
    }

    #[test]
    fn test_unicode_escape_requires_four_hex_digits() {
        // `u16::from_str_radix` accepts a leading `+`, so `\u+041` used to
        // decode to "A"; JSON requires exactly four hex DIGITS.
        assert_eq!(crate::unquote_str(r#""\u+041""#), None);
        assert_eq!(crate::unquote_str(r#""\u-041""#), None);
        assert_eq!(crate::unquote_str(r#""\u 041""#), None);
        // Valid escapes (including surrogate pairs) still decode.
        assert_eq!(crate::unquote_str(r#""A""#), Some("A".to_string()));
        assert_eq!(crate::unquote_str(r#""😀""#), Some("😀".to_string()));
    }

    #[test]
    fn test_truncated_unicode_escape_reports_a_location() {
        // The streaming `take`/`tag` combinators returned `Incomplete`, which
        // `format_nom_error` reports without line, column, or context — a total
        // loss of location for a protocol whose errors exist so an LLM can
        // self-correct.
        let err = crate::parse_json(r#""\u12""#).unwrap_err();
        let msg = &err.message;
        assert!(
            !msg.contains("Parse incomplete"),
            "truncated escape must not report as incomplete: {msg}"
        );
        assert!(
            msg.contains("line 1, column"),
            "truncated escape must report a location: {msg}"
        );
    }

    #[test]
    fn test_out_of_range_integer_literal_is_rejected() {
        // serde_json (without `arbitrary_precision`) degrades an out-of-range
        // integer to f64, storing 1.8446744073709552e19 — a DIFFERENT number —
        // and re-serializing it in exponent form, so EXPORT no longer
        // round-trips the literal.
        assert!(crate::parse_json("18446744073709551617").is_err());
        assert!(crate::parse_json("-9223372036854775809").is_err());

        // In a statement the error is anchored at the offending literal.
        let err = crate::parse_kml(
            r#"CREATE CONCEPT ?c { TYPE "T" SET ATTRIBUTES { n: 18446744073709551617 } }"#,
        )
        .unwrap_err();
        assert!(
            err.message.contains("line 1, column 50"),
            "error should point at the literal: {}",
            err.message
        );

        // The i64/u64 boundaries themselves still parse exactly.
        assert_eq!(
            crate::parse_json("18446744073709551615").unwrap(),
            Json::Number(Number::from(u64::MAX))
        );
        assert_eq!(
            crate::parse_json("-9223372036854775808").unwrap(),
            Json::Number(Number::from(i64::MIN))
        );
        // Floats are unaffected: an out-of-range float was already rejected,
        // an in-range one still parses.
        assert!(crate::parse_json("1e400").is_err());
        assert_eq!(
            crate::parse_json("1.8446744073709552e19").unwrap(),
            Json::Number(Number::from_f64(1.8446744073709552e19).unwrap())
        );
        // `-0` has no fraction or exponent, so it is the integer 0.
        assert_eq!(
            crate::parse_json("-0").unwrap(),
            Json::Number(Number::from(0))
        );
    }

    #[test]
    fn test_json_literals_are_case_sensitive() {
        // "The KIP protocol is case-sensitive" (§2.8.2).
        assert!(crate::parse_json("TRUE").is_err());
        assert!(crate::parse_json("FaLsE").is_err());
        assert!(crate::parse_json("NULL").is_err());
        assert_eq!(crate::parse_json("true").unwrap(), Json::Bool(true));
        assert_eq!(crate::parse_json("false").unwrap(), Json::Bool(false));
        assert_eq!(crate::parse_json("null").unwrap(), Json::Null);
    }
}
