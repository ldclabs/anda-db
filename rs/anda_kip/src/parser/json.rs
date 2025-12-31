// https://github.com/rust-bakery/nom/blob/main/examples/json2.rs

use nom::{
    IResult, Mode, Parser,
    branch::alt,
    bytes::{tag, tag_no_case, take},
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
use nom_language::error::VerboseError;
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
    map_res(recognize_float, Number::from_str).parse(input)
}

pub fn ws<'a, O, F>(f: F) -> impl Parser<&'a str, Output = O, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    delimited(skip_ws_and_comments, f, skip_ws_and_comments)
}

/// Skips whitespace and line comments.
fn skip_ws_and_comments(input: &str) -> IResult<&str, (), VerboseError<&str>> {
    let mut remaining = input;

    loop {
        let start_len = remaining.len();

        // 跳过空白字符
        let trimmed = remaining.trim_start_matches(|c: char| c.is_whitespace());
        remaining = trimmed;

        // 跳过行注释
        if remaining.starts_with("//") {
            if let Some(newline_pos) = remaining.find('\n') {
                remaining = &remaining[newline_pos + 1..];
            } else {
                // 注释到文件末尾
                remaining = "";
            }
        }

        // 如果没有更多内容被跳过，退出循环
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
        "JSON array [ ... ]",
        delimited(
            char('['),
            cut(ws(terminated(
                separated_list0(ws(char(',')), json_value()),
                opt(ws(char(','))),
            ))),
            char(']'),
        ),
    )
}

// An identifier starts with a letter or underscore, followed by any combination of letters, digits, or underscores.
fn identifier<'a>() -> impl Parser<&'a str, Output = &'a str, Error = VerboseError<&'a str>> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))
}

fn object<'a>() -> impl Parser<&'a str, Output = Map<String, Json>, Error = VerboseError<&'a str>> {
    context(
        "JSON object { ... }",
        map(
            delimited(
                char('{'),
                cut(ws(terminated(
                    separated_list0(
                        ws(char(',')),
                        separated_pair(
                            alt((string(), map(identifier(), |s| s.to_string()))),
                            ws(char(':')),
                            json_value(),
                        ),
                    ),
                    opt(ws(char(','))),
                ))),
                char('}'),
            ),
            |key_values| key_values.into_iter().collect(),
        ),
    )
}

fn u16_hex<'a>() -> impl Parser<&'a str, Output = u16, Error = VerboseError<&'a str>> {
    map_res(take(4usize), |s| u16::from_str_radix(s, 16))
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
    Character
}

struct Character;

impl<'a> Parser<&'a str> for Character {
    type Output = char;

    type Error = VerboseError<&'a str>;

    fn process<OM: nom::OutputMode>(
        &mut self,
        input: &'a str,
    ) -> nom::PResult<OM, &'a str, Self::Output, Self::Error> {
        let (input, c): (&str, char) =
            none_of("\"").process::<nom::OutputM<nom::Emit, OM::Error, OM::Incomplete>>(input)?;
        if c == '\\' {
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
            ))
            .process::<OM>(input)
        } else {
            Ok((input, OM::Output::bind(|| c)))
        }
    }
}

struct JsonParser;

impl<'a> Parser<&'a str> for JsonParser {
    type Output = Json;
    type Error = VerboseError<&'a str>;

    fn process<OM: nom::OutputMode>(
        &mut self,
        input: &'a str,
    ) -> nom::PResult<OM, &'a str, Self::Output, Self::Error> {
        let mut parser = alt((
            value(Json::Null, tag_no_case("null")),
            value(Json::Bool(true), tag_no_case("true")),
            value(Json::Bool(false), tag_no_case("false")),
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
}
