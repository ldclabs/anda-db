use nom::{
    IResult, Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, char},
    combinator::{cut, map, opt, recognize, value},
    error::{ErrorKind, ParseError, context},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, terminated},
};
use nom_language::error::VerboseError;

use super::json::{json_value, parse_number};
use crate::ast::{DotPathVar, Json, KeyValue, Map, Value};

pub use super::json::{quoted_string, ws};

pub type VResult<'a, T> = IResult<&'a str, T, VerboseError<&'a str>>;

/// Parses the contents of a block enclosed in curly braces.
pub fn braced_block<'a, O, F>(
    f: F,
) -> impl Parser<&'a str, Output = O, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    delimited(ws(char('{')), f, ws(char('}')))
}

/// Parses the contents of a block enclosed in parentheses.
pub fn parenthesized_block<'a, O, F>(
    f: F,
) -> impl Parser<&'a str, Output = O, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    delimited(ws(char('(')), f, ws(char(')')))
}

/// Parses a valid identifier (e.g., for variables, types, predicates).
/// An identifier starts with a letter or underscore, followed by any combination of letters, digits, or underscores.
pub fn identifier(input: &str) -> VResult<'_, &str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)
}

/// Parses a KIP variable, like `?my_var`.
pub fn variable(input: &str) -> VResult<'_, String> {
    map(preceded(char('?'), cut(identifier)), |s| s.to_string()).parse(input)
}

/// Parses a dot notation path, like `?var`, `?var.field` or `?var.attributes.key`.
pub fn dot_path_var(input: &str) -> VResult<'_, DotPathVar> {
    let (remaining, (var, path_components)) = context(
        "KIP dot notation path",
        pair(
            preceded(char('?'), cut(identifier)),
            many0(preceded(char('.'), identifier)),
        ),
    )
    .parse(input)?;

    // 验证剩余输入不以点号开头（避免 "?var." 这种情况）
    if remaining.starts_with('.') {
        return Err(nom::Err::Error(
            <VerboseError<&str> as ParseError<&str>>::from_error_kind(remaining, ErrorKind::Verify),
        ));
    }

    Ok((
        remaining,
        DotPathVar {
            var: var.to_string(),
            path: path_components.into_iter().map(|s| s.to_string()).collect(),
        },
    ))
}

/// Parses any KIP value (string, number, boolean, null).
pub fn kip_value(input: &str) -> VResult<'_, Value> {
    alt((
        value(Value::Null, tag_no_case("null")),
        value(Value::Bool(true), tag_no_case("true")),
        value(Value::Bool(false), tag_no_case("false")),
        map(quoted_string, Value::String),
        map(parse_number, Value::Number),
    ))
    .parse(input)
}

/// Parses a key-value pair, like `name: "Aspirin"`.
pub fn key_value_pair(input: &str) -> VResult<'_, KeyValue> {
    map(
        separated_pair(identifier, ws(char(':')), kip_value),
        |(k, v)| KeyValue {
            key: k.to_string(),
            value: v,
        },
    )
    .parse(input)
}

/// Parses a list of key-value pairs inside braces, like `{ key1: val1, key2: val2 }`.
pub fn json_value_map(input: &str) -> VResult<'_, Map<String, Json>> {
    map(
        context(
            "KIP key-value map",
            preceded(
                ws(char('{')),
                cut(terminated(
                    opt(terminated(
                        separated_list1(ws(char(',')), key_json_pair),
                        opt(ws(char(','))), // Allow trailing comma
                    )),
                    ws(char('}')),
                )),
            ),
        ),
        |opt_kvs| opt_kvs.unwrap_or_default().into_iter().collect(),
    )
    .parse(input)
}

fn key_json_pair(input: &str) -> VResult<'_, (String, Json)> {
    context(
        "key-value pair",
        separated_pair(
            alt((quoted_string, map(identifier, |s| s.to_string()))),
            cut(ws(char(':'))),
            cut(json_value()),
        ),
    )
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Number;

    #[test]
    fn test_ws() {
        assert_eq!(
            ws(char::<&str, VerboseError<_>>('a')).parse("  a  "),
            Ok(("", 'a'))
        );
        assert_eq!(
            ws(char::<&str, VerboseError<_>>('a')).parse("a"),
            Ok(("", 'a'))
        );
        assert_eq!(
            ws(char::<&str, VerboseError<_>>('a')).parse("\n\t a \r\n"),
            Ok(("", 'a'))
        );
    }

    #[test]
    fn test_ws_with_comments() {
        // 测试注释后跟换行符
        let input = "// comment\nvalue";
        let result = ws(tag::<&str, &str, VerboseError<_>>("value")).parse(input);
        assert!(result.is_ok());

        // 测试多行注释和空白字符混合
        let input = "  // comment1\n  // comment2\n  value";
        let result = ws(tag::<&str, &str, VerboseError<_>>("value")).parse(input);
        assert!(result.is_ok());

        // 测试注释在末尾
        let input = "value  // comment";
        let result = ws(tag::<&str, &str, VerboseError<_>>("value")).parse(input);
        assert!(result.is_ok());
    }

    #[test]
    fn test_identifier() {
        assert_eq!(identifier("hello"), Ok(("", "hello")));
        assert_eq!(identifier("_private"), Ok(("", "_private")));
        assert_eq!(identifier("var123"), Ok(("", "var123")));
        assert_eq!(identifier("hello_world"), Ok(("", "hello_world")));
        assert!(identifier("123invalid").is_err());
        assert!(identifier("").is_err());
    }

    #[test]
    fn test_variable() {
        assert_eq!(variable("?my_var"), Ok(("", "my_var".to_string())));
        assert_eq!(variable("?_private"), Ok(("", "_private".to_string())));
        assert_eq!(variable("?var123"), Ok(("", "var123".to_string())));
        assert!(variable("my_var").is_err());
        assert!(variable("?").is_err());
    }

    #[test]
    fn test_dot_path_var() {
        // 测试简单变量（无路径组件）
        let result = dot_path_var("?var");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(dot_path.var, "var");
        assert_eq!(dot_path.path, Vec::<String>::new());

        // 测试带一个路径组件的变量
        let result = dot_path_var("?drug.name");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(dot_path.var, "drug");
        assert_eq!(dot_path.path, vec!["name".to_string()]);

        // 测试带多个路径组件的变量
        let result = dot_path_var("?drug.attributes.risk_level");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(dot_path.var, "drug");
        assert_eq!(
            dot_path.path,
            vec!["attributes".to_string(), "risk_level".to_string()]
        );

        // 测试更复杂的路径
        let result = dot_path_var("?entity.metadata.created_by.user_id");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(dot_path.var, "entity");
        assert_eq!(
            dot_path.path,
            vec![
                "metadata".to_string(),
                "created_by".to_string(),
                "user_id".to_string()
            ]
        );

        // 测试带下划线的变量名和路径
        let result = dot_path_var("?my_var._private_field.sub_key");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(dot_path.var, "my_var");
        assert_eq!(
            dot_path.path,
            vec!["_private_field".to_string(), "sub_key".to_string()]
        );

        // 测试带数字的标识符
        let result = dot_path_var("?var123.field456.key789");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, "");
        assert_eq!(dot_path.var, "var123");
        assert_eq!(
            dot_path.path,
            vec!["field456".to_string(), "key789".to_string()]
        );

        // 测试解析停止在非标识符字符处
        let result = dot_path_var("?var.field extra");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, " extra");
        assert_eq!(dot_path.var, "var");
        assert_eq!(dot_path.path, vec!["field".to_string()]);

        // 测试解析停止在特殊字符处
        let result = dot_path_var("?var.field,");
        assert!(result.is_ok());
        let (remaining, dot_path) = result.unwrap();
        assert_eq!(remaining, ",");
        assert_eq!(dot_path.var, "var");
        assert_eq!(dot_path.path, vec!["field".to_string()]);
    }

    #[test]
    fn test_dot_path_var_errors() {
        // 测试缺少问号前缀
        assert!(dot_path_var("var.field").is_err());

        // 测试只有问号
        assert!(dot_path_var("?").is_err());

        // 测试问号后跟无效标识符
        assert!(dot_path_var("?123invalid").is_err());

        // 测试点后没有标识符
        assert!(dot_path_var("?var.").is_err());
        assert!(dot_path_var("?var..").is_err());

        // 测试点后跟无效标识符
        assert!(dot_path_var("?var.123invalid").is_err());

        // 测试空输入
        assert!(dot_path_var("").is_err());

        // 测试连续的点
        assert!(dot_path_var("?var..field").is_err());
    }

    #[test]
    fn test_quoted_string() {
        assert_eq!(quoted_string(r#""hello""#), Ok(("", "hello".to_string())));
        assert_eq!(
            quoted_string(r#""hello world""#),
            Ok(("", "hello world".to_string()))
        );
        assert_eq!(
            quoted_string(r#""with \"quotes\"""#),
            Ok(("", r#"with "quotes""#.to_string()))
        );
        assert_eq!(
            quoted_string(r#""with \\backslash""#),
            Ok(("", r#"with \backslash"#.to_string()))
        );
        assert_eq!(
            quoted_string(r#""with \n newline""#),
            Ok(("", "with \n newline".to_string()))
        );
        assert_eq!(
            quoted_string(r#""with \t tab""#),
            Ok(("", "with \t tab".to_string()))
        );
        assert_eq!(
            quoted_string(r#""with \r return""#),
            Ok(("", "with \r return".to_string()))
        );
        assert_eq!(quoted_string(r#""""#), Ok(("", "".to_string())));
        assert!(quoted_string(r#""unclosed"#).is_err());
    }

    #[test]
    fn test_kip_value() {
        assert_eq!(kip_value("42"), Ok(("", Value::Number(Number::from(42)))));
        assert_eq!(kip_value("-42"), Ok(("", Value::Number(Number::from(-42)))));
        assert_eq!(
            kip_value("0.618"),
            Ok(("", Value::Number(Number::from_f64(0.618f64).unwrap())))
        );
        assert_eq!(
            kip_value(r#""hello""#),
            Ok(("", Value::String("hello".to_string())))
        );
        assert_eq!(kip_value("true"), Ok(("", Value::Bool(true))));
        assert_eq!(kip_value("TRUE"), Ok(("", Value::Bool(true))));
        assert_eq!(kip_value("false"), Ok(("", Value::Bool(false))));
        assert_eq!(kip_value("FALSE"), Ok(("", Value::Bool(false))));
        assert_eq!(kip_value("null"), Ok(("", Value::Null)));
        assert_eq!(kip_value("NULL"), Ok(("", Value::Null)));
    }

    #[test]
    fn test_key_value_pair() {
        let result = key_value_pair(r#"name: "John""#);
        assert!(result.is_ok());
        let (_, kv) = result.unwrap();
        assert_eq!(kv.key, "name");
        assert_eq!(kv.value, Value::String("John".to_string()));

        let result = key_value_pair("age: 25");
        assert!(result.is_ok());
        let (_, kv) = result.unwrap();
        assert_eq!(kv.key, "age");
        assert_eq!(kv.value, Value::Number(Number::from(25)));

        let result = key_value_pair("active: true");
        assert!(result.is_ok());
        let (_, kv) = result.unwrap();
        assert_eq!(kv.key, "active");
        assert_eq!(kv.value, Value::Bool(true));
    }

    #[test]
    fn test_key_value_map() {
        let result = json_value_map(r#"{ name: "John", age: 25 }"#);
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("name"), Some(&Json::String("John".to_string())));
        assert_eq!(map.get("age"), Some(&Json::Number(Number::from(25))));

        let result = json_value_map(r#"{ "name" : "John", "age": 25 }"#);
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("name"), Some(&Json::String("John".to_string())));
        assert_eq!(map.get("age"), Some(&Json::Number(Number::from(25))));

        // Test empty map
        let result = json_value_map("{}");
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 0);

        // Test with trailing comma
        let result = json_value_map(r#"{ name: "John", age: 25, }"#);
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 2);

        // Test with whitespace
        let result = json_value_map(
            r#"{
            name: "John",
            age: 25,
            active: true
        }"#,
        );
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map.get("active"), Some(&Json::Bool(true)));

        // Test single item
        let result = json_value_map(r#"{ name: "John" }"#);
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_braced_block() {
        let mut parser = braced_block(char::<&str, VerboseError<_>>('a'));
        assert_eq!(parser.parse("{ a }"), Ok(("", 'a')));
        assert_eq!(parser.parse("{a}"), Ok(("", 'a')));
        assert_eq!(parser.parse("{\n  a  \n}"), Ok(("", 'a')));
        assert!(parser.parse("{ b }").is_err());
        assert!(parser.parse("{ a").is_err());
        assert!(parser.parse("a }").is_err());
    }

    #[test]
    fn test_parenthesized_block() {
        let mut parser = parenthesized_block(char::<&str, VerboseError<_>>('a'));
        assert_eq!(parser.parse("( a )"), Ok(("", 'a')));
        assert_eq!(parser.parse("(a)"), Ok(("", 'a')));
        assert_eq!(parser.parse("(\n  a  \n)"), Ok(("", 'a')));
        assert!(parser.parse("( b )").is_err());
        assert!(parser.parse("( a").is_err());
        assert!(parser.parse("a )").is_err());
    }

    #[test]
    fn test_complex_nested_values() {
        let result = kip_value(r#""nested \"quotes\" and \n newlines""#);
        assert!(result.is_ok());
        let (_, value) = result.unwrap();
        assert_eq!(
            value,
            Value::String("nested \"quotes\" and \n newlines".to_string())
        );
    }

    #[test]
    fn test_key_value_map_complex() {
        let input = r#"{
            name: "John Doe",
            age: 30,
            height: 5.9,
            active: true,
            score: null,
            negative: -42,
        }"#;

        let result = json_value_map(input);
        assert!(result.is_ok());
        let (_, map) = result.unwrap();
        assert_eq!(map.len(), 6);
        assert_eq!(map.get("name"), Some(&Json::String("John Doe".to_string())));
        assert_eq!(map.get("age"), Some(&Json::Number(Number::from(30))));
        assert_eq!(
            map.get("height"),
            Some(&Json::Number(Number::from_f64(5.9).unwrap()))
        );
        assert_eq!(map.get("active"), Some(&Json::Bool(true)));
        assert_eq!(map.get("score"), Some(&Json::Null));
        assert_eq!(map.get("negative"), Some(&Json::Number(Number::from(-42))));
    }

    #[test]
    fn test_edge_cases() {
        // Test identifier edge cases
        assert_eq!(identifier("a"), Ok(("", "a")));
        assert_eq!(identifier("a1"), Ok(("", "a1")));
        assert_eq!(identifier("_"), Ok(("", "_")));
        assert_eq!(identifier("_1"), Ok(("", "_1")));

        // Test that identifier stops at non-alphanumeric/underscore
        assert_eq!(identifier("hello-world"), Ok(("-world", "hello")));
        assert_eq!(identifier("hello world"), Ok((" world", "hello")));
    }

    #[test]
    fn test_kip_value_precedence() {
        // Test that boolean parsing works correctly
        assert_eq!(kip_value("true"), Ok(("", Value::Bool(true))));
        assert_eq!(kip_value("false"), Ok(("", Value::Bool(false))));
        assert_eq!(kip_value("null"), Ok(("", Value::Null)));

        // Test mixed case
        assert_eq!(kip_value("True"), Ok(("", Value::Bool(true))));
        assert_eq!(kip_value("False"), Ok(("", Value::Bool(false))));
        assert_eq!(kip_value("Null"), Ok(("", Value::Null)));

        // Test that numbers are parsed correctly
        assert_eq!(kip_value("0"), Ok(("", Value::Number(Number::from(0)))));
        assert_eq!(
            kip_value("-0"),
            Ok(("", Value::Number(Number::from_f64(-0.0).unwrap())))
        );
        assert_eq!(
            kip_value("0.0"),
            Ok(("", Value::Number(Number::from_f64(0.0).unwrap())))
        );
    }

    #[test]
    fn test_error_handling() {
        // Test malformed inputs
        assert!(quoted_string("'single quotes'").is_err());
        assert!(quoted_string("\"unclosed").is_err());
        assert!(key_value_pair("key: ").is_err());
        assert!(json_value_map("{ key: }").is_err());
        assert!(json_value_map("{ key value }").is_err());
    }

    #[test]
    fn test_error_messages_with_context() {
        // 测试未闭合字符串的错误信息应包含完整上下文
        let input = r#"{ name: "test, age: 25 }"#;
        let result = json_value_map(input);
        assert!(result.is_err());
        if let Err(nom::Err::Failure(e)) = &result {
            let err_str = format!("{}", e);
            // 验证错误信息包含 JSON string 和 KIP key-value map 的上下文
            assert!(
                err_str.contains("JSON string"),
                "Error should mention JSON string context"
            );
            assert!(
                err_str.contains("KIP key-value map"),
                "Error should mention KIP key-value map context"
            );
        } else {
            panic!("Expected Failure error, got {:?}", result);
        }

        // 测试空值的错误信息
        let input2 = r#"{ key: }"#;
        let result2 = json_value_map(input2);
        assert!(result2.is_err());
        if let Err(nom::Err::Failure(e)) = &result2 {
            let err_str = format!("{}", e);
            assert!(
                err_str.contains("KIP key-value map"),
                "Error should mention KIP key-value map context"
            );
        }
    }
}
