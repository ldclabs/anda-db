/// Represents different types of boolean queries that can be parsed from a query string.
/// Supports Term, Or, And, and Not operations for building complex search expressions.
/// Operator precedence: OR < AND < NOT.
///
/// # Examples
///
/// ```
/// use anda_db_tfs::QueryType;
///
/// let query = QueryType::parse("(hello AND world) OR (rust AND NOT java)");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// A simple term query that matches a single word or phrase
    Term(String),

    /// A logical OR query that requires at least one subquery to match
    Or(Vec<Box<QueryType>>),

    /// A logical AND query that requires all subqueries to match
    And(Vec<Box<QueryType>>),

    /// A logical NOT query that negates the result of its subquery
    Not(Box<QueryType>),
}

impl QueryType {
    /// Parses a query string into a QueryType structure.
    ///
    /// This is the main entry point for converting a string query into a structured
    /// representation that can be used for searching.
    ///
    /// # Arguments
    ///
    /// * `query` - A string slice containing the query to parse
    ///
    /// # Returns
    ///
    /// A QueryType representing the parsed query
    ///
    /// # Examples
    ///
    /// ```
    /// use anda_db_tfs::QueryType;
    ///
    /// let query = QueryType::parse("(hello AND world) OR (rust AND NOT java)");
    /// ```
    pub fn parse(query: &str) -> Self {
        let query = query.trim();
        if query.is_empty() {
            return QueryType::Or(vec![]);
        }

        Self::parse_or_expression(query)
    }

    /// Parses an OR expression, which has the lowest precedence in the query grammar.
    ///
    /// # Arguments
    ///
    /// * `query` - A string slice containing the query to parse
    ///
    /// # Returns
    ///
    /// A QueryType representing the parsed OR expression
    fn parse_or_expression(query: &str) -> Self {
        let parts: Vec<&str> = Self::split_top_level(query, " OR ");

        if parts.len() == 1 {
            return Self::parse_and_expression(parts[0]);
        }

        let subqueries: Vec<Box<QueryType>> = parts
            .into_iter()
            .map(|p| Box::new(Self::parse_and_expression(p)))
            .collect();

        QueryType::Or(subqueries)
    }

    /// Parses an AND expression, which has medium precedence in the query grammar.
    ///
    /// # Arguments
    ///
    /// * `query` - A string slice containing the query to parse
    ///
    /// # Returns
    ///
    /// A QueryType representing the parsed AND expression
    fn parse_and_expression(query: &str) -> Self {
        let parts: Vec<&str> = Self::split_top_level(query, " AND ");

        if parts.len() == 1 {
            return Self::parse_not_expression(parts[0]);
        }

        let subqueries: Vec<Box<QueryType>> = parts
            .into_iter()
            .map(|p| Box::new(Self::parse_not_expression(p)))
            .collect();

        QueryType::And(subqueries)
    }

    /// Parses a NOT expression, which has high precedence in the query grammar.
    ///
    /// # Arguments
    ///
    /// * `query` - A string slice containing the query to parse
    ///
    /// # Returns
    ///
    /// A QueryType representing the parsed NOT expression
    fn parse_not_expression(query: &str) -> Self {
        let query = query.trim();

        if let Some(stripped) = query.strip_prefix("NOT ") {
            return QueryType::Not(Box::new(Self::parse_term(stripped)));
        }

        Self::parse_term(query)
    }

    /// Parses a term or parenthesized expression, which has the highest precedence.
    ///
    /// # Arguments
    ///
    /// * `query` - A string slice containing the query to parse
    ///
    /// # Returns
    ///
    /// A QueryType representing the parsed term or parenthesized expression
    fn parse_term(query: &str) -> Self {
        let query = query.trim();

        // Handle parenthesized expressions
        if let Some(stripped) = query.strip_prefix('(') {
            // 处理可能存在的非平衡括号
            if stripped.ends_with(')') && Self::is_balanced_parentheses(query) {
                // 完全平衡的括号表达式
                return Self::parse_or_expression(&stripped[..stripped.len() - 1]);
            } else {
                // 处理不平衡的括号
                // 1. 如果缺少右括号，尝试解析括号内的内容
                return Self::parse_or_expression(stripped);
            }
        } else if let Some(stripped) = query.strip_suffix(')') {
            // 处理只有右括号的情况
            return Self::parse_or_expression(stripped);
        }

        // Handle multiple terms (default to OR relationship)
        let terms: Vec<&str> = query.split_whitespace().collect();
        if terms.len() > 1 {
            let subqueries: Vec<Box<QueryType>> = terms
                .into_iter()
                .map(|t| Box::new(QueryType::Term(t.to_lowercase())))
                .collect();
            return QueryType::Or(subqueries);
        }

        // Handle single term
        if !query.is_empty() {
            return QueryType::Term(query.to_lowercase());
        }

        // Handle empty query
        QueryType::Or(vec![])
    }

    /// Checks if parentheses in a string are balanced.
    ///
    /// # Arguments
    ///
    /// * `s` - A string slice to check for balanced parentheses
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the parentheses are balanced
    fn is_balanced_parentheses(s: &str) -> bool {
        let mut count = 0;

        for c in s.chars() {
            if c == '(' {
                count += 1;
            } else if c == ')' {
                count -= 1;
                if count < 0 {
                    return false;
                }
            }
        }

        count == 0
    }

    /// Splits a string at the top level by a delimiter, ignoring delimiters inside parentheses.
    /// Handles unbalanced parentheses by treating them as part of the text.
    ///
    /// This is a key function that enables proper parsing of nested expressions.
    ///
    /// # Arguments
    ///
    /// * `s` - A string slice to split
    /// * `delimiter` - The delimiter to split by
    ///
    /// # Returns
    ///
    /// A vector of string slices resulting from the split
    fn split_top_level<'a>(s: &'a str, delimiter: &str) -> Vec<&'a str> {
        // Delimiters (" OR ", " AND ") are pure ASCII, so byte-level comparison
        // is correct and inherently avoids UTF-8 char boundary issues.
        debug_assert!(delimiter.is_ascii());

        let mut result = Vec::new();
        let mut start = 0;
        let mut paren_count: u32 = 0;
        let bytes = s.as_bytes();
        let delim_bytes = delimiter.as_bytes();
        let delim_len = delim_bytes.len();
        let mut i = 0;

        while i < bytes.len() {
            match bytes[i] {
                b'(' => {
                    paren_count += 1;
                    i += 1;
                }
                b')' => {
                    paren_count = paren_count.saturating_sub(1);
                    i += 1;
                }
                _ if paren_count == 0
                    && i + delim_len <= bytes.len()
                    && bytes[i..i + delim_len] == *delim_bytes =>
                {
                    // Safety: start and i are always at ASCII boundaries,
                    // which are valid UTF-8 char boundaries.
                    result.push(s[start..i].trim());
                    i += delim_len;
                    start = i;
                }
                _ => {
                    i += 1;
                }
            }
        }

        result.push(s[start..].trim());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests parsing a simple term query
    #[test]
    fn test_simple_term() {
        assert_eq!(
            QueryType::parse("hello"),
            QueryType::Term("hello".to_string())
        );
    }

    /// Tests parsing an AND query
    #[test]
    fn test_and_query() {
        assert_eq!(
            QueryType::parse("hello AND world"),
            QueryType::And(vec![
                Box::new(QueryType::Term("hello".to_string())),
                Box::new(QueryType::Term("world".to_string()))
            ])
        );
    }

    /// Tests parsing an OR query
    #[test]
    fn test_or_query() {
        assert_eq!(
            QueryType::parse("hello OR world"),
            QueryType::Or(vec![
                Box::new(QueryType::Term("hello".to_string())),
                Box::new(QueryType::Term("world".to_string()))
            ])
        );
    }

    /// Tests parsing a NOT query
    #[test]
    fn test_not_query() {
        assert_eq!(
            QueryType::parse("NOT hello"),
            QueryType::Not(Box::new(QueryType::Term("hello".to_string())))
        );
    }

    /// Tests parsing a complex query with nested expressions
    #[test]
    fn test_complex_query() {
        assert_eq!(
            QueryType::parse("(hello AND world) OR (rust AND NOT java)"),
            QueryType::Or(vec![
                Box::new(QueryType::And(vec![
                    Box::new(QueryType::Term("hello".to_string())),
                    Box::new(QueryType::Term("world".to_string()))
                ])),
                Box::new(QueryType::And(vec![
                    Box::new(QueryType::Term("rust".to_string())),
                    Box::new(QueryType::Not(Box::new(QueryType::Term(
                        "java".to_string()
                    ))))
                ]))
            ])
        );
    }

    /// Tests parsing queries with unbalanced parentheses
    #[test]
    fn test_unbalanced_parentheses() {
        // 缺少右括号
        assert_eq!(
            QueryType::parse("(hello AND world"),
            QueryType::And(vec![
                Box::new(QueryType::Term("hello".to_string())),
                Box::new(QueryType::Term("world".to_string()))
            ])
        );

        // 缺少左括号
        assert_eq!(
            QueryType::parse("hello AND world)"),
            QueryType::And(vec![
                Box::new(QueryType::Term("hello".to_string())),
                Box::new(QueryType::Term("world".to_string()))
            ])
        );

        // 嵌套括号不平衡
        assert_eq!(
            QueryType::parse("(hello AND (world OR rust)"),
            QueryType::And(vec![
                Box::new(QueryType::Term("hello".to_string())),
                Box::new(QueryType::Or(vec![
                    Box::new(QueryType::Term("world".to_string())),
                    Box::new(QueryType::Term("rust".to_string()))
                ]))
            ])
        );
    }

    /// Tests that multi-byte UTF-8 characters don't cause panic in split_top_level
    #[test]
    fn test_multibyte_utf8_query() {
        // 纯中文词，不应 panic
        assert_eq!(
            QueryType::parse("巨蟹"),
            QueryType::Term("巨蟹".to_string())
        );

        // 中文词 AND 英文词
        assert_eq!(
            QueryType::parse("巨蟹 AND rust"),
            QueryType::And(vec![
                Box::new(QueryType::Term("巨蟹".to_string())),
                Box::new(QueryType::Term("rust".to_string()))
            ])
        );

        // 中文词 OR 中文词
        assert_eq!(
            QueryType::parse("巨蟹 OR 天蝎"),
            QueryType::Or(vec![
                Box::new(QueryType::Term("巨蟹".to_string())),
                Box::new(QueryType::Term("天蝎".to_string()))
            ])
        );

        // 带括号的中文表达式
        assert_eq!(
            QueryType::parse("(巨蟹 AND 座) OR 天蝎"),
            QueryType::Or(vec![
                Box::new(QueryType::And(vec![
                    Box::new(QueryType::Term("巨蟹".to_string())),
                    Box::new(QueryType::Term("座".to_string()))
                ])),
                Box::new(QueryType::Term("天蝎".to_string()))
            ])
        );

        // NOT + 中文词
        assert_eq!(
            QueryType::parse("NOT 巨蟹"),
            QueryType::Not(Box::new(QueryType::Term("巨蟹".to_string())))
        );

        // 多个中文词（默认 OR 关系）
        assert_eq!(
            QueryType::parse("巨蟹 天蝎 双鱼"),
            QueryType::Or(vec![
                Box::new(QueryType::Term("巨蟹".to_string())),
                Box::new(QueryType::Term("天蝎".to_string())),
                Box::new(QueryType::Term("双鱼".to_string()))
            ])
        );
    }
}
