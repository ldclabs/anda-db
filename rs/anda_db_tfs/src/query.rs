/// Represents different types of boolean queries that can be parsed from a query string.
/// Supports Term, Or, And, and Not operations for building complex search expressions.
/// Operator precedence: OR < AND < NOT.
///
/// # Grammar (informal)
///
/// ```text
/// expr    := or_expr
/// or_expr := and_expr ( " OR " and_expr )*
/// and_expr := not_expr ( " AND " not_expr )*
/// not_expr := "NOT " not_expr | term
/// term    := chunk ( whitespace chunk )*
/// chunk   := "(" or_expr ")" | word
/// ```
///
/// Whitespace-separated chunks at the `term` level default to an implicit
/// `OR` between them, matching the behaviour of [`BM25Index::search`]:
/// `rust (async AND tokio)` is `rust OR (async AND tokio)`.
///
/// The operators are case-sensitive and must stand between spaces (`NOT`
/// followed by a space): `a and b` is three search words. Each `NOT` nests
/// one negation, so `NOT NOT a` parses as `Not(Not(a))`.
///
/// The parser is intentionally lenient: unbalanced parentheses are treated as
/// part of the surrounding text so that user input never causes a parse error.
/// An empty group is an empty `OR`, which matches nothing — inside an `AND`
/// (`a AND ()`) it empties the whole conjunction.
///
/// # Examples
///
/// ```
/// use anda_db_tfs::QueryType;
///
/// let query = QueryType::parse("(hello AND world) OR (rust AND NOT java)");
/// ```
///
/// [`BM25Index::search`]: crate::BM25Index::search
#[derive(Debug, Clone, PartialEq)]
pub enum QueryType {
    /// A simple term query that matches a single word or phrase
    Term(String),

    /// A logical OR query that requires at least one subquery to match
    Or(Vec<QueryType>),

    /// A logical AND query that requires all subqueries to match
    And(Vec<QueryType>),

    /// A logical NOT query that negates the result of its subquery
    Not(Box<QueryType>),
}

const MAX_LOGICAL_QUERY_LEN: usize = 8 * 1024;
const MAX_LOGICAL_QUERY_DEPTH: usize = 64;
const MAX_LOGICAL_QUERY_NODES: usize = 1_024;
const MAX_LOGICAL_QUERY_BRANCHES: usize = 512;

impl QueryType {
    /// Parses a query string into a QueryType structure.
    ///
    /// This is the main entry point for converting a string query into a structured
    /// representation that can be used for searching.
    ///
    /// Parsing is total and stack-safe: parenthesis nesting deeper than the
    /// internal budget (64 levels) degrades leniently to plain terms instead
    /// of recursing further. Use [`QueryType::try_parse`] to reject oversized
    /// or overly complex input instead of degrading.
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

        Self::parse_or_expression(query, 0, &mut false)
    }

    /// Parses a query string after applying resource-exhaustion guards.
    pub fn try_parse(query: &str) -> Result<Self, String> {
        validate_query_input(query)?;
        let mut budget_exhausted = false;
        let query = Self::parse_or_expression(query.trim(), 0, &mut budget_exhausted);
        if budget_exhausted {
            return Err("logical query combined nesting exceeds parser depth budget".into());
        }
        query.validate_complexity()?;
        Ok(query)
    }

    fn validate_complexity(&self) -> Result<(), String> {
        let mut stats = QueryStats::default();
        validate_ast(self, 0, &mut stats)
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
    fn parse_or_expression(query: &str, depth: usize, budget_exhausted: &mut bool) -> Self {
        let parts: Vec<&str> = Self::split_top_level(query, " OR ");

        if parts.len() == 1 {
            return Self::parse_and_expression(parts[0], depth, budget_exhausted);
        }

        let subqueries: Vec<QueryType> = parts
            .into_iter()
            .map(|p| Self::parse_and_expression(p, depth, budget_exhausted))
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
    fn parse_and_expression(query: &str, depth: usize, budget_exhausted: &mut bool) -> Self {
        let parts: Vec<&str> = Self::split_top_level(query, " AND ");

        if parts.len() == 1 {
            return Self::parse_not_expression(parts[0], depth, budget_exhausted);
        }

        let subqueries: Vec<QueryType> = parts
            .into_iter()
            .map(|p| Self::parse_not_expression(p, depth, budget_exhausted))
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
    fn parse_not_expression(query: &str, depth: usize, budget_exhausted: &mut bool) -> Self {
        let mut rest = query.trim();

        // Every `NOT` nests one more level. Counting them iteratively and
        // stopping at the nesting budget keeps the parser stack-safe and the
        // resulting tree shallow enough to execute (and drop) recursively;
        // past the budget the remaining `NOT`s degrade to plain words, like
        // parentheses do in `parse_term`.
        let mut negations = 0usize;
        while depth + negations < MAX_LOGICAL_QUERY_DEPTH
            && let Some(stripped) = rest.strip_prefix("NOT ")
        {
            negations += 1;
            rest = stripped.trim_start();
        }

        if rest.starts_with("NOT ") {
            *budget_exhausted = true;
        }
        let mut expr = Self::parse_term(rest, depth + negations, budget_exhausted);
        for _ in 0..negations {
            expr = QueryType::Not(Box::new(expr));
        }
        expr
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
    fn parse_term(query: &str, depth: usize, budget_exhausted: &mut bool) -> Self {
        let query = query.trim();

        // A group next to other words (`(a AND b) c`) is an implicit OR of
        // its chunks. Stripping the outer characters of the whole string
        // instead would glue the group's operators to the neighbouring words.
        let chunks = Self::split_top_level_whitespace(query);
        if chunks.len() > 1 {
            return QueryType::Or(
                chunks
                    .into_iter()
                    .map(|chunk| Self::parse_term(chunk, depth, budget_exhausted))
                    .collect(),
            );
        }

        if depth >= MAX_LOGICAL_QUERY_DEPTH && (query.starts_with('(') || query.ends_with(')')) {
            *budget_exhausted = true;
        }
        // Handle parenthesized expressions.
        //
        // The parenthesis handling below recurses back into
        // `parse_or_expression`, so an adversarial run of parentheses could
        // otherwise grow the call stack linearly with the input. The public
        // `parse` entry point has no input-size guard (only `try_parse`
        // validates), therefore recursion is bounded here: once the nesting
        // budget is exhausted the rest of the input degrades to plain terms
        // instead of overflowing the stack (an abort that cannot be caught).
        if depth < MAX_LOGICAL_QUERY_DEPTH {
            if let Some(stripped) = query.strip_prefix('(') {
                // 处理可能存在的非平衡括号
                if stripped.ends_with(')') && Self::is_balanced_parentheses(query) {
                    // 完全平衡的括号表达式
                    return Self::parse_or_expression(
                        &stripped[..stripped.len() - 1],
                        depth + 1,
                        budget_exhausted,
                    );
                } else {
                    // 处理不平衡的括号
                    // 1. 如果缺少右括号，尝试解析括号内的内容
                    return Self::parse_or_expression(stripped, depth + 1, budget_exhausted);
                }
            } else if query.ends_with(')') {
                // 处理只有右括号的情况。Strip ALL contiguous trailing ')' at
                // once: stripping one per recursion needs O(n) stack (and
                // O(n²) rescans) for a `")"` flood, which overflowed the
                // stack before this guard existed.
                return Self::parse_or_expression(
                    query.trim_end_matches(')'),
                    depth + 1,
                    budget_exhausted,
                );
            }
        }

        // Handle multiple terms (default to OR relationship)
        let terms: Vec<&str> = query.split_whitespace().collect();
        if terms.len() > 1 {
            let subqueries: Vec<QueryType> = terms
                .into_iter()
                .map(|t| QueryType::Term(t.to_owned()))
                .collect();
            return QueryType::Or(subqueries);
        }

        // Handle single term
        if !query.is_empty() {
            return QueryType::Term(query.to_owned());
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

    /// Splits at whitespace outside parentheses. An unclosed `(` keeps the
    /// rest of the input in one chunk; an unmatched `)` is ordinary text.
    fn split_top_level_whitespace(s: &str) -> Vec<&str> {
        let mut chunks = Vec::new();
        let mut start = None;
        let mut paren_count: u32 = 0;
        for (i, ch) in s.char_indices() {
            match ch {
                '(' => paren_count += 1,
                ')' => paren_count = paren_count.saturating_sub(1),
                _ if paren_count == 0 && ch.is_whitespace() => {
                    if let Some(begin) = start.take() {
                        chunks.push(&s[begin..i]);
                    }
                    continue;
                }
                _ => {}
            }
            start.get_or_insert(i);
        }
        if let Some(begin) = start {
            chunks.push(&s[begin..]);
        }
        chunks
    }
}

#[derive(Default)]
struct QueryStats {
    nodes: usize,
    branches: usize,
}

fn validate_query_input(query: &str) -> Result<(), String> {
    if query.len() > MAX_LOGICAL_QUERY_LEN {
        return Err(format!(
            "logical query length {} exceeds maximum {MAX_LOGICAL_QUERY_LEN}",
            query.len()
        ));
    }

    // Count both directions: a flood of ')' (or other unmatched closing
    // parentheses) previously bypassed this guard entirely because only '('
    // contributed to the depth, while each unmatched ')' still costs the
    // parser a recursion step.
    let mut depth = 0usize;
    let mut max_depth = 0usize;
    let mut unmatched_close = 0usize;
    for ch in query.chars() {
        match ch {
            '(' => {
                depth = depth.saturating_add(1);
                max_depth = max_depth.max(depth);
                if max_depth > MAX_LOGICAL_QUERY_DEPTH {
                    return Err(format!(
                        "logical query parenthesis depth exceeds maximum {MAX_LOGICAL_QUERY_DEPTH}"
                    ));
                }
            }
            ')' => {
                if depth == 0 {
                    unmatched_close += 1;
                    if unmatched_close > MAX_LOGICAL_QUERY_DEPTH {
                        return Err(format!(
                            "logical query unmatched closing parenthesis count exceeds maximum {MAX_LOGICAL_QUERY_DEPTH}"
                        ));
                    }
                } else {
                    depth -= 1;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn validate_ast(query: &QueryType, depth: usize, stats: &mut QueryStats) -> Result<(), String> {
    if depth > MAX_LOGICAL_QUERY_DEPTH {
        return Err(format!(
            "logical query AST depth exceeds maximum {MAX_LOGICAL_QUERY_DEPTH}"
        ));
    }

    stats.nodes = stats.nodes.saturating_add(1);
    if stats.nodes > MAX_LOGICAL_QUERY_NODES {
        return Err(format!(
            "logical query AST node count exceeds maximum {MAX_LOGICAL_QUERY_NODES}"
        ));
    }

    match query {
        QueryType::Term(_) => Ok(()),
        QueryType::Not(query) => validate_ast(query, depth + 1, stats),
        QueryType::Or(queries) | QueryType::And(queries) => {
            stats.branches = stats.branches.saturating_add(queries.len());
            if stats.branches > MAX_LOGICAL_QUERY_BRANCHES {
                return Err(format!(
                    "logical query AST branch count exceeds maximum {MAX_LOGICAL_QUERY_BRANCHES}"
                ));
            }
            for query in queries {
                validate_ast(query, depth + 1, stats)?;
            }
            Ok(())
        }
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
                QueryType::Term("hello".to_string()),
                QueryType::Term("world".to_string())
            ])
        );
    }

    /// Tests parsing an OR query
    #[test]
    fn test_or_query() {
        assert_eq!(
            QueryType::parse("hello OR world"),
            QueryType::Or(vec![
                QueryType::Term("hello".to_string()),
                QueryType::Term("world".to_string())
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
                QueryType::And(vec![
                    QueryType::Term("hello".to_string()),
                    QueryType::Term("world".to_string())
                ]),
                QueryType::And(vec![
                    QueryType::Term("rust".to_string()),
                    QueryType::Not(Box::new(QueryType::Term("java".to_string())))
                ])
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
                QueryType::Term("hello".to_string()),
                QueryType::Term("world".to_string())
            ])
        );

        // 缺少左括号
        assert_eq!(
            QueryType::parse("hello AND world)"),
            QueryType::And(vec![
                QueryType::Term("hello".to_string()),
                QueryType::Term("world".to_string())
            ])
        );

        // 嵌套括号不平衡
        assert_eq!(
            QueryType::parse("(hello AND (world OR rust)"),
            QueryType::And(vec![
                QueryType::Term("hello".to_string()),
                QueryType::Or(vec![
                    QueryType::Term("world".to_string()),
                    QueryType::Term("rust".to_string())
                ])
            ])
        );
    }

    #[test]
    fn groups_next_to_words_are_or_chunks() {
        let term = |text: &str| QueryType::Term(text.to_string());
        let and = |a: &str, b: &str| QueryType::And(vec![term(a), term(b)]);
        assert_eq!(
            QueryType::parse("(a AND b) c"),
            QueryType::Or(vec![and("a", "b"), term("c")])
        );
        assert_eq!(
            QueryType::parse("x (a AND b)"),
            QueryType::Or(vec![term("x"), and("a", "b")])
        );
        assert_eq!(
            QueryType::parse("(a AND b) (c AND d)"),
            QueryType::Or(vec![and("a", "b"), and("c", "d")])
        );
        assert_eq!(
            QueryType::parse("NOT (a OR b) c"),
            QueryType::Not(Box::new(QueryType::Or(vec![
                QueryType::Or(vec![term("a"), term("b")]),
                term("c"),
            ])))
        );
        // An unclosed group still swallows the rest of the input.
        assert_eq!(
            QueryType::parse("x (a AND b"),
            QueryType::Or(vec![term("x"), and("a", "b")])
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
                QueryType::Term("巨蟹".to_string()),
                QueryType::Term("rust".to_string())
            ])
        );

        // 中文词 OR 中文词
        assert_eq!(
            QueryType::parse("巨蟹 OR 天蝎"),
            QueryType::Or(vec![
                QueryType::Term("巨蟹".to_string()),
                QueryType::Term("天蝎".to_string())
            ])
        );

        // 带括号的中文表达式
        assert_eq!(
            QueryType::parse("(巨蟹 AND 座) OR 天蝎"),
            QueryType::Or(vec![
                QueryType::And(vec![
                    QueryType::Term("巨蟹".to_string()),
                    QueryType::Term("座".to_string())
                ]),
                QueryType::Term("天蝎".to_string())
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
                QueryType::Term("巨蟹".to_string()),
                QueryType::Term("天蝎".to_string()),
                QueryType::Term("双鱼".to_string())
            ])
        );
    }

    #[test]
    fn try_parse_rejects_excessive_parenthesis_depth() {
        let query = format!("{}hello{}", "(".repeat(MAX_LOGICAL_QUERY_DEPTH + 1), ")");
        assert!(QueryType::try_parse(&query).is_err());
    }

    #[test]
    fn try_parse_rejects_close_parenthesis_flood() {
        // Regression: a ')' flood used to bypass validate_query_input (which
        // only counted '(') and overflow the stack inside parse_term.
        let query = format!("x{}", ")".repeat(MAX_LOGICAL_QUERY_DEPTH + 1));
        assert!(QueryType::try_parse(&query).is_err());

        // Within the guard budget the query still parses leniently.
        let query = format!("x{}", ")".repeat(MAX_LOGICAL_QUERY_DEPTH));
        assert_eq!(
            QueryType::try_parse(&query).unwrap(),
            QueryType::Term("x".to_string())
        );
    }

    #[test]
    fn parse_survives_parenthesis_floods_without_stack_overflow() {
        // Regression: the unguarded public `parse` used to recurse once per
        // trailing ')', so `"x" + ")"*8190` aborted with a stack overflow in
        // debug builds. Contiguous ')' runs are now stripped in one step.
        let query = format!("x{}", ")".repeat(8190));
        assert_eq!(QueryType::parse(&query), QueryType::Term("x".to_string()));

        // Far past any length guard: still O(1) recursion.
        let query = format!("x{}", ")".repeat(1_000_000));
        assert_eq!(QueryType::parse(&query), QueryType::Term("x".to_string()));

        // '(' floods recurse once per '(' but are cut off by the depth budget.
        let query = format!("{}hello", "(".repeat(1_000_000));
        let _ = QueryType::parse(&query);

        // Spaced ')' floods cannot be stripped in one pass; the depth budget
        // caps the recursion and the rest degrades to plain terms.
        let query = format!("x{}", " )".repeat(100_000));
        let _ = QueryType::parse(&query);

        // Alternating unbalanced parentheses.
        let query = ")(".repeat(500_000);
        let _ = QueryType::parse(&query);
    }

    #[test]
    fn parse_close_paren_stripping_matches_old_semantics() {
        // Trailing ')' stripping must keep the lenient-parse results.
        assert_eq!(
            QueryType::parse("hello AND world))"),
            QueryType::And(vec![
                QueryType::Term("hello".to_string()),
                QueryType::Term("world".to_string())
            ])
        );
        assert_eq!(
            QueryType::parse("a) OR b)"),
            QueryType::Or(vec![
                QueryType::Term("a".to_string()),
                QueryType::Term("b".to_string())
            ])
        );
        assert_eq!(
            QueryType::parse("NOT NOT a))"),
            QueryType::Not(Box::new(QueryType::Not(Box::new(QueryType::Term(
                "a".to_string()
            )))))
        );
        assert_eq!(QueryType::parse(")))"), QueryType::Or(vec![]));
    }

    #[test]
    fn parse_not_chains_nest_one_negation_per_not() {
        assert_eq!(
            QueryType::parse("NOT a"),
            QueryType::Not(Box::new(QueryType::Term("a".to_string())))
        );
        assert_eq!(
            QueryType::parse("NOT NOT a"),
            QueryType::Not(Box::new(QueryType::Not(Box::new(QueryType::Term(
                "a".to_string()
            )))))
        );
        assert_eq!(
            QueryType::parse("b AND NOT NOT a"),
            QueryType::And(vec![
                QueryType::Term("b".to_string()),
                QueryType::Not(Box::new(QueryType::Not(Box::new(QueryType::Term(
                    "a".to_string()
                )))))
            ])
        );
        // Operators are case-sensitive: a lowercase `not` is a search word.
        assert_eq!(
            QueryType::parse("not a"),
            QueryType::Or(vec![
                QueryType::Term("not".to_string()),
                QueryType::Term("a".to_string())
            ])
        );
        // A NOT flood is bounded like a parenthesis flood: parsing stays
        // total and the tree stays shallow enough to validate and drop.
        let flood = "NOT ".repeat(100_000) + "a";
        let query = QueryType::parse(&flood);
        assert!(query.validate_complexity().is_err());
    }
}
