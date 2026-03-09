use std::collections::HashMap;

pub use tantivy_tokenizer_api::*;

/// A chain of tokenizers and filters implemented Tokenizer trait.
pub struct TokenizerChain {
    tokenizer: Box<dyn BoxableTokenizer>,
}

impl TokenizerChain {
    /// Creates a new `TokenizerChainBuilder` with the given tokenizer
    pub fn builder<T: Tokenizer>(tokenizer: T) -> TokenizerChainBuilder<T> {
        TokenizerChainBuilder::new(tokenizer)
    }
}

/// Builder for `TokenizerChain`
pub struct TokenizerChainBuilder<T = Box<dyn BoxableTokenizer>> {
    tokenizer: T,
}

impl<T: Tokenizer> TokenizerChainBuilder<T> {
    /// Creates a new `TokenizerChainBuilder` with the given tokenizer
    pub fn new(tokenizer: T) -> Self {
        TokenizerChainBuilder { tokenizer }
    }

    /// Adds a token filter to the chain
    pub fn filter<F: TokenFilter>(self, token_filter: F) -> TokenizerChainBuilder<F::Tokenizer<T>> {
        TokenizerChainBuilder {
            tokenizer: token_filter.transform(self.tokenizer),
        }
    }

    /// Builds the `TokenizerChain`
    pub fn build(self) -> TokenizerChain {
        TokenizerChain {
            tokenizer: Box::new(self.tokenizer),
        }
    }
}

impl Clone for TokenizerChain {
    fn clone(&self) -> Self {
        TokenizerChain {
            tokenizer: self.tokenizer.box_clone(),
        }
    }
}

/// Implement Tokenizer trait for TokenizerChain
impl Tokenizer for TokenizerChain {
    /// The token stream returned by this Tokenizer.
    type TokenStream<'a> = BoxTokenStream<'a>;

    /// Creates a token stream for a given `str`.
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        self.tokenizer.box_token_stream(text)
    }
}

/// A boxable `Tokenizer`, with its `TokenStream` type erased.
pub trait BoxableTokenizer: 'static + Send + Sync {
    /// Creates a boxed token stream for a given `str`.
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a>;
    /// Clone this tokenizer.
    fn box_clone(&self) -> Box<dyn BoxableTokenizer>;
}

impl<T: Tokenizer> BoxableTokenizer for T {
    fn box_token_stream<'a>(&'a mut self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::new(self.token_stream(text))
    }
    fn box_clone(&self) -> Box<dyn BoxableTokenizer> {
        Box::new(self.clone())
    }
}

/// Creates a new `TokenizerChain` with `SimpleTokenizer` as the default tokenizer and `RemoveLongFilter`, `LowerCaser` and `Stemmer` as the default filters.
#[cfg(any(test, feature = "tantivy"))]
pub fn default_tokenizer() -> TokenizerChain {
    use tantivy::tokenizer::{LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer};

    TokenizerChain::builder(SimpleTokenizer::default())
        .filter(RemoveLongFilter::limit(32))
        .filter(LowerCaser)
        .filter(Stemmer::default())
        .build()
}

/// Tokenizes text and optionally filters tokens
///
/// # Arguments
///
/// * `tokenizer` - Tokenizer to use for processing text
/// * `text` - Text to tokenize
/// * `inclusive` - Optional list of tokens to include (if provided, only tokens in this list will be returned)
///
/// # Returns
///
/// A HashMap of tokens and their counts.
/// The keys are the token strings and the values are their counts.
pub fn collect_tokens<T: Tokenizer>(
    tokenizer: &mut T,
    text: &str,
    inclusive: Option<&HashMap<String, usize>>,
) -> HashMap<String, usize> {
    let mut stream = tokenizer.token_stream(text);
    let mut tokens = HashMap::new();
    while let Some(token) = stream.next() {
        if token.text.len() <= 1 {
            continue;
        }

        if let Some(inclusive) = inclusive
            && !inclusive.contains_key(&token.text)
        {
            continue;
        }

        *tokens.entry(token.text.to_owned()).or_default() += 1;
    }
    tokens
}

/// Performs a simple full-text search by finding matching tokens in a document
///
/// # Arguments
///
/// * `tokenizer` - Tokenizer to use for processing text
/// * `query` - Search query text
/// * `text` - Segment text to search in
///
/// # Returns
///
/// A HashMap of matching tokens and their counts.
/// The keys are the token strings and the values are their counts.
pub fn flat_full_text_search<T: Tokenizer>(
    tokenizer: &mut T,
    query: &str,
    text: &str,
) -> HashMap<String, usize> {
    let tokens = collect_tokens(tokenizer, query, None);
    collect_tokens(tokenizer, text, Some(&tokens))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flat_full_text_search() {
        let mut tokenizer = default_tokenizer();

        // 测试基本匹配
        let matches = flat_full_text_search(
            &mut tokenizer,
            "fox dog",
            "The quick brown fox jumps over the lazy dog",
        );
        assert_eq!(matches.len(), 2);
        assert_eq!(matches.get("fox"), Some(&1));
        assert_eq!(matches.get("dog"), Some(&1));

        // 测试部分匹配
        let matches = flat_full_text_search(
            &mut tokenizer,
            "fox cat",
            "The quick brown fox jumps over the lazy dog",
        );
        assert_eq!(matches.len(), 1);
        assert!(matches.contains_key("fox"));

        // 测试无匹配
        let matches = flat_full_text_search(
            &mut tokenizer,
            "elephant giraffe",
            "The quick brown fox jumps over the lazy dog",
        );
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_collect_tokens() {
        let mut tokenizer = default_tokenizer();

        // 测试基本分词
        let tokens = collect_tokens(&mut tokenizer, "The quick brown fox", None);
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.get("the"), Some(&1));
        assert_eq!(tokens.get("quick"), Some(&1));
        assert_eq!(tokens.get("brown"), Some(&1));
        assert_eq!(tokens.get("fox"), Some(&1));

        // 测试过滤分词
        let inclusive = HashMap::from([("quick".to_string(), 1), ("fox".to_string(), 1)]);
        let tokens = collect_tokens(
            &mut tokenizer,
            "The quick brown fox, foxes",
            Some(&inclusive),
        );
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.get("quick"), Some(&1));
        assert_eq!(tokens.get("fox"), Some(&2));
    }
}
