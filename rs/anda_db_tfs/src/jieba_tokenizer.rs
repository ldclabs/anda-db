use tantivy::tokenizer::{LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer};
use tantivy_jieba::JiebaTokenizer;
use tantivy_tokenizer_api::{Token, TokenFilter, TokenStream, Tokenizer};

use super::TokenizerChain;

/// Creates a new `TokenizerChain` with `JiebaTokenizer` as the default tokenizer.
pub fn jieba_tokenizer() -> TokenizerChain {
    TokenizerChain::builder(SimpleTokenizer::default())
        .filter(JiebaMergeFilter::new())
        .filter(RemoveLongFilter::limit(32))
        .filter(LowerCaser)
        .filter(Stemmer::default())
        .build()
}

/// 检测文本的主要字符集/语言
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Script {
    Latin,
    Cyrillic,
    Arabic,
    Cjk,
    Other,
}

pub fn detect_script(text: &str) -> Script {
    let mut latin = 0;
    let mut cyrillic = 0;
    let mut arabic = 0;
    let mut cjk = 0;

    for c in text.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '\u{00C0}'..='\u{024F}' => latin += 1,
            '\u{0400}'..='\u{04FF}' => cyrillic += 1,
            '\u{0600}'..='\u{06FF}' | '\u{0750}'..='\u{077F}' => arabic += 1,
            '\u{4e00}'..='\u{9fff}' | '\u{3400}'..='\u{4dbf}' => cjk += 1,
            _ => {}
        }
    }

    if cjk > 0 {
        return Script::Cjk;
    }

    let max = latin.max(cyrillic).max(arabic);
    if max == 0 {
        return Script::Other;
    }

    if latin == max {
        Script::Latin
    } else if cyrillic == max {
        Script::Cyrillic
    } else {
        Script::Arabic
    }
}

/// Jieba 中文分词合并过滤器
#[derive(Clone)]
pub struct JiebaMergeFilter {
    tokenizer: JiebaTokenizer,
}

impl Default for JiebaMergeFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl JiebaMergeFilter {
    pub fn new() -> Self {
        Self {
            tokenizer: JiebaTokenizer::new(),
        }
    }
}

impl TokenFilter for JiebaMergeFilter {
    type Tokenizer<T: Tokenizer> = JiebaMergeTokenizer<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        JiebaMergeTokenizer {
            inner: tokenizer,
            tokenizer: self.tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct JiebaMergeTokenizer<T> {
    inner: T,
    tokenizer: JiebaTokenizer,
}

impl<T: Tokenizer> Tokenizer for JiebaMergeTokenizer<T> {
    type TokenStream<'a> = MergedTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let mut inner_stream = self.inner.token_stream(text);
        let mut tokens = Vec::new();
        let mut needs_sort = false;

        while let Some(token) = inner_stream.next() {
            let mut handle_cjk = false;
            if detect_script(&token.text) == Script::Cjk {
                needs_sort = true;
                let mut jieba_stream = self.tokenizer.token_stream(&token.text);
                while let Some(jieba_token) = jieba_stream.next() {
                    let mut new_token = jieba_token.clone();
                    new_token.offset_from += token.offset_from;
                    new_token.offset_to += token.offset_from;
                    new_token.position = token.position;
                    new_token.position_length = token.position_length;
                    handle_cjk = true;
                    tokens.push(new_token);
                }
            }

            if !handle_cjk {
                tokens.push(token.clone());
            }
        }

        if needs_sort {
            tokens.sort_unstable_by(|a, b| {
                a.offset_from
                    .cmp(&b.offset_from)
                    .then(a.offset_to.cmp(&b.offset_to))
                    .then(a.position.cmp(&b.position))
                    .then(a.text.cmp(&b.text))
            });
        }

        MergedTokenStream { tokens, index: 0 }
    }
}

pub struct MergedTokenStream {
    tokens: Vec<Token>,
    index: usize,
}

impl TokenStream for MergedTokenStream {
    fn advance(&mut self) -> bool {
        if self.index >= self.tokens.len() {
            return false;
        }
        self.index += 1;
        true
    }

    fn token(&self) -> &Token {
        &self.tokens[self.index - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.index - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::collect_tokens;

    #[test]
    fn test_jieba_collect_tokens() {
        let mut tokenizer = jieba_tokenizer();

        let tokens = collect_tokens(
            &mut tokenizer,
            "北京市东城区长安街。55 Rue du Faubourg Saint-Honoré, Paris. Москва, Красная площадь. 1600 Pennsylvania Avenue NW, Washington, D.C. 湖南湘潭",
            None,
        );
        println!("{:?}", tokens);
        assert!(tokens.contains_key("北京"));
        assert!(tokens.contains_key("东城区"));
        assert!(tokens.contains_key("长安街"));
        assert!(tokens.contains_key("55"));
        assert!(tokens.contains_key("rue"));
        assert!(tokens.contains_key("faubourg"));
        assert!(tokens.contains_key("saint"));
        assert!(tokens.contains_key("honoré"));
        assert!(tokens.contains_key("pari"));
        assert!(tokens.contains_key("москва"));
        assert!(tokens.contains_key("красная"));
        assert!(tokens.contains_key("площадь"));
        assert!(tokens.contains_key("1600"));
        assert!(tokens.contains_key("pennsylvania"));
        assert!(tokens.contains_key("avenu"));
        assert!(tokens.contains_key("nw"));
        assert!(tokens.contains_key("washington"));
        // assert!(tokens.contains_key("d"));
        // assert!(tokens.contains_key("c"));
        assert!(tokens.contains_key("湖南"));
        assert!(tokens.contains_key("湘潭"));
    }
}
