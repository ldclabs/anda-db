//! The token pipeline — one shared tokenization authority for indexing and
//! querying.
//!
//! ALL normalization lives here, never in clients (write/read symmetry):
//! NFKC → lowercase → script-aware segmentation (Han runs through jieba
//! cut_for_search, everything else through UAX#29 word boundaries) → drop
//! tokens with no letter/digit → dedupe (order-preserving) → cap.

use std::collections::HashSet;
use std::sync::OnceLock;

use jieba_rs::Jieba;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

/// Persisted with every index row by consumers. Bump on ANY behavior change
/// (normalization rules, jieba-rs upgrade, dictionary change): a mismatch
/// means "rebuild the search index", not "hope the vocabularies overlap".
pub const TOKENIZER_VERSION: &str = "1";

/// Per-text token cap. Search-mode jieba emits overlapping n-grams for CJK, so
/// this must be large enough for normal title-and-summary documents; a
/// distinctive keyword past the cap never enters the index.
const MAX_TOKENS_PER_TEXT: usize = 256;
/// Defensive input truncation (chars) — truncate, don't reject: the write
/// path must converge even on an oversized row. Axum's 2 MB default body limit
/// remains the effective per-request ceiling.
const MAX_CHARS_PER_TEXT: usize = 1024 * 1024;

static JIEBA: OnceLock<Jieba> = OnceLock::new();

fn jieba() -> &'static Jieba {
    JIEBA.get_or_init(Jieba::new)
}

/// Build the jieba dictionary now, so the first request after a cold start
/// pays network latency only, not the jieba init.
pub fn warm_up() {
    let _ = jieba();
}

/// Han ideographs — the scripts jieba actually has a dictionary for. Kana,
/// Hangul, Cyrillic, Arabic and friends fall through to UAX#29 word
/// segmentation: jieba would degrade them to single characters.
fn is_han(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}'      // CJK Unified Ideographs
        | '\u{3400}'..='\u{4DBF}'    // Extension A
        | '\u{F900}'..='\u{FAFF}'    // Compatibility Ideographs
        | '\u{20000}'..='\u{2FA1F}'  // Extensions B–F + supplement
        | '\u{3005}' | '\u{3007}'    // 々 iteration mark, 〇 zero
    )
}

/// Search folding for spelling variants that the downstream FTS layer must
/// not be asked to reconcile. Runs after NFKC + lowercase, before
/// segmentation, on queries and indexed text alike (write/read symmetry).
/// `None` drops the character.
fn search_fold(c: char) -> Option<char> {
    match c {
        // Russian: ё and е are interchangeable in everyday typing — queries
        // routinely spell партнёр as партнер.
        'ё' => Some('е'),
        // Arabic, per Lucene's ArabicNormalizer: strip tatweel + harakat…
        '\u{0640}' => None,              // ـ tatweel (kashida stretching)
        '\u{064B}'..='\u{0652}' => None, // fathatan … sukun (vowel marks)
        '\u{0670}' => None,              // superscript alef
        // …and fold the variants writers use interchangeably.
        '\u{0622}' | '\u{0623}' | '\u{0625}' => Some('\u{0627}'), // آ أ إ → ا
        '\u{0649}' => Some('\u{064A}'),                           // ى → ي
        '\u{0629}' => Some('\u{0647}'),                           // ة → ه
        _ => Some(c),
    }
}

/// Maximal same-script runs (Han vs everything else) in input order, as
/// slices of the input — each segmenter only ever sees text it can cut.
fn script_runs(text: &str) -> Vec<(&str, bool)> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut run_is_han = false;
    for (i, c) in text.char_indices() {
        let han = is_han(c);
        if i == 0 {
            run_is_han = han;
        } else if han != run_is_han {
            runs.push((&text[start..i], run_is_han));
            start = i;
            run_is_han = han;
        }
    }
    if start < text.len() {
        runs.push((&text[start..], run_is_han));
    }
    runs
}

/// Collects segmenter output under the pipeline's tail rules: noise filter,
/// order-preserving dedupe, per-text cap.
#[derive(Default)]
struct TokenSink {
    seen: HashSet<String>,
    tokens: Vec<String>,
}

impl TokenSink {
    fn push(&mut self, raw: &str) {
        let token = raw.trim();
        // Keep only tokens that carry at least one letter or digit: pure
        // punctuation/whitespace/emoji segments are FTS noise.
        if !token.chars().any(char::is_alphanumeric) {
            return;
        }
        if self.is_full() || self.seen.contains(token) {
            return;
        }
        self.seen.insert(token.to_owned());
        self.tokens.push(token.to_owned());
    }

    fn is_full(&self) -> bool {
        self.tokens.len() >= MAX_TOKENS_PER_TEXT
    }
}

/// One text → its normalized search tokens. The single definition of the
/// token pipeline — the golden tests below pin its behavior across releases.
/// Han runs go through jieba cut_for_search (overlapping n-grams for recall);
/// every other run — Latin, Cyrillic, Arabic, digits — splits on UAX#29 word
/// boundaries, which jieba cannot do outside ASCII.
pub fn tokenize_for_search(text: &str) -> Vec<String> {
    let truncated: String = text.chars().take(MAX_CHARS_PER_TEXT).collect();
    // NFKC first (full-width → half-width, compatibility forms), then a full
    // Unicode lowercase, then per-script search folding — so "ＡＩ" and "Ai"
    // both index as "ai", "партнёр" as "партнер".
    let normalized: String = truncated
        .nfkc()
        .collect::<String>()
        .to_lowercase()
        .chars()
        .filter_map(search_fold)
        .collect();
    let mut sink = TokenSink::default();
    for (run, is_han) in script_runs(&normalized) {
        // Cap reached — later runs can't contribute; don't keep feeding a
        // megabyte of defensively-truncated text through jieba for nothing.
        if sink.is_full() {
            break;
        }
        if is_han {
            for raw in jieba().cut_for_search(run, true) {
                sink.push(raw.word);
            }
        } else {
            for word in run.unicode_words() {
                sink.push(word);
            }
        }
    }
    sink.tokens
}

// ---------------------------------------------------------------------------
// Golden tests: these PIN tokenizer behavior. If an intentional change (jieba
// upgrade, rule change) alters any expectation, bump TOKENIZER_VERSION and
// run the §7.3 index rebuild as part of the same release.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_chinese() {
        assert_eq!(
            tokenize_for_search("我在找深圳的硬件供应链合作伙伴"),
            vec![
                "我",
                "在",
                "找",
                "深圳",
                "的",
                "硬件",
                "供应",
                "供应链",
                "合作",
                "伙伴",
                "合作伙伴"
            ]
        );
    }

    #[test]
    fn golden_english() {
        assert_eq!(
            tokenize_for_search("Looking for a hardware supply-chain partner in Shenzhen"),
            vec!["looking", "for", "a", "hardware", "supply", "chain", "partner", "in", "shenzhen"]
        );
    }

    #[test]
    fn golden_mixed_cjk_latin() {
        assert_eq!(
            tokenize_for_search("AI创业者 seeking GPU credits 🚀"),
            vec!["ai", "创业", "业者", "创业者", "seeking", "gpu", "credits"]
        );
    }

    #[test]
    fn golden_fullwidth_nfkc_folds() {
        // Full-width "ＧＰＵ" and half-width "gpu" must index identically.
        assert_eq!(tokenize_for_search("ＧＰＵ"), vec!["gpu"]);
    }

    #[test]
    fn golden_russian_words_not_chars() {
        // Cyrillic segments on UAX#29 word boundaries (jieba alone split this
        // into 18 single letters — alphabet soup, not an index), and ё folds
        // to е (партнёра → партнера).
        assert_eq!(
            tokenize_for_search("Ищу партнёра по цепочке поставок в Москве"),
            vec![
                "ищу",
                "партнера",
                "по",
                "цепочке",
                "поставок",
                "в",
                "москве"
            ]
        );
    }

    #[test]
    fn golden_arabic_words_not_chars() {
        // Orthographic folding: أبحث → ابحث (alef hamza), سلسلة → سلسله
        // (teh marbuta).
        assert_eq!(
            tokenize_for_search("أبحث عن شريك في سلسلة التوريد"),
            vec!["ابحث", "عن", "شريك", "في", "سلسله", "التوريد"]
        );
    }

    #[test]
    fn golden_arabic_orthographic_folds() {
        // Vocalized (harakat) and variant spellings must index identically:
        // writers use أ/ا, ى/ي, ة/ه interchangeably and queries omit harakat.
        assert_eq!(tokenize_for_search("مُهَنْدِس"), tokenize_for_search("مهندس"));
        assert_eq!(tokenize_for_search("أبحث"), tokenize_for_search("ابحث"));
        assert_eq!(tokenize_for_search("مصطفى"), tokenize_for_search("مصطفي"));
    }

    #[test]
    fn golden_russian_yo_folds_to_ye() {
        // Without ё→е here,
        // an intent saying "партнёр" never matches a query typed "партнер".
        assert_eq!(tokenize_for_search("партнёр"), vec!["партнер"]);
        assert_eq!(
            tokenize_for_search("Партнёр"),
            tokenize_for_search("партнер")
        );
    }

    #[test]
    fn golden_spanish() {
        // Plain UAX#29 path; ñ/accents stay intact. Second "de" dedupes.
        assert_eq!(
            tokenize_for_search("Busco un socio de cadena de suministro en Cataluña"),
            vec![
                "busco",
                "un",
                "socio",
                "de",
                "cadena",
                "suministro",
                "en",
                "cataluña"
            ]
        );
    }

    #[test]
    fn golden_french_elision_stays_joined() {
        // UAX#29 keeps l'/d' elisions attached (apostrophe between letters).
        // That is fine: the FTS layer splits at the apostrophe on
        // BOTH write and query side, so "entrepreneur" matches "l'entrepreneur".
        assert_eq!(
            tokenize_for_search("L'entrepreneur cherche un partenaire d'affaires"),
            vec![
                "l'entrepreneur",
                "cherche",
                "un",
                "partenaire",
                "d'affaires"
            ]
        );
    }

    #[test]
    fn golden_accented_latin_stays_whole() {
        // jieba-alone segmentation fragmented "café" into "caf" + "é"; words
        // must survive intact.
        assert_eq!(
            tokenize_for_search("Café Übersetzen für São Paulo"),
            vec!["café", "übersetzen", "für", "são", "paulo"]
        );
    }

    #[test]
    fn golden_mixed_han_cyrillic() {
        // Run boundaries: Han goes through jieba, Cyrillic through UAX#29,
        // order preserved.
        assert_eq!(
            tokenize_for_search("寻找Москва的供应链伙伴"),
            vec!["寻找", "москва", "的", "供应", "供应链", "伙伴"]
        );
    }

    #[test]
    fn golden_emoji_only_is_empty() {
        assert_eq!(tokenize_for_search("🚀🔥✨"), Vec::<String>::new());
    }

    #[test]
    fn golden_empty_and_whitespace() {
        assert_eq!(tokenize_for_search(""), Vec::<String>::new());
        assert_eq!(tokenize_for_search("   \n\t"), Vec::<String>::new());
    }

    #[test]
    fn dedupes_preserving_first_occurrence_order() {
        assert_eq!(
            tokenize_for_search("gpu gpu credits gpu"),
            vec!["gpu", "credits"]
        );
    }

    #[test]
    fn caps_tokens_per_text() {
        let long: String = (0..MAX_TOKENS_PER_TEXT + 100)
            .map(|i| format!("word{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(tokenize_for_search(&long).len(), MAX_TOKENS_PER_TEXT);
    }

    #[test]
    fn cap_covers_full_field_budget() {
        // A common title + summary payload can contain ~200 distinct filler
        // words; a distinctive keyword at the end must still enter the index
        // (an earlier 64-token cap dropped it).
        let mut text = (0..200)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        text.push_str(" 供应链");
        assert!(tokenize_for_search(&text).contains(&"供应链".to_string()));
    }

    #[test]
    fn truncates_oversized_input_without_error() {
        // Latin filler (script-agnostic truncation happens before
        // segmentation; a couple MiB of Han through debug-build jieba would
        // make this test crawl for nothing).
        let phrase = "lorem ipsum dolor sit amet ";
        let oversized = phrase.repeat(MAX_CHARS_PER_TEXT * 2 / phrase.len());
        assert!(oversized.chars().count() > MAX_CHARS_PER_TEXT);
        let tokens = tokenize_for_search(&oversized);
        assert!(!tokens.is_empty());
        assert!(tokens.len() <= MAX_TOKENS_PER_TEXT);
    }

    #[test]
    fn cap_stops_segmentation_early() {
        // Once the cap fills, remaining runs are skipped entirely — with a
        // 1 MiB input ceiling the pipeline must not keep segmenting for
        // tokens it can never emit. Output equivalence is the contract.
        let head: String = (0..MAX_TOKENS_PER_TEXT)
            .map(|i| format!("word{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let with_tail = format!("{head} 供应链 tail");
        assert_eq!(tokenize_for_search(&with_tail), tokenize_for_search(&head));
    }
}
