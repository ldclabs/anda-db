use anda_db_tfs::{
    JiebaMergeFilter, TokenizerChain, collect_tokens, default_tokenizer, jieba_tokenizer,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::{hint::black_box, time::Duration};
use tantivy::tokenizer::{LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer};

fn generic_jieba() -> TokenizerChain {
    TokenizerChain::builder(SimpleTokenizer::default())
        .filter(JiebaMergeFilter::new())
        .filter(RemoveLongFilter::limit(32))
        .filter(LowerCaser)
        .filter(Stemmer::default())
        .build()
}

fn benchmark(c: &mut Criterion) {
    // Fixed English, Chinese and mixed corpora: benchmark input must not
    // change when documentation or Cargo.lock changes.
    let english = "Rust powers embedded memory. Search ranks documents with lexical retrieval and document indexing. ";
    let chinese = "数据库支持全文搜索。人工智能需要长期记忆。北京大学研究语言模型。";
    let mut group = c.benchmark_group("tokenizer");
    group.sample_size(20);
    for (name, text) in [
        ("english", english.repeat(128)),
        ("chinese", chinese.repeat(128)),
        ("mixed", format!("{english}{chinese}").repeat(128)),
    ] {
        group.throughput(Throughput::Bytes(text.len() as u64));
        let variants = if name == "english" {
            vec![
                ("default", default_tokenizer()),
                ("generic_jieba", generic_jieba()),
                ("streaming_jieba", jieba_tokenizer()),
            ]
        } else {
            vec![
                ("generic_jieba", generic_jieba()),
                ("streaming_jieba", jieba_tokenizer()),
            ]
        };
        // Initialize Jieba's dictionary outside the measurement.
        for (variant, mut tokenizer) in variants {
            assert!(!collect_tokens(&mut tokenizer, &text, None).is_empty());
            group.bench_function(BenchmarkId::new(variant, name), |b| {
                b.iter(|| black_box(collect_tokens(&mut tokenizer, black_box(&text), None)))
            });
        }
    }
    group.finish();
}

criterion_group! { name = benches; config = Criterion::default().warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1)); targets = benchmark }
criterion_main!(benches);
