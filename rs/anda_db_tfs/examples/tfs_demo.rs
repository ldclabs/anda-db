use anda_db_tfs::{BM25Index, jieba_tokenizer};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Document {
    id: u64,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    structured_logger::Builder::new().init();

    // 创建索引
    let index = BM25Index::new("anda_db_tfs_bm25".to_string(), jieba_tokenizer(), None);

    // 批量添加文档
    let docs = vec![
        Document {
            id: 0,
            text: "Rust is a systems programming language".into(),
        },
        Document {
            id: 1,
            text: "Rust is fast and memory efficient, 安全、并发、实用".into(),
        },
        Document {
            id: 2,
            text: "Python is a dynamic language".into(),
        },
    ];

    for doc in &docs {
        index.insert(doc.id, &doc.text, 0).unwrap();
    }

    assert_eq!(index.len(), 3);

    // 搜索
    let results = index.search("rust memory", 2, None);
    for (doc_id, score) in results {
        println!("Found doc {}, score: {:.2}", doc_id, score);
    }

    let results = index.search("安全", 2, None);
    for (doc_id, score) in results {
        println!("Found doc {}, score: {:.2}", doc_id, score);
    }

    // 删除文档
    index.remove(docs[2].id, &docs[2].text, 0);
    println!("Total documents after removal: {}", index.len());

    // 保存和加载
    std::fs::create_dir_all("debug/tfs_demo")?;
    {
        let metadata = std::fs::File::create("debug/tfs_demo/metadata.cbor")?;
        index
            .flush(metadata, 0, async |id, data| {
                let mut node = std::fs::File::create(format!("debug/tfs_demo/b_{id}.cbor"))?;
                node.write_all(data)?;
                Ok(true)
            })
            .await?;
    }

    let metadata = std::fs::File::open("debug/tfs_demo/metadata.cbor")?;
    let loaded_index = BM25Index::load_all(jieba_tokenizer(), metadata, async |id| {
        let mut node = std::fs::File::open(format!("debug/tfs_demo/b_{id}.cbor"))?;
        let mut buf = Vec::new();
        node.read_to_end(&mut buf)?;
        Ok(Some(buf))
    })
    .await?;
    println!("Loaded index with {} documents", loaded_index.len());

    Ok(())
}
