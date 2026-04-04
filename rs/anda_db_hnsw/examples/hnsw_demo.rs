use anda_db_hnsw::{HnswConfig, HnswIndex};
use rand::RngExt;
use std::io::{Read, Write};
use tokio::time;

// extern crate blas_src;

pub fn unix_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch");
    ts.as_millis() as u64
}

// cargo build --example hnsw_demo --release
// ./target/release/examples/hnsw_demo
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    structured_logger::Builder::new().init();

    const DIM: usize = 384;

    // 创建索引 (384维向量，如BERT嵌入)
    let config = HnswConfig {
        dimension: DIM,
        ..Default::default()
    };
    // 39900 inserted 100 vectors in 2.479251625s
    // 39900 Search returned 10 results in 2.368542ms
    // 40000 inserted 100 vectors in 2.459010458s
    // 40000 Search returned 10 results in 2.661541ms
    // 40000 Removed vector 16909 in 16.174542ms
    // config.select_neighbors_strategy = SelectNeighborsStrategy::Simple;
    // 39900 inserted 100 vectors in 631.205083ms
    // 39900 Search returned 10 results in 2.442875ms
    // 40000 inserted 100 vectors in 637.636791ms
    // 40000 Search returned 10 results in 2.136208ms
    // 40000 Removed vector 13432 in 12.864834ms
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // 模拟数据流
    let mut rng = rand::rng();

    let mut inert_start = time::Instant::now();
    for i in 0..1_000 {
        let vector: Vec<f32> = (0..DIM).map(|_| rng.random::<f32>()).collect();
        let _ = index.insert_f32(i as u64, vector, unix_ms())?;
        // println!("{} inserted vector {}", i, i);

        // 模拟搜索查询
        if i % 100 == 0 {
            println!("{} inserted 100 vectors in {:?}", i, inert_start.elapsed());
            inert_start = time::Instant::now();

            let query: Vec<f32> = (0..DIM).map(|_| rng.random::<f32>()).collect();
            let query_start = time::Instant::now();
            let results = index.search_f32(&query, 10)?;
            println!(
                "{} Search returned {} results in {:?}",
                i,
                results.len(),
                query_start.elapsed()
            );
        }

        // 模拟删除
        if i % 1000 == 0 && i > 0 {
            let to_remove = rng.random_range(0..i);
            let remove_start = time::Instant::now();
            index.remove(to_remove, unix_ms());
            println!(
                "{} Removed vector {} in {:?}",
                i,
                to_remove,
                remove_start.elapsed()
            );
        }
    }

    // 打印统计信息
    let stats = index.stats();
    println!("Index statistics:");
    println!("- Total vectors: {}", stats.num_elements);
    println!("- Max layer: {}", stats.max_layer);
    println!("- Search operations: {}", stats.search_count);
    println!("- Insert operations: {}", stats.insert_count);
    println!("- Delete operations: {}", stats.delete_count);

    // 最终保存
    {
        let metadata = std::fs::File::create("debug/hnsw_demo/metadata.cbor")?;
        let ids = std::fs::File::create("debug/hnsw_demo/ids.cbor")?;
        let store_start = time::Instant::now();
        index
            .flush(metadata, ids, 0, async |id, data| {
                let mut node = std::fs::File::create(format!("debug/hnsw_demo/node_{id}.cbor"))?;
                node.write_all(data)?;
                Ok(true)
            })
            .await?;

        // metadata.close().await?;
        // ids.close().await?;
        println!("Stored index with nodes in {:?}", store_start.elapsed());
    }

    let metadata = std::fs::File::open("debug/hnsw_demo/metadata.cbor")?;
    let ids = std::fs::File::open("debug/hnsw_demo/ids.cbor")?;
    let load_start = time::Instant::now();
    let loaded_index = HnswIndex::load_all(metadata, ids, async |id| {
        let mut node = std::fs::File::open(format!("debug/hnsw_demo/node_{id}.cbor"))?;
        let mut buf = Vec::new();
        node.read_to_end(&mut buf)?;
        Ok(Some(buf))
    })
    .await?;

    println!("Load index in {:?}", load_start.elapsed());
    let query: Vec<f32> = (0..DIM).map(|_| rng.random::<f32>()).collect();
    let query_start = time::Instant::now();
    let results = loaded_index.search_f32(&query, 10)?;
    println!(
        "Search returned {} results in {:?}",
        results.len(),
        query_start.elapsed()
    );

    Ok(())
}
