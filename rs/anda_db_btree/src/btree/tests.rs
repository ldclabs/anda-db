#[derive(Serialize)]
struct LegacyIndexRef<'a> {
    #[serde(serialize_with = "serialize_legacy_metadata")]
    metadata: &'a BTreeMetadata,
}
fn serialize_legacy_metadata<S: serde::Serializer>(
    m: &BTreeMetadata,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;
    let mut state = serializer.serialize_struct("BTreeMetadata", 3)?;
    state.serialize_field("name", &m.name)?;
    state.serialize_field("config", &m.config)?;
    state.serialize_field("stats", &m.stats)?;
    state.end()
}

use super::*;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Barrier;

// 获取当前时间戳（毫秒）
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// In-memory model of the durable store: one metadata object plus one
/// object per `(bucket_id, generation)`, mirroring the production
/// object-store layout.
#[derive(Default, Clone)]
struct MemStore {
    metadata: Vec<u8>,
    buckets: HashMap<BucketObject, Vec<u8>>,
}

/// Flushes `index` into `store` the way the production adapter does:
/// buckets first, then the manifest commit, then best-effort deletion of
/// the obsolete objects.
async fn flush_to<PK, FV>(
    index: &BTreeIndex<PK, FV>,
    store: &mut MemStore,
    now_ms: u64,
) -> FlushOutcome
where
    PK: BTreeKey,
    FV: BTreeKey,
{
    let mut meta_buf: Vec<u8> = Vec::new();
    let buckets = &mut store.buckets;
    let outcome = index
        .flush_owned_with(
            now_ms,
            |data| {
                meta_buf = data;
                std::future::ready(Ok(()))
            },
            |object, data| {
                buckets.insert(object, data);
                std::future::ready(Ok(()))
            },
        )
        .await
        .unwrap();
    if outcome.saved {
        store.metadata = meta_buf;
        for object in &outcome.obsolete {
            store.buckets.remove(object);
        }
    }
    outcome
}

/// Loads a complete index from `store`.
async fn load_from<PK, FV>(store: &MemStore) -> BTreeIndex<PK, FV>
where
    PK: BTreeKey,
    FV: BTreeKey,
{
    BTreeIndex::load_all(&store.metadata[..], async |object| {
        Ok(store.buckets.get(&object).cloned())
    })
    .await
    .unwrap()
}

// 辅助函数：创建一个测试用的 B-tree 索引
fn create_test_index() -> BTreeIndex<u64, String> {
    let config = BTreeConfig {
        bucket_overload_size: 1024,
        allow_duplicates: true,
    };
    BTreeIndex::new("test_index".to_string(), Some(config))
}

// 辅助函数：创建一个测试用的 B-tree 索引并插入一些数据
fn create_populated_index() -> BTreeIndex<u64, String> {
    let index = create_test_index();

    // 插入一些测试数据
    let _ = index.insert(1, "apple".to_string(), now_ms());
    let _ = index.insert(2, "banana".to_string(), now_ms());
    let _ = index.insert(3, "cherry".to_string(), now_ms());
    let _ = index.insert(4, "date".to_string(), now_ms());
    let _ = index.insert(5, "eggplant".to_string(), now_ms());

    // 测试重复键
    let _ = index.insert(6, "apple".to_string(), now_ms());
    let _ = index.insert(7, "banana".to_string(), now_ms());

    index
}

fn encode_bucket(index: &BTreeIndex<u64, String>, bucket_id: u32) -> Vec<u8> {
    let bucket = index.buckets.get(&bucket_id).unwrap();
    let mut buf = Vec::new();
    cbor2::to_writer(
        &BucketView {
            index,
            id: bucket_id,
            fields: &bucket.fields,
        },
        &mut buf,
    )
    .unwrap();
    buf
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct TestKey(String);

/// A value whose serialization fails on demand (`.1 == true`), used to
/// exercise the non-panicking serialization error paths.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
struct Flaky(u8, bool);

impl Serialize for Flaky {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.1 {
            Err(serde::ser::Error::custom("flaky serialization failure"))
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl TryFrom<String> for TestKey {
    type Error = BoxError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == "bad" {
            Err("bad key".into())
        } else {
            Ok(TestKey(value))
        }
    }
}

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("writer failed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Process-wide capture of `warn!` records for tests that assert on
/// logging. `log::set_logger` succeeds once per process, so every such
/// test shares this logger and filters the captured messages by content.
struct CaptureLogger;
static CAPTURED_LOGS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
static CAPTURE_LOGGER: CaptureLogger = CaptureLogger;

impl log::Log for CaptureLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            CAPTURED_LOGS
                .lock()
                .unwrap()
                .push(record.args().to_string());
        }
    }
    fn flush(&self) {}
}

fn install_capture_logger() {
    // A second caller just reuses the installed logger; ignore the error.
    let _ = log::set_logger(&CAPTURE_LOGGER);
    log::set_max_level(log::LevelFilter::Warn);
}

/// Every live posting must be listed by exactly the bucket it points at
/// (stale listings of removed postings are tolerated drift).
fn assert_bucket_ownership<PK, FV>(index: &BTreeIndex<PK, FV>)
where
    PK: BTreeKey,
    FV: BTreeKey,
{
    for entry in index.postings.iter() {
        let bucket_id = entry.value().bucket_id;
        let bucket = index
            .buckets
            .get(&bucket_id)
            .unwrap_or_else(|| panic!("{:?} points at missing bucket {bucket_id}", entry.key()));
        assert!(
            bucket.fields.contains(entry.key()),
            "bucket {bucket_id} does not list {:?}",
            entry.key()
        );
    }
    for bucket in index.buckets.iter() {
        for fv in bucket.fields.iter() {
            if let Some(posting) = index.postings.get(fv) {
                assert_eq!(
                    posting.bucket_id,
                    *bucket.key(),
                    "{fv:?} listed by bucket {} but owned by bucket {}",
                    bucket.key(),
                    posting.bucket_id
                );
            }
        }
    }
}

#[test]
fn test_create_index() {
    let index = create_test_index();

    assert_eq!(index.name(), "test_index");
    assert_eq!(index.len(), 0);
    assert!(index.is_empty());

    let metadata = index.metadata();
    assert_eq!(metadata.name, "test_index");
    assert_eq!(metadata.stats.num_elements, 0);
}

#[test]
fn test_default_config_getters_and_query_private_helpers() {
    let default_config = BTreeConfig::default();
    assert_eq!(default_config.bucket_overload_size, 1024 * 512);
    assert!(default_config.allow_duplicates);

    let index = BTreeIndex::<u64, String>::new("defaulted".to_string(), None);
    assert_eq!(index.name(), "defaulted");
    assert!(index.allow_duplicates());
    assert!(index.has_pending_metadata_flush());
    assert!(!index.has_dirty_buckets());
    assert_eq!(index.keys(None, None), Vec::<String>::new());
    assert_eq!(
        index.range_query_with(RangeQuery::Ge("a".to_string()), |_, _| (true, vec![1_u64])),
        Vec::<u64>::new()
    );

    let populated = create_populated_index();
    assert_eq!(
        populated.keys(Some("apple".to_string()), Some(2)),
        vec!["banana".to_string(), "cherry".to_string()]
    );
    assert_eq!(
        populated.keys(Some("cherry".to_string()), None),
        vec!["date".to_string(), "eggplant".to_string()]
    );
    assert_eq!(
        populated.keys(None, Some(2)),
        vec!["apple".to_string(), "banana".to_string()]
    );

    let mut writer = FailingWriter;
    writer.flush().unwrap();
}

#[test]
fn test_range_query_try_convert_from_all_variants_and_errors() {
    let converted =
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Eq("a".to_string())).unwrap();
    assert!(matches!(converted, RangeQuery::Eq(TestKey(ref v)) if v == "a"));

    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Gt("a".to_string())).unwrap(),
        RangeQuery::Gt(TestKey(ref v)) if v == "a"
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Ge("a".to_string())).unwrap(),
        RangeQuery::Ge(TestKey(ref v)) if v == "a"
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Lt("a".to_string())).unwrap(),
        RangeQuery::Lt(TestKey(ref v)) if v == "a"
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Le("a".to_string())).unwrap(),
        RangeQuery::Le(TestKey(ref v)) if v == "a"
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Between(
            "a".to_string(),
            "z".to_string(),
        ))
        .unwrap(),
        RangeQuery::Between(TestKey(ref a), TestKey(ref z)) if a == "a" && z == "z"
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Include(vec![
            "a".to_string(),
            "b".to_string(),
        ]))
        .unwrap(),
        RangeQuery::Include(keys) if keys == vec![TestKey("a".to_string()), TestKey("b".to_string())]
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::And(vec![
            Box::new(RangeQuery::Ge("a".to_string())),
            Box::new(RangeQuery::Le("z".to_string())),
        ]))
        .unwrap(),
        RangeQuery::And(queries) if queries.len() == 2
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Or(vec![
            Box::new(RangeQuery::Eq("a".to_string())),
            Box::new(RangeQuery::Eq("b".to_string())),
        ]))
        .unwrap(),
        RangeQuery::Or(queries) if queries.len() == 2
    ));
    assert!(matches!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::Not(Box::new(RangeQuery::Eq(
            "a".to_string()
        ))))
        .unwrap(),
        RangeQuery::Not(_)
    ));

    assert!(RangeQuery::<TestKey>::try_convert_from(RangeQuery::Eq("bad".to_string())).is_err());
    assert!(
        RangeQuery::<TestKey>::try_convert_from(RangeQuery::And(vec![
            Box::new(RangeQuery::Eq("ok".to_string())),
            Box::new(RangeQuery::Eq("bad".to_string())),
        ]))
        .is_err()
    );
}

#[test]
fn test_range_query_with_early_stop_variants_and_prefix_empty() {
    let index = create_populated_index();

    for query in [
        RangeQuery::Eq("apple".to_string()),
        RangeQuery::Gt("apple".to_string()),
        RangeQuery::Ge("apple".to_string()),
        RangeQuery::Between("apple".to_string(), "date".to_string()),
        RangeQuery::Include(vec!["banana".to_string(), "date".to_string()]),
        RangeQuery::And(vec![
            Box::new(RangeQuery::Ge("apple".to_string())),
            Box::new(RangeQuery::Le("date".to_string())),
        ]),
        RangeQuery::Or(vec![
            Box::new(RangeQuery::Eq("apple".to_string())),
            Box::new(RangeQuery::Eq("date".to_string())),
        ]),
        RangeQuery::Not(Box::new(RangeQuery::Eq("apple".to_string()))),
    ] {
        let values =
            index.range_query_with(query, |key, ids| (false, vec![(key.clone(), ids.len())]));
        assert_eq!(values.len(), 1);
    }

    assert_eq!(
        index.range_query_with(
            RangeQuery::Between("z".to_string(), "a".to_string()),
            |key, _| (true, vec![key.clone()])
        ),
        Vec::<String>::new()
    );

    assert_eq!(
        index.range_keys(RangeQuery::Gt("cherry".to_string())),
        vec!["date".to_string(), "eggplant".to_string()]
    );
    assert_eq!(
        index.range_keys(RangeQuery::Include(vec![
            "missing".to_string(),
            "banana".to_string(),
        ])),
        vec!["banana".to_string()]
    );

    let all_prefix =
        index.prefix_query_with("", |key, ids| (false, Some((key.to_string(), ids.len()))));
    assert_eq!(all_prefix, vec![("apple".to_string(), 2)]);

    let empty = create_test_index();
    assert_eq!(
        empty.prefix_query_with("", |key, _| (true, Some(key.to_string()))),
        Vec::<String>::new()
    );
}

#[test]
fn test_insert() {
    let index = create_test_index();

    // 测试插入
    let result = index.insert(1, "apple".to_string(), now_ms());
    assert!(result.is_ok());
    assert!(result.unwrap());

    assert_eq!(index.len(), 1);
    assert!(!index.is_empty());

    // 测试重复插入相同的文档ID和字段值
    let result = index.insert(1, "apple".to_string(), now_ms());
    assert!(result.is_ok());
    assert!(!result.unwrap()); // 应该返回 false，因为没有实际插入新数据

    // 测试插入相同字段值但不同文档ID
    let result = index.insert(2, "apple".to_string(), now_ms());
    assert!(result.is_ok());
    assert!(result.unwrap());

    // 测试不允许重复键的情况
    let config = BTreeConfig {
        bucket_overload_size: 1024,
        allow_duplicates: false,
    };
    let unique_index = BTreeIndex::new("unique_index".to_string(), Some(config));

    let result = unique_index.insert(1, "apple".to_string(), now_ms());
    assert!(result.is_ok());

    // unique 索引：重复插入同一个 doc_id 应该是幂等的
    let result = unique_index.insert(1, "apple".to_string(), now_ms());
    assert!(result.is_ok());
    assert!(!result.unwrap());

    let result = unique_index.insert(2, "apple".to_string(), now_ms());
    assert!(result.is_err());
    match result {
        Err(BTreeError::AlreadyExists { .. }) => (),
        _ => panic!("Expected AlreadyExists error"),
    }
}

#[test]
fn test_insert_idempotent_does_not_update_stats() {
    let index = create_test_index();

    let inserted = index.insert(1, "apple".to_string(), now_ms()).unwrap();
    assert!(inserted);
    let stats_after_first = index.stats();

    let inserted = index.insert(1, "apple".to_string(), now_ms()).unwrap();
    assert!(!inserted);
    let stats_after_second = index.stats();

    assert_eq!(stats_after_first.insert_count, 1);
    assert_eq!(
        stats_after_second.insert_count,
        stats_after_first.insert_count
    );
    assert_eq!(stats_after_second.version, stats_after_first.version);
}

#[test]
fn test_remove() {
    let index = create_populated_index();

    // 测试删除存在的条目
    let result = index.remove(1, "apple".to_string(), now_ms());
    assert!(result);

    // 测试删除不存在的条目
    let result = index.remove(100, "nonexistent".to_string(), now_ms());
    assert!(!result);

    // key 存在但 doc_id 不存在：应该返回 false
    let result = index.remove(999, "banana".to_string(), now_ms());
    assert!(!result);

    // 测试删除后的搜索
    let result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_some());
    let ids = result.unwrap();
    assert!(!ids.contains(&1)); // ID 1 已被删除
    assert!(ids.contains(&6)); // ID 6 仍然存在

    // 测试删除所有相关文档后，键应该被完全移除
    let result = index.remove(6, "apple".to_string(), now_ms());
    assert!(result);

    let result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_none()); // 键应该已经被完全移除
}

#[test]
fn test_query() {
    let index = create_populated_index();

    // 测试精确搜索
    let result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_some());
    let ids = result.unwrap();
    assert!(ids.contains(&1));
    assert!(ids.contains(&6));

    // 测试搜索不存在的键
    let result = index.query_with(&"nonexistent".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_none());
}

#[test]
fn test_range_query() {
    let index = create_populated_index();
    let apple = "apple".to_string();
    let banana = "banana".to_string();
    let cherry = "cherry".to_string();
    let date = "date".to_string();
    let eggplant = "eggplant".to_string();

    // 测试等于查询
    let query = RangeQuery::Eq(apple.clone());
    let results = index.range_query_with(query, |k, ids| (true, vec![(k.clone(), ids.clone())]));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "apple");

    // 测试大于查询
    let query = RangeQuery::Gt(cherry.clone());
    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 2);
    assert!(results.contains(&"date".to_string()));
    assert!(results.contains(&"eggplant".to_string()));

    // 测试大于等于查询
    let query = RangeQuery::Ge(cherry.clone());
    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 3);
    assert!(results.contains(&"cherry".to_string()));

    // 测试小于查询
    let query = RangeQuery::Lt(cherry.clone());
    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 2);
    assert!(results.contains(&apple));
    assert!(results.contains(&banana));

    // 测试小于等于查询
    let query = RangeQuery::Le(cherry.clone());
    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 3);
    assert!(results.contains(&cherry));

    // 测试范围查询
    let query = RangeQuery::Between(banana.clone(), date.clone());
    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 3);
    assert!(results.contains(&banana));
    assert!(results.contains(&cherry));
    assert!(results.contains(&date));

    // 测试包含查询
    let keys = vec![apple.clone(), eggplant.clone()];
    let query = RangeQuery::Include(keys);
    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 2);
    assert!(results.contains(&apple));
    assert!(results.contains(&eggplant));

    // 测试提前终止搜索
    let query = RangeQuery::Ge(apple.clone());
    let results = index.range_query_with(query, |k, _| (k != "banana", vec![k.clone()]));
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], "apple");
    assert_eq!(results[1], "banana");
}

#[test]
fn test_logical_queries() {
    let index = create_populated_index();

    // 额外插入一些测试数据以丰富测试用例
    let _ = index.insert(8, "grape".to_string(), now_ms());
    let _ = index.insert(9, "fig".to_string(), now_ms());
    let _ = index.insert(10, "berry".to_string(), now_ms());
    let _ = index.insert(11, "berry".to_string(), now_ms());

    // 准备常用的查询键
    let apple = "apple".to_string();
    let banana = "banana".to_string();
    let berry = "berry".to_string();
    let cherry = "cherry".to_string();
    let date = "date".to_string();
    let eggplant = "eggplant".to_string();
    let fig = "fig".to_string();
    let grape = "grape".to_string();

    // ===== 测试 AND 操作 =====
    // 测试两个有交集的范围的 AND 操作
    let query = RangeQuery::And(vec![
        Box::new(RangeQuery::Le(date.clone())), // <= date (apple, banana, cherry, date)
        Box::new(RangeQuery::Ge(cherry.clone())), // >= cherry (cherry, date, eggplant, fig, grape)
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 2);
    assert!(results.contains(&cherry));
    assert!(results.contains(&date));

    // 测试空交集的 AND 操作
    let query = RangeQuery::And(vec![
        Box::new(RangeQuery::Lt(cherry.clone())), // < cherry (apple, banana)
        Box::new(RangeQuery::Gt(date.clone())),   // > date (eggplant, fig, grape)
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 0); // 应该为空集

    // 测试精确匹配和范围查询的 AND 操作
    let query = RangeQuery::And(vec![
        Box::new(RangeQuery::Ge(banana.clone())),   // >= banana
        Box::new(RangeQuery::Lt(eggplant.clone())), // < eggplant
        Box::new(RangeQuery::Eq(cherry.clone())),   // == cherry
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 1);
    assert!(results.contains(&cherry));

    // ===== 测试 OR 操作 =====
    // 测试两个不相交范围的 OR 操作
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Le(banana.clone())), // <= banana (apple, banana)
        Box::new(RangeQuery::Ge(fig.clone())),    // >= fig (fig, grape)
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 4);
    assert!(results.contains(&apple));
    assert!(results.contains(&banana));
    assert!(results.contains(&fig));
    assert!(results.contains(&grape));

    // 测试有重叠的 OR 操作
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Between(banana.clone(), date.clone())), // banana到date
        Box::new(RangeQuery::Between(cherry.clone(), fig.clone())),  // cherry到fig
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(results.len(), 6);
    assert!(results.contains(&banana));
    assert!(results.contains(&berry));
    assert!(results.contains(&cherry));
    assert!(results.contains(&date));
    assert!(results.contains(&eggplant));
    assert!(results.contains(&fig));

    // ===== 测试 NOT 操作 =====
    // 测试基本的 NOT 操作
    let query = RangeQuery::Not(Box::new(RangeQuery::Between(
        cherry.clone(),
        eggplant.clone(),
    )));

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert!(results.contains(&apple));
    assert!(results.contains(&banana));
    assert!(results.contains(&fig));
    assert!(results.contains(&grape));
    assert!(!results.contains(&cherry));
    assert!(!results.contains(&date));
    assert!(!results.contains(&eggplant));

    // 测试 NOT + Eq 操作
    let query = RangeQuery::Not(Box::new(RangeQuery::Eq(apple.clone())));

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert!(!results.contains(&apple));
    assert!(results.contains(&banana));
    assert!(results.contains(&cherry));
    // ...验证其它键

    // ===== 测试复合逻辑查询 =====
    // 测试 AND(OR, OR) 复杂嵌套
    let query = RangeQuery::And(vec![
        Box::new(RangeQuery::Or(vec![
            Box::new(RangeQuery::Le(cherry.clone())), // <= cherry
            Box::new(RangeQuery::Ge(fig.clone())),    // >= fig
        ])),
        Box::new(RangeQuery::Or(vec![
            Box::new(RangeQuery::Le(banana.clone())),   // <= banana
            Box::new(RangeQuery::Ge(eggplant.clone())), // >= eggplant
        ])),
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert!(results.contains(&apple));
    assert!(results.contains(&banana));
    assert!(results.contains(&fig));
    assert!(results.contains(&grape));
    assert!(!results.contains(&cherry));
    assert!(!results.contains(&date));

    // 测试 OR(NOT, NOT) 复杂嵌套
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Not(Box::new(RangeQuery::Ge(date.clone())))), // NOT >= date
        Box::new(RangeQuery::Not(Box::new(RangeQuery::Le(cherry.clone())))), // NOT <= cherry
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    // 这应该返回所有键，因为每个键要么 < date 要么 > cherry
    assert_eq!(results.len(), index.len());

    // 测试 NOT(AND) 复合操作
    let query = RangeQuery::Not(Box::new(RangeQuery::And(vec![
        Box::new(RangeQuery::Ge(cherry.clone())),   // >= cherry
        Box::new(RangeQuery::Le(eggplant.clone())), // <= eggplant
    ])));

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert!(results.contains(&apple));
    assert!(results.contains(&banana));
    assert!(results.contains(&fig));
    assert!(results.contains(&grape));
    assert!(!results.contains(&cherry));
    assert!(!results.contains(&date));
    assert!(!results.contains(&eggplant));

    // 测试提前终止功能
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Ge(apple.clone())),
        Box::new(RangeQuery::Le(grape.clone())),
    ]);

    let mut count = 0;
    let results = index.range_query_with(query, |_, _| {
        count += 1;
        (count < 3, vec![count.to_string()])
    });

    assert_eq!(results.len(), 3);
    assert_eq!(count, 3); // 确认查询在第三项后停止
}

#[test]
fn test_range_query_lt_le_full_order() {
    let index = create_populated_index();
    // keys: apple < banana < cherry < date < eggplant

    // Lt(date) -> apple, banana, cherry (正序)
    let results = index.range_query_with(RangeQuery::Lt("date".to_string()), |k, _| {
        (true, vec![k.clone()])
    });
    assert_eq!(results, vec!["apple", "banana", "cherry"]);

    // Le(date) -> apple, banana, cherry, date (正序)
    let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
        (true, vec![k.clone()])
    });
    assert_eq!(results, vec!["apple", "banana", "cherry", "date"]);
}

#[test]
fn test_range_query_lt_le_with_early_stop_limit_semantics() {
    let index = create_populated_index();
    // 方向由调用方选择，而不是由查询形态推断：同一个 Lt/Le 查询配同一个
    // 上限，正向扫描取最小的若干个 key，反向扫描取最大的若干个，两者的
    // 输出都是正序。

    // 反向：Lt(date) 倒序遍历 cherry, banana, apple，截断 2 个
    // => [cherry, banana]，最终正序 [banana, cherry]
    let mut count = 0usize;
    let results = index.range_query_rev_with(RangeQuery::Lt("date".to_string()), |k, _| {
        count += 1;
        (count < 2, vec![k.clone()])
    });
    assert_eq!(results, vec!["banana", "cherry"]);

    // 正向：同样的 Lt(date) 从最小 key 开始，截断 2 个 => [apple, banana]
    let mut count = 0usize;
    let results = index.range_query_with(RangeQuery::Lt("date".to_string()), |k, _| {
        count += 1;
        (count < 2, vec![k.clone()])
    });
    assert_eq!(results, vec!["apple", "banana"]);

    // 反向：Le(date) 倒序遍历 date, cherry, banana, apple，截断 2 个
    // => [date, cherry]，最终正序 [cherry, date]
    let mut count = 0usize;
    let results = index.range_query_rev_with(RangeQuery::Le("date".to_string()), |k, _| {
        count += 1;
        (count < 2, vec![k.clone()])
    });
    assert_eq!(results, vec!["cherry", "date"]);

    // 正向：Le(date) 截断 2 个 => [apple, banana]
    let mut count = 0usize;
    let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
        count += 1;
        (count < 2, vec![k.clone()])
    });
    assert_eq!(results, vec!["apple", "banana"]);

    // 反向扫描同样适用于 Gt/Ge/Between：Ge(banana) 取最大的 2 个
    // （banana, cherry, date, eggplant 中最大的两个）
    let mut count = 0usize;
    let results = index.range_query_rev_with(RangeQuery::Ge("banana".to_string()), |k, _| {
        count += 1;
        (count < 2, vec![k.clone()])
    });
    assert_eq!(results, vec!["date", "eggplant"]);

    // 再测试当“上限”大于可返回数量时，两个方向都返回全部（正序）
    let mut count = 0usize;
    let results = index.range_query_with(RangeQuery::Lt("banana".to_string()), |k, _| {
        count += 1;
        (count < 10, vec![k.clone()])
    });
    assert_eq!(results, vec!["apple"]);

    let mut count = 0usize;
    let results = index.range_query_rev_with(RangeQuery::Lt("banana".to_string()), |k, _| {
        count += 1;
        (count < 10, vec![k.clone()])
    });
    assert_eq!(results, vec!["apple"]);
}

#[test]
fn test_range_query_lt_le_group_order_preserved() {
    let index = create_populated_index();
    // 反向遍历后“组内顺序”保持，并最终整体正序。
    // 取 Lt(date) 最大的 2 个 key：banana, cherry，最终顺序应为：
    // banana-1, banana-2, cherry-1, cherry-2

    let mut count = 0usize;
    let results = index.range_query_rev_with(RangeQuery::Lt("date".to_string()), |k, _| {
        count += 1;
        let v = vec![format!("{k}-1"), format!("{k}-2")];
        (count < 2, v)
    });
    assert_eq!(
        results,
        vec![
            "banana-1".to_string(),
            "banana-2".to_string(),
            "cherry-1".to_string(),
            "cherry-2".to_string()
        ]
    );

    // Le(date) 最大的 2 个：cherry, date，组内顺序保持：
    // cherry-1, cherry-2, date-1, date-2
    let mut count = 0usize;
    let results = index.range_query_rev_with(RangeQuery::Le("date".to_string()), |k, _| {
        count += 1;
        let v = vec![format!("{k}-1"), format!("{k}-2")];
        (count < 2, v)
    });
    assert_eq!(
        results,
        vec![
            "cherry-1".to_string(),
            "cherry-2".to_string(),
            "date-1".to_string(),
            "date-2".to_string()
        ]
    );

    // 正向遍历取最小的 2 个 key，组内顺序同样保持
    let mut count = 0usize;
    let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
        count += 1;
        let v = vec![format!("{k}-1"), format!("{k}-2")];
        (count < 2, v)
    });
    assert_eq!(
        results,
        vec![
            "apple-1".to_string(),
            "apple-2".to_string(),
            "banana-1".to_string(),
            "banana-2".to_string()
        ]
    );
}

#[test]
fn test_range_keys() {
    let index = create_populated_index();

    // 测试 range_keys 方法处理 And 逻辑
    let apple = "apple".to_string();
    let banana = "banana".to_string();
    let cherry = "cherry".to_string();
    let eggplant = "eggplant".to_string();

    let query = RangeQuery::And(vec![
        Box::new(RangeQuery::Ge(banana.clone())),
        Box::new(RangeQuery::Le(cherry.clone())),
    ]);

    let keys = index.range_keys(query);
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&banana));
    assert!(keys.contains(&cherry));

    // 测试 range_keys 方法处理 Or 逻辑
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Eq(apple.clone())),
        Box::new(RangeQuery::Eq(eggplant.clone())),
    ]);

    let keys = index.range_keys(query);
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&apple));
    assert!(keys.contains(&eggplant));

    // 测试 range_keys 方法处理 Not 逻辑
    let query = RangeQuery::Not(Box::new(RangeQuery::Eq(apple.clone())));

    let keys = index.range_keys(query);
    assert!(!keys.contains(&apple));
    assert!(keys.contains(&banana));
    assert!(keys.contains(&cherry));
}

#[test]
fn test_range_keys_invalid_between_inside_logical_queries() {
    let index = create_populated_index();

    let invalid_between = RangeQuery::Between("date".to_string(), "banana".to_string());

    let results = index.range_query_with(
        RangeQuery::Or(vec![
            Box::new(invalid_between.clone()),
            Box::new(RangeQuery::Eq("apple".to_string())),
        ]),
        |key, _| (true, vec![key.clone()]),
    );
    assert_eq!(results, vec!["apple"]);

    let results = index.range_query_with(
        RangeQuery::And(vec![
            Box::new(RangeQuery::Ge("apple".to_string())),
            Box::new(invalid_between.clone()),
        ]),
        |key, _| (true, vec![key.clone()]),
    );
    assert!(results.is_empty());

    let results = index.range_query_with(RangeQuery::Not(Box::new(invalid_between)), |key, _| {
        (true, vec![key.clone()])
    });
    assert_eq!(results, index.keys(None, None));
}

#[test]
fn test_prefix_query() {
    let index = create_populated_index();

    // 插入一些带前缀的数据
    let _ = index.insert(10, "app".to_string(), now_ms());
    let _ = index.insert(11, "application".to_string(), now_ms());

    // 测试前缀搜索
    let results = index.prefix_query_with("app", |k, _| (true, Some(k.to_string())));
    assert_eq!(results.len(), 3);
    assert!(results.contains(&"app".to_string()));
    assert!(results.contains(&"apple".to_string()));
    assert!(results.contains(&"application".to_string()));

    // 测试提前终止搜索
    let results = index.prefix_query_with("app", |k, _| (k != "apple", Some(k.to_string())));
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], "app");
    assert_eq!(results[1], "apple");
}

#[tokio::test]
async fn test_serialization() {
    let index = create_populated_index();

    // 通过 manifest flush 持久化元数据与全部脏桶
    let mut store = MemStore::default();
    let outcome = flush_to(&index, &mut store, now_ms()).await;
    assert!(outcome.saved);

    println!("Serialized metadata: {:?}", hex::encode(&store.metadata));

    // 重新加载
    let loaded_index: BTreeIndex<u64, String> = load_from(&store).await;

    // 验证加载后的索引
    assert_eq!(loaded_index.name(), "test_index");
    assert_eq!(loaded_index.len(), index.len());

    // 测试搜索
    let result = loaded_index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_some());
    let ids = result.unwrap();
    assert!(ids.contains(&1));
    assert!(ids.contains(&6));
}

/// A dirty bucket always forces a manifest commit — bucket objects are
/// unreachable until the metadata references them. This covers the
/// load-time repair path, which marks buckets dirty without bumping the
/// stats version.
#[tokio::test]
async fn test_flush_commits_manifest_even_if_metadata_version_unchanged() {
    let index = create_test_index();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, now_ms()).await;
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_buckets());

    // Simulate a load-time repair: dirty bucket, no version bump.
    {
        let mut bucket = index.buckets.get_mut(&0).expect("bucket 0 exists");
        index.mark_bucket_dirty(&mut bucket);
    }
    assert!(!index.has_pending_metadata_flush());
    assert!(index.has_dirty_buckets());

    let before = store.clone();
    let outcome = flush_to(&index, &mut store, now_ms()).await;
    assert!(outcome.saved);
    assert!(!index.has_dirty_buckets());
    assert!(!index.has_pending_metadata_flush());
    assert_ne!(
        before.metadata, store.metadata,
        "the manifest commit must rewrite the metadata"
    );

    let reloaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(reloaded.len(), index.len());
}

#[tokio::test]
async fn test_flush_propagates_bucket_write_error_and_commits_nothing() {
    let index = create_test_index();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();
    assert!(index.has_dirty_buckets());

    let mut meta_buf = Vec::new();
    let err = index
        .flush(&mut meta_buf, now_ms(), |_, _| {
            std::future::ready(Err::<(), BoxError>("write failed".into()))
        })
        .await
        .unwrap_err();

    match err {
        BTreeError::Generic { .. } => {}
        other => panic!("Expected Generic error, got: {other:?}"),
    }

    assert!(meta_buf.is_empty(), "no manifest commit on bucket failure");
    assert!(index.has_dirty_buckets());
    assert!(index.has_pending_metadata_flush());
}

#[tokio::test]
async fn test_migrated_source_bucket_is_persisted_to_prevent_resurrection() {
    let config = BTreeConfig {
        bucket_overload_size: 80,
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("resurrection_test".to_string(), Some(config));
    let mut store = MemStore::default();

    // Step 1: initial data persisted in bucket 0. `anchor` shares the
    // bucket so that `apple` migrates once the bucket is full instead of
    // growing in place as a sole occupant would.
    index.insert(1, "anchor".to_string(), now_ms()).unwrap();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();
    flush_to(&index, &mut store, now_ms()).await;

    // Step 2: force migration of "apple" to a new bucket.
    let mut doc_id = 2u64;
    while index.stats().max_bucket_id == 0 && doc_id < 200 {
        index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
        doc_id += 1;
    }
    assert!(index.stats().max_bucket_id > 0);
    flush_to(&index, &mut store, now_ms()).await;

    // Step 3: remove all docs for "apple", persist again.
    for id in 1..doc_id {
        index.remove(id, "apple".to_string(), now_ms());
    }
    assert!(
        index
            .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
            .is_none()
    );
    flush_to(&index, &mut store, now_ms()).await;

    // Step 4: reload and verify no resurrection.
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert!(
        loaded
            .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
            .is_none(),
        "apple should not resurrect from a stale bucket object"
    );
}

#[tokio::test]
async fn test_legacy_load_reconciles_stale_source_bucket_duplicate() {
    let config = BTreeConfig {
        bucket_overload_size: 80,
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("partial_migration_flush".to_string(), Some(config));

    // Persist the pre-migration state (apple lives in bucket 0, shared
    // with `anchor` so that it migrates rather than growing in place).
    index.insert(1, "anchor".to_string(), now_ms()).unwrap();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, now_ms()).await;
    let stale_bucket0 = store
        .buckets
        .values()
        .next()
        .expect("bucket 0 must be persisted")
        .clone();

    // Migrate "apple" out of bucket 0 in memory.
    let mut doc_id = 2u64;
    while index.stats().max_bucket_id == 0 && doc_id < 200 {
        index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
        doc_id += 1;
    }
    let apple = "apple".to_string();
    let migrated_bucket_id = index.postings.get(&apple).unwrap().bucket_id;
    assert!(
        migrated_bucket_id > 0,
        "apple should migrate out of bucket 0"
    );

    // Craft the legacy (pre-manifest) crash layout: metadata without a
    // manifest, the stale bucket 0 object, and the migrated destination
    // bucket — the old protocol could crash in exactly this state.
    let mut legacy_meta = index.metadata();
    legacy_meta.buckets = BTreeMap::new();
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &legacy_meta,
        },
        &mut metadata_buf,
    )
    .unwrap();
    let legacy_store = MemStore {
        metadata: metadata_buf,
        buckets: HashMap::from_iter([
            (
                BucketObject {
                    bucket_id: 0,
                    generation: 0,
                },
                stale_bucket0,
            ),
            (
                BucketObject {
                    bucket_id: migrated_bucket_id,
                    generation: 0,
                },
                encode_bucket(&index, migrated_bucket_id),
            ),
        ]),
    };

    let loaded: BTreeIndex<u64, String> = load_from(&legacy_store).await;
    assert!(
        loaded.has_dirty_buckets(),
        "stale source bucket needs repair"
    );
    assert_eq!(
        loaded.postings.get(&apple).unwrap().bucket_id,
        migrated_bucket_id,
        "the higher-numbered legacy bucket must win"
    );

    // The repair flush upgrades to the manifest format; a reload keeps
    // the reconciled ownership.
    let mut repaired = legacy_store.clone();
    let outcome = flush_to(&loaded, &mut repaired, now_ms()).await;
    assert!(outcome.saved);

    let reloaded: BTreeIndex<u64, String> = load_from(&repaired).await;
    assert_eq!(
        reloaded.postings.get(&apple).unwrap().bucket_id,
        migrated_bucket_id
    );
    assert!(!reloaded.has_dirty_buckets());
}

/// The manifest commit must be the last write of a flush: every dirty
/// bucket object precedes the metadata, and each is written to a fresh
/// generation-suffixed object.
#[tokio::test]
async fn test_flush_writes_all_buckets_before_manifest_commit() {
    let config = BTreeConfig {
        bucket_overload_size: 80,
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("ordered_migration_flush".to_string(), Some(config));
    let mut store = MemStore::default();
    // `anchor` shares bucket 0 so that `apple` migrates once it is full.
    index.insert(1, "anchor".to_string(), now_ms()).unwrap();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();
    flush_to(&index, &mut store, now_ms()).await;

    // Dirty both the old bucket and freshly-allocated migration targets.
    let mut doc_id = 2u64;
    while index.stats().max_bucket_id == 0 && doc_id < 200 {
        index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
        doc_id += 1;
    }
    assert!(index.stats().max_bucket_id > 0);

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Bucket(u32),
        Metadata,
    }
    let events = std::cell::RefCell::new(Vec::<Event>::new());
    index
        .flush_owned_with(
            now_ms(),
            |_data| {
                events.borrow_mut().push(Event::Metadata);
                std::future::ready(Ok(()))
            },
            |object, _data| {
                assert!(
                    object.generation > 0,
                    "bucket writes must target generation-suffixed objects"
                );
                events.borrow_mut().push(Event::Bucket(object.bucket_id));
                std::future::ready(Ok(()))
            },
        )
        .await
        .unwrap();

    let events = events.into_inner();
    assert!(
        events.len() > 1,
        "expected bucket writes and one metadata write: {events:?}"
    );
    assert_eq!(
        events.last(),
        Some(&Event::Metadata),
        "the manifest commit must come last: {events:?}"
    );
    assert_eq!(
        events.iter().filter(|e| **e == Event::Metadata).count(),
        1,
        "exactly one metadata write: {events:?}"
    );
}

/// A crash after some (or all) new-generation bucket objects are written
/// but before the manifest commit must leave the previous snapshot fully
/// intact — the new objects are unreferenced garbage — and a retry must
/// converge.
#[tokio::test]
async fn test_flush_crash_before_manifest_commit_keeps_previous_snapshot() {
    let config = BTreeConfig {
        bucket_overload_size: 80,
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("crash_before_commit".to_string(), Some(config));
    let mut store = MemStore::default();
    // `anchor` shares bucket 0 so that `apple` migrates once it is full.
    index.insert(1, "anchor".to_string(), now_ms()).unwrap();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();
    flush_to(&index, &mut store, now_ms()).await;

    // Migrate "apple" and add new keys, then crash mid-flush: the first
    // bucket write succeeds, the second fails, the metadata is never
    // written.
    let mut doc_id = 2u64;
    while index.stats().max_bucket_id == 0 && doc_id < 200 {
        index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
        doc_id += 1;
    }
    index
        .insert(doc_id, "banana".to_string(), now_ms())
        .unwrap();

    let mut crashed = store.clone();
    let mut metadata_written = false;
    {
        let buckets = &mut crashed.buckets;
        let mut writes = 0usize;
        let err = index
            .flush_owned_with(
                now_ms(),
                |_data| {
                    metadata_written = true;
                    std::future::ready(Ok(()))
                },
                |object, data| {
                    if writes >= 1 {
                        return std::future::ready(Err::<(), BoxError>(
                            "crash after first bucket".into(),
                        ));
                    }
                    writes += 1;
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BTreeError::Generic { .. }));
    }
    assert!(
        !metadata_written,
        "the manifest must not be committed when a bucket write fails"
    );

    // Reload from the old manifest plus orphaned objects: the previous
    // snapshot is complete, the uncommitted mutations are invisible.
    let loaded: BTreeIndex<u64, String> = load_from(&crashed).await;
    let apple_ids = loaded
        .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
        .expect("apple must survive the crash");
    assert_eq!(apple_ids, vec![1], "only the committed posting is visible");
    assert!(
        loaded
            .query_with(&"banana".to_string(), |ids| Some(ids.clone()))
            .is_none(),
        "uncommitted key must stay invisible"
    );

    // The retry persists one complete new snapshot.
    let mut recovered_store = crashed;
    assert!(flush_to(&index, &mut recovered_store, now_ms()).await.saved);
    let recovered: BTreeIndex<u64, String> = load_from(&recovered_store).await;
    assert_eq!(recovered.len(), index.len());
    let apple_ids = recovered
        .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
        .expect("apple must be present after recovery");
    assert_eq!(apple_ids.len() as u64, doc_id - 1);
    assert!(
        recovered
            .query_with(&"banana".to_string(), |ids| Some(ids.clone()))
            .is_some()
    );
}

/// After the manifest commit, the replaced objects are garbage. A crash
/// (or plain failure) before they are deleted must not affect reloads:
/// the manifest never references them.
#[tokio::test]
async fn test_reload_unaffected_when_obsolete_deletion_fails() {
    let index = create_populated_index();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, now_ms()).await;

    // Mutate and flush again, but "crash" between the manifest commit
    // and the cleanup: keep every obsolete object in the store.
    index.insert(100, "fig".to_string(), now_ms()).unwrap();
    let outcome;
    let mut meta_buf: Vec<u8> = Vec::new();
    {
        let buckets = &mut store.buckets;
        outcome = index
            .flush_owned_with(
                now_ms(),
                |data| {
                    meta_buf = data;
                    std::future::ready(Ok(()))
                },
                |object, data| {
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();
    }
    assert!(outcome.saved);
    assert!(
        !outcome.obsolete.is_empty(),
        "the rewritten bucket's previous object must be reported obsolete"
    );
    for object in &outcome.obsolete {
        assert!(
            store.buckets.contains_key(object),
            "test setup: obsolete {object:?} must still exist in the store"
        );
    }
    store.metadata = meta_buf;

    // Reload with the leaked garbage still present: invisible.
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(loaded.len(), index.len());
    let ids = loaded
        .query_with(&"fig".to_string(), |ids| Some(ids.clone()))
        .expect("fig must be present");
    assert_eq!(ids, vec![100]);
    assert!(
        !loaded.has_dirty_buckets(),
        "leaked garbage must not dirty anything on load"
    );
}

/// Data persisted by a pre-manifest release (metadata without a manifest,
/// un-suffixed bucket objects) loads correctly, and the first flush
/// upgrades the durable layout to the manifest format while retiring the
/// rewritten legacy objects.
#[tokio::test]
async fn test_legacy_format_loads_and_upgrades_on_first_flush() {
    let config = BTreeConfig {
        bucket_overload_size: 80,
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("legacy_upgrade".to_string(), Some(config));
    for (id, key) in [
        (1u64, "apple"),
        (2, "banana"),
        (3, "cherry"),
        (4, "date"),
        (5, "eggplant"),
        (6, "fig"),
        (7, "grape"),
    ] {
        index.insert(id, key.to_string(), now_ms()).unwrap();
    }
    let mut store = MemStore::default();
    flush_to(&index, &mut store, now_ms()).await;
    assert!(
        store.buckets.len() > 1,
        "scenario must span several buckets"
    );

    // Transform the store into the legacy layout: strip the manifest from
    // the metadata and re-key every bucket object to generation 0.
    let mut legacy_meta = index.metadata();
    legacy_meta.buckets = BTreeMap::new();
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &legacy_meta,
        },
        &mut metadata_buf,
    )
    .unwrap();
    let legacy_store = MemStore {
        metadata: metadata_buf,
        buckets: store
            .buckets
            .iter()
            .map(|(object, data)| {
                (
                    BucketObject {
                        bucket_id: object.bucket_id,
                        generation: 0,
                    },
                    data.clone(),
                )
            })
            .collect(),
    };

    // Legacy data loads through the bucket-id-scan path.
    let loaded: BTreeIndex<u64, String> = load_from(&legacy_store).await;
    assert_eq!(loaded.len(), index.len());
    for (id, key) in [(1u64, "apple"), (7, "grape")] {
        let ids = loaded
            .query_with(&key.to_string(), |ids| Some(ids.clone()))
            .unwrap_or_else(|| panic!("{key} must load from the legacy layout"));
        assert!(ids.contains(&id));
    }

    // Mutate and flush: the durable metadata upgrades to the manifest
    // format; rewritten legacy objects are reported obsolete.
    loaded.insert(8, "honeydew".to_string(), now_ms()).unwrap();
    let mut upgraded = legacy_store.clone();
    let outcome = flush_to(&loaded, &mut upgraded, now_ms()).await;
    assert!(outcome.saved);
    for object in &outcome.obsolete {
        assert_eq!(
            object.generation, 0,
            "only replaced legacy objects may be obsolete here"
        );
    }

    let upgraded_meta = BTreeIndex::<u64, String>::load_metadata(&upgraded.metadata[..])
        .unwrap()
        .metadata();
    assert!(
        !upgraded_meta.buckets.is_empty(),
        "the first flush must commit a manifest"
    );

    let reloaded: BTreeIndex<u64, String> = load_from(&upgraded).await;
    assert_eq!(reloaded.len(), 8);
    for key in ["apple", "grape", "honeydew"] {
        assert!(
            reloaded
                .query_with(&key.to_string(), |ids| Some(ids.clone()))
                .is_some(),
            "{key} must be present after the format upgrade"
        );
    }
}

#[test]
fn test_insert_refuses_metadata_only_index() {
    let meta = BTreeMetadata {
        name: "loaded_index".to_string(),
        config: BTreeConfig {
            bucket_overload_size: 1024,
            allow_duplicates: true,
        },
        stats: BTreeStats {
            version: 1,
            max_bucket_id: 3,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };

    let owned = BTreeIndexRef { metadata: &meta };
    let mut buf = Vec::new();
    cbor2::to_writer(&owned, &mut buf).unwrap();

    let index = BTreeIndex::<u64, String>::load_metadata(&buf[..]).unwrap();
    let result = index.insert(1, "apple".to_string(), now_ms());
    assert!(result.is_err());
}

#[tokio::test]
async fn test_insert_hot_posting_grows_in_place_after_isolation() {
    // Regression: a posting that alone exceeded the bucket limit used to
    // be migrated to a fresh bucket on every append, leaving an empty
    // bucket behind each time (500 appends produced ~470 buckets).
    let index = BTreeIndex::<u64, String>::new(
        "hot_insert".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: 64,
            allow_duplicates: true,
        }),
    );
    index.insert(0, "cold".to_string(), now_ms()).unwrap();
    for i in 0..500u64 {
        assert!(index.insert(i, "hot".to_string(), now_ms()).unwrap());
    }

    // The hot posting left the shared bucket exactly once, then grew in
    // place.
    assert_eq!(index.stats().max_bucket_id, 1);
    assert_eq!(index.buckets.len(), 2);
    let hot_bucket = index.postings.get(&"hot".to_string()).unwrap().bucket_id;
    let cold_bucket = index.postings.get(&"cold".to_string()).unwrap().bucket_id;
    assert_ne!(hot_bucket, cold_bucket);
    assert_eq!(index.buckets.get(&hot_bucket).unwrap().fields.len(), 1);
    assert!(index.buckets.get(&cold_bucket).unwrap().size < 64);
    assert_bucket_ownership(&index);

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    assert_eq!(store.buckets.len(), 2);
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(
        loaded.query_with(&"hot".to_string(), |ids| Some(ids.len())),
        Some(500)
    );
    assert_eq!(
        loaded.query_with(&"cold".to_string(), |ids| Some(ids.len())),
        Some(1)
    );
}

#[tokio::test]
async fn test_insert_array_isolates_hot_posting_from_shared_bucket() {
    // `insert_array` used to never move an existing posting, so a hot
    // posting kept its (cold) neighbours in the same ever-growing bucket
    // and dragged them into every rewrite. It now leaves a shared bucket
    // once and then grows in place, exactly like `insert`.
    let index = BTreeIndex::<u64, String>::new(
        "hot_insert_array".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: 64,
            allow_duplicates: true,
        }),
    );
    index
        .insert_array(0, vec!["cold".to_string()], now_ms())
        .unwrap();
    for i in 0..500u64 {
        assert_eq!(
            index
                .insert_array(i, vec!["hot".to_string()], now_ms())
                .unwrap(),
            1
        );
    }

    assert_eq!(index.stats().max_bucket_id, 1);
    let hot_bucket = index.postings.get(&"hot".to_string()).unwrap().bucket_id;
    let cold_bucket = index.postings.get(&"cold".to_string()).unwrap().bucket_id;
    assert_ne!(
        hot_bucket, cold_bucket,
        "the hot posting must leave the shared bucket"
    );
    assert_eq!(index.buckets.get(&hot_bucket).unwrap().fields.len(), 1);
    // The cold bucket reclaimed the migrated posting's size.
    assert!(index.buckets.get(&cold_bucket).unwrap().size < 64);
    assert_bucket_ownership(&index);

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(
        loaded.query_with(&"hot".to_string(), |ids| Some(ids.len())),
        Some(500)
    );
    assert_eq!(
        loaded.query_with(&"cold".to_string(), |ids| Some(ids.len())),
        Some(1)
    );
    assert_bucket_ownership(&loaded);
}

#[tokio::test]
async fn test_load_buckets_rejects_corrupted_legacy_max_bucket_id() {
    fn legacy_metadata(max_bucket_id: u32) -> Vec<u8> {
        let metadata = BTreeMetadata {
            name: "legacy_cap".to_string(),
            config: BTreeConfig::default(),
            stats: BTreeStats {
                version: 1,
                max_bucket_id,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut buf = Vec::new();
        cbor2::to_writer(
            &LegacyIndexRef {
                metadata: &metadata,
            },
            &mut buf,
        )
        .unwrap();
        buf
    }

    let mut probes = 0u32;
    let mut index: BTreeIndex<u64, String> =
        BTreeIndex::load_metadata(&legacy_metadata(u32::MAX)[..]).unwrap();
    let err = index
        .load_buckets(async |_| {
            probes += 1;
            Ok(None)
        })
        .await
        .unwrap_err();
    assert!(matches!(err, BTreeError::Generic { .. }), "{err:?}");
    assert!(err.to_string().contains("corrupted"), "{err}");
    assert_eq!(probes, 0, "a corrupted watermark must not be probed");

    // A watermark inside the range is still probed exhaustively.
    let mut index: BTreeIndex<u64, String> =
        BTreeIndex::load_metadata(&legacy_metadata(3)[..]).unwrap();
    index
        .load_buckets(async |_| {
            probes += 1;
            Ok(None)
        })
        .await
        .unwrap();
    assert_eq!(probes, 4);
}

#[tokio::test]
async fn test_load_buckets_warns_on_missing_manifest_object() {
    install_capture_logger();
    let index = create_populated_index();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let objects: Vec<BucketObject> = store.buckets.keys().copied().collect();
    assert!(!objects.is_empty());

    // Every bucket object is gone: loading must still succeed (read-only
    // partial loads are allowed) but must say so.
    let mut lossy = store.clone();
    lossy.buckets.clear();
    let mut loaded = BTreeIndex::<u64, String>::load_metadata(&lossy.metadata[..]).unwrap();
    loaded
        .load_buckets_partial(async |object| Ok(lossy.buckets.get(&object).cloned()))
        .await
        .unwrap();
    assert_eq!(loaded.len(), 0);
    {
        let captured = CAPTURED_LOGS.lock().unwrap().clone();
        for object in &objects {
            assert!(
                captured.iter().any(|msg| {
                    msg.contains("referenced by the manifest")
                        && msg.contains(loaded.name())
                        && msg.contains(&format!("({}, {})", object.bucket_id, object.generation))
                }),
                "expected a missing-object warning for {object:?}, got: {captured:?}"
            );
        }
    }

    assert_eq!(loaded.load_state(), LoadState::Partial);
    let old_version = loaded.stats().version;
    assert!(loaded.insert(99, "blocked".into(), 2).is_err());
    assert_eq!(loaded.compact_buckets(), (objects.len(), objects.len()));
    assert!(
        loaded
            .flush(Vec::new(), 2, |_, _| std::future::ready(Ok(())))
            .await
            .is_err()
    );
    assert_eq!(loaded.stats().version, old_version);
}

#[tokio::test]
async fn test_flush_refuses_index_without_loaded_buckets() {
    let index = create_populated_index();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;

    let meta_only: BTreeIndex<u64, String> =
        BTreeIndex::load_metadata(&store.metadata[..]).unwrap();
    assert!(
        meta_only
            .flush_owned_with(
                2,
                |_| std::future::ready(Ok(())),
                |_, _| std::future::ready(Ok(()))
            )
            .await
            .is_err()
    );
    assert_eq!(meta_only.compact_buckets(), (1, 1));
    assert!(meta_only.insert(42, "zebra".into(), now_ms()).is_err());

    // Loading the buckets lifts the refusal.
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    loaded.insert(42, "zebra".to_string(), now_ms()).unwrap();
    let mut store2 = store.clone();
    assert!(flush_to(&loaded, &mut store2, 4).await.saved);
    let reloaded: BTreeIndex<u64, String> = load_from(&store2).await;
    assert_eq!(reloaded.len(), index.len() + 1);
}

#[tokio::test]
async fn test_flush_wrapper_flushes_the_metadata_writer() {
    struct FlushProbe {
        flushed: Arc<AtomicBool>,
    }

    impl Write for FlushProbe {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let index = create_populated_index();
    let flushed = Arc::new(AtomicBool::new(false));
    let outcome = index
        .flush(
            FlushProbe {
                flushed: flushed.clone(),
            },
            1,
            |_, _| std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    assert!(outcome.saved);
    assert!(
        flushed.load(Ordering::SeqCst),
        "flush() must flush the metadata writer at the commit point"
    );
}

#[test]
fn test_compact_buckets_splits_oversized_single_bucket() {
    let mut index = BTreeIndex::<u64, String>::new("split".to_string(), None);
    for i in 0..30u64 {
        index.insert(i, format!("value_{i:03}"), now_ms()).unwrap();
    }
    assert_eq!(index.buckets.len(), 1);

    // A single bucket within its limit is left alone.
    assert_eq!(index.compact_buckets(), (1, 1));

    // Shrink the limit: the sole bucket is now far over it while holding
    // many postings, so compaction must split it.
    index.config.bucket_overload_size = 64;
    index.metadata.write().config.bucket_overload_size = 64;
    let (old, new) = index.compact_buckets();
    assert_eq!(old, 1);
    assert!(new > 1, "oversized bucket should be split, got {new}");
    assert_eq!(index.buckets.len(), new);
    assert_eq!(index.stats().max_bucket_id as usize, new - 1);
    for bucket in index.buckets.iter() {
        assert!(
            bucket.size < 64 || bucket.fields.len() == 1,
            "bucket {} has size {} with {} postings",
            bucket.key(),
            bucket.size,
            bucket.fields.len()
        );
        assert!(bucket.dirty, "every rebuilt bucket is dirty");
    }
    assert_bucket_ownership(&index);
    assert_eq!(index.len(), 30);

    // A single posting that alone exceeds the limit cannot be split and
    // is left alone as well.
    let mut single = BTreeIndex::<u64, String>::new("single".to_string(), None);
    for i in 0..200u64 {
        single.insert(i, "hot".to_string(), now_ms()).unwrap();
    }
    single.config.bucket_overload_size = 64;
    assert!(single.buckets.get(&0).unwrap().size > 64);
    assert_eq!(single.compact_buckets(), (1, 1));
}

#[tokio::test]
async fn test_has_dirty_buckets_hint_tracks_flushes() {
    let index = create_test_index();
    assert!(!index.has_dirty_buckets());
    assert!(!index.dirty_hint.load(Ordering::Acquire));

    index.insert(1, "a".to_string(), now_ms()).unwrap();
    assert!(index.has_dirty_buckets());

    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    assert!(!index.has_dirty_buckets());
    assert!(
        !index.dirty_hint.load(Ordering::Acquire),
        "a flush that leaves nothing dirty lowers the hint"
    );

    assert!(index.remove(1, "a".to_string(), now_ms()));
    assert!(index.has_dirty_buckets());
    flush_to(&index, &mut store, 2).await;
    assert!(!index.has_dirty_buckets());

    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert!(!loaded.has_dirty_buckets());
    loaded
        .insert_array(2, vec!["b".to_string(), "c".to_string()], now_ms())
        .unwrap();
    assert!(loaded.has_dirty_buckets());
    assert_eq!(loaded.remove_array(2, vec!["b".to_string()], now_ms()), 1);
    flush_to(&loaded, &mut store, 3).await;
    assert!(!loaded.has_dirty_buckets());

    // A no-op compaction leaves the index clean; a rebuild dirties it.
    assert_eq!(loaded.compact_buckets(), (1, 1));
    assert!(!loaded.has_dirty_buckets());
    let reloaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(reloaded.keys(None, None), vec!["c".to_string()]);
}

#[test]
fn test_bucket_overflow() {
    // 创建一个非常小的 bucket 大小的索引，以便测试 bucket 溢出
    let config = BTreeConfig {
        bucket_overload_size: 100, // 非常小的 bucket 大小
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("overflow_test".to_string(), Some(config));

    // 插入足够多的数据以触发 bucket 溢出
    for i in 0..100 {
        let key = format!("key_{i}");
        let _ = index.insert(i, key, now_ms());
    }

    // 验证创建了多个 bucket
    println!("index.stats(): {:?}", index.stats());
    assert!(index.stats().max_bucket_id > 1);

    // 验证所有数据都可以被搜索到
    for i in 0..100 {
        let key = format!("key_{i}");
        let result = index.query_with(&key, |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&i));
    }
}

#[test]
fn test_insert_array() {
    let index = create_test_index();

    // Test batch insert with empty values
    let result = index.insert_array(1, vec![], now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    // Test batch insert with multiple values
    let values = vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
    ];
    let result = index.insert_array(1, values.clone(), now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);

    // Verify all values were inserted
    for value in &values {
        let result = index.query_with(value, |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&1));
    }

    // Test inserting duplicate document ID for existing values (should be no-op)
    let result = index.insert_array(1, values.clone(), now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);

    // Test inserting new document ID for existing values
    let result = index.insert_array(2, values.clone(), now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);

    // Verify both document IDs are present
    for value in &values {
        let result = index.query_with(value, |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }

    // Test with non-duplicate configuration
    let config = BTreeConfig {
        bucket_overload_size: 1024,
        allow_duplicates: false,
    };
    let unique_index = BTreeIndex::new("unique_index".to_string(), Some(config));

    // First insert should succeed
    let result = unique_index.insert_array(1, vec!["apple".to_string()], now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);

    // Second insert with same value but different doc_id should fail
    let result = unique_index.insert_array(2, vec!["apple".to_string()], now_ms());
    assert!(result.is_err());

    // Test bucket overflow handling
    let small_bucket_config = BTreeConfig {
        bucket_overload_size: 50,
        allow_duplicates: true,
    };
    let overflow_index = BTreeIndex::new("overflow_test".to_string(), Some(small_bucket_config));

    // Create large values that will cause bucket overflow
    let large_values: Vec<_> = (0..20).map(|i| format!("large_value_{i}")).collect();

    let result = overflow_index.insert_array(1, large_values.clone(), now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 20);

    let result = overflow_index.insert_array(2, large_values.clone(), now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 20);

    // Verify bucket overflow occurred and created multiple buckets
    let stats = overflow_index.stats();
    println!("Overflow index stats: {stats:?}");
    assert!(stats.max_bucket_id > 0);

    // Verify all values can still be found
    for value in &large_values {
        let result = overflow_index.query_with(value, |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
    }
}

#[test]
fn test_remove_array() {
    let index = create_test_index();

    // 首先插入一批数据
    let values = vec![
        "apple".to_string(),
        "banana".to_string(),
        "cherry".to_string(),
        "date".to_string(),
        "eggplant".to_string(),
    ];

    // 插入相同的值，但使用不同的文档ID
    let _ = index.insert_array(1, values.clone(), now_ms());
    let _ = index.insert_array(2, values.clone(), now_ms());
    let _ = index.insert_array(3, vec![values[0].clone(), values[1].clone()], now_ms());

    // 确认初始数据已正确插入
    for value in &values {
        let result = index.query_with(value, |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();

        if value == "apple" || value == "banana" {
            assert_eq!(ids.len(), 3); // 这些值应该有3个文档ID
            assert!(ids.contains(&1) && ids.contains(&2) && ids.contains(&3));
        } else {
            assert_eq!(ids.len(), 2); // 其他值应该只有2个文档ID
            assert!(ids.contains(&1) && ids.contains(&2));
        }
    }

    // 测试1: 批量删除空列表 - 应该无效果
    let removed = index.remove_array(1, vec![], now_ms());
    assert_eq!(removed, 0);
    assert_eq!(index.len(), 5); // 索引中的键数量不变

    // 测试2: 批量删除部分存在的值
    let remove_values = vec![
        "apple".to_string(),
        "nonexistent".to_string(), // 不存在的值
        "banana".to_string(),
    ];
    let removed = index.remove_array(1, remove_values, now_ms());
    assert_eq!(removed, 2); // 只有2个值被实际删除

    // 验证删除结果 - apple和banana仍然存在，但不再包含文档ID 1
    let apple_result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
    assert!(apple_result.is_some());
    let apple_ids = apple_result.unwrap();
    assert_eq!(apple_ids.len(), 2);
    assert!(!apple_ids.contains(&1) && apple_ids.contains(&2) && apple_ids.contains(&3));

    let banana_result = index.query_with(&"banana".to_string(), |ids| Some(ids.clone()));
    assert!(banana_result.is_some());
    let banana_ids = banana_result.unwrap();
    assert_eq!(banana_ids.len(), 2);
    assert!(!banana_ids.contains(&1) && banana_ids.contains(&2) && banana_ids.contains(&3));

    // 测试3: 删除某个值的最后一个文档ID - 该键应该从索引中完全移除
    // 首先删除date和eggplant的文档ID 2，只剩下文档ID 1
    let _ = index.remove_array(
        2,
        vec!["date".to_string(), "eggplant".to_string()],
        now_ms(),
    );

    // 然后删除最后剩余的文档ID
    let remove_values = vec!["date".to_string(), "eggplant".to_string()];
    let removed = index.remove_array(1, remove_values, now_ms());
    assert_eq!(removed, 2);

    // 验证这些键已经完全从索引中移除
    assert!(
        index
            .query_with(&"date".to_string(), |ids| Some(ids.clone()))
            .is_none()
    );
    assert!(
        index
            .query_with(&"eggplant".to_string(), |ids| Some(ids.clone()))
            .is_none()
    );

    // 验证索引中的键数量减少
    assert_eq!(index.len(), 3); // 现在只剩下apple, banana, cherry

    // 测试4: 测试统计信息更新
    let stats = index.stats();
    assert!(stats.delete_count > 0);

    // 测试5: 测试从多个桶中删除（首先创建具有溢出的索引）
    let small_bucket_config = BTreeConfig {
        bucket_overload_size: 50,
        allow_duplicates: true,
    };
    let overflow_index = BTreeIndex::new("overflow_test".to_string(), Some(small_bucket_config));

    // 插入足够多的数据以触发桶溢出
    let large_values: Vec<_> = (0..20).map(|i| format!("large_value_{i}")).collect();
    let _ = overflow_index.insert_array(1, large_values.clone(), now_ms());
    let _ = overflow_index.insert_array(2, large_values.clone(), now_ms());

    // 验证桶溢出
    let stats = overflow_index.stats();
    assert!(stats.max_bucket_id > 0);

    // 删除所有文档ID 1的条目
    let removed = overflow_index.remove_array(1, large_values.clone(), now_ms());
    assert_eq!(removed, 20);

    // 验证所有键仍然存在，但只包含文档ID 2
    for value in &large_values {
        let result = overflow_index.query_with(value, |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&2));
    }

    // 删除所有文档ID 2的条目 - 这应该完全清空索引
    let removed = overflow_index.remove_array(2, large_values.clone(), now_ms());
    assert_eq!(removed, 20);
    assert_eq!(overflow_index.len(), 0);

    // 验证所有键都已被移除
    for value in &large_values {
        let result = overflow_index.query_with(value, |ids| Some(ids.clone()));
        assert!(result.is_none());
    }
}

#[test]
fn test_batch_update() {
    let index = create_test_index();

    // 初始插入 ["a", "b"]
    let _ = index.insert_array(1, vec!["a".to_string(), "b".to_string()], now_ms());

    // 1. 只增加新值
    let (removed, inserted) = index
        .batch_update(
            1,
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            now_ms(),
        )
        .unwrap();
    assert_eq!(removed, 0);
    assert_eq!(inserted, 1);
    let ids = index
        .query_with(&"c".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    assert!(ids.contains(&1));

    // 2. 只减少旧值
    let (removed, inserted) = index
        .batch_update(
            1,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            vec!["a".to_string()],
            now_ms(),
        )
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(inserted, 0);
    assert_eq!(
        index
            .query_with(&"a".to_string(), |ids| {
                println!("ids for 'a': {:?}", ids);
                Some(ids.clone())
            })
            .unwrap()
            .len(),
        1
    );
    assert!(
        index
            .query_with(&"c".to_string(), |ids| Some(ids.clone()))
            .is_none()
    );

    // 3. 增减混合
    let (removed, inserted) = index
        .batch_update(
            1,
            vec!["a".to_string()],
            vec!["b".to_string(), "c".to_string()],
            now_ms(),
        )
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(inserted, 2);
    let ids_b = index
        .query_with(&"b".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    let ids_c = index
        .query_with(&"c".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    assert!(ids_b.contains(&1));
    assert!(ids_c.contains(&1));
    assert!(
        index
            .query_with(&"a".to_string(), |ids| Some(ids.clone()))
            .unwrap_or_default()
            .is_empty()
    );

    // 4. 完全替换
    let (removed, inserted) = index
        .batch_update(
            1,
            vec!["b".to_string(), "c".to_string()],
            vec!["x".to_string(), "y".to_string()],
            now_ms(),
        )
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(inserted, 2);
    let ids_x = index
        .query_with(&"x".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    let ids_y = index
        .query_with(&"y".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    assert!(ids_x.contains(&1));
    assert!(ids_y.contains(&1));
    assert!(
        index
            .query_with(&"b".to_string(), |ids| Some(ids.clone()))
            .unwrap_or_default()
            .is_empty()
    );
    assert!(
        index
            .query_with(&"c".to_string(), |ids| Some(ids.clone()))
            .unwrap_or_default()
            .is_empty()
    );

    // 5. 新旧完全相同，无变化
    let (removed, inserted) = index
        .batch_update(
            1,
            vec!["x".to_string(), "y".to_string()],
            vec!["x".to_string(), "y".to_string()],
            now_ms(),
        )
        .unwrap();
    assert_eq!(removed, 0);
    assert_eq!(inserted, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_chaos() {
    let index = Arc::new(BTreeIndex::<u64, String>::new(
        "chaos_index".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: 256,
            allow_duplicates: true,
        }),
    ));

    let n_threads = 10;
    let n_keys_per_thread = 100;
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut handles = Vec::new();

    for t in 0..n_threads {
        let index = index.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            // 等待所有线程准备好
            b.wait().await;

            let base = t * n_keys_per_thread;
            let items: Vec<_> = (0..n_keys_per_thread)
                .map(|i| format!("key_{}", base + i))
                .collect();
            // 多次调用 insert_array，模拟混乱
            for j in 0..5 {
                let _ = index.insert_array((base + j) as u64, items.clone(), now_ms());
            }
        }));
    }

    // 等待所有任务完成
    futures::future::try_join_all(handles).await.unwrap();

    // 检查所有数据都能被检索到
    for t in 0..n_threads {
        let base = t * n_keys_per_thread;
        for i in 0..n_keys_per_thread {
            let key = format!("key_{}", base + i);
            let result = index.query_with(&key, |ids| Some(ids.clone()));
            assert!(result.is_some(), "key {key} not found");

            // 验证该键包含5个文档ID
            let ids = result.unwrap();
            assert_eq!(ids.len(), 5, "key {key} should have 5 doc IDs");

            for j in 0..5 {
                let doc_id = (base + j) as u64;
                assert!(ids.contains(&doc_id), "id {doc_id} not found for key {key}");
            }
        }
    }

    // 记录当前索引的大小
    let size_before_remove = index.len();
    assert_eq!(size_before_remove, n_threads * n_keys_per_thread);
    println!("索引大小 (删除前): {size_before_remove}");

    // 第二阶段：多线程同时批量删除数据
    let barrier = Arc::new(Barrier::new(n_threads));
    let mut handles = Vec::new();

    for t in 0..n_threads {
        let index = index.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            // 等待所有线程准备好
            b.wait().await;

            let base = t * n_keys_per_thread;
            let items: Vec<_> = (0..n_keys_per_thread)
                .map(|i| format!("key_{}", base + i))
                .collect();

            // 删除前3个文档ID
            for j in 0..3 {
                let doc_id = (base + j) as u64;
                let removed = index.remove_array(doc_id, items.clone(), now_ms());
                assert_eq!(
                    removed, n_keys_per_thread,
                    "应删除 {n_keys_per_thread} 个键，实际删除 {removed}"
                );
            }
        }));
    }

    // 等待所有删除任务完成
    futures::future::try_join_all(handles).await.unwrap();

    // 验证删除结果：
    // 1. 所有键都应该仍然存在，因为每个键仍有2个文档ID (4和5)
    // 2. 每个键现在应该只包含2个文档ID
    for t in 0..n_threads {
        let base = t * n_keys_per_thread;
        for i in 0..n_keys_per_thread {
            let key = format!("key_{}", base + i);
            let result = index.query_with(&key, |ids| Some(ids.clone()));
            assert!(result.is_some(), "删除后键 {key} 不应该被完全移除");

            let ids = result.unwrap();
            assert_eq!(ids.len(), 2, "删除后键 {key} 应该有2个文档ID");

            // 验证文档ID 0,1,2已被删除，3,4仍然存在
            for j in 0..3 {
                let doc_id = (base + j) as u64;
                assert!(!ids.contains(&doc_id), "文档ID {doc_id} 应该已被删除");
            }

            for j in 3..5 {
                let doc_id = (base + j) as u64;
                assert!(ids.contains(&doc_id), "文档ID {doc_id} 应该仍然存在");
            }
        }
    }

    // 第三阶段：删除所有剩余的文档ID，清空索引
    let mut handles = Vec::new();

    for t in 0..n_threads {
        let index = index.clone();
        handles.push(tokio::spawn(async move {
            let base = t * n_keys_per_thread;
            let items: Vec<_> = (0..n_keys_per_thread)
                .map(|i| format!("key_{}", base + i))
                .collect();

            // 删除剩余的2个文档ID
            for j in 3..5 {
                let doc_id = (base + j) as u64;
                index.remove_array(doc_id, items.clone(), now_ms());
            }
        }));
    }

    // 等待所有删除任务完成
    futures::future::try_join_all(handles).await.unwrap();

    // 验证索引现在应该是空的
    assert_eq!(index.len(), 0, "删除所有文档ID后索引应该为空");

    // 尝试查找任意键，应该返回None
    for t in 0..n_threads {
        let base = t * n_keys_per_thread;
        for i in 0..n_keys_per_thread {
            let key = format!("key_{}", base + i);
            let result = index.query_with(&key, |ids| Some(ids.clone()));
            assert!(result.is_none(), "键 {key} 应该已完全从索引中移除");
        }
    }
}

#[test]
fn test_stats() {
    let index = create_test_index();

    // 初始状态
    let stats = index.stats();
    assert_eq!(stats.num_elements, 0);
    assert_eq!(stats.query_count, 0);
    assert_eq!(stats.insert_count, 0);
    assert_eq!(stats.delete_count, 0);

    // 插入一些数据
    let _ = index.insert(1, "apple".to_string(), now_ms());
    let _ = index.insert(2, "banana".to_string(), now_ms());

    // 检查插入后的统计信息
    let stats = index.stats();
    assert_eq!(stats.num_elements, 2);
    assert_eq!(stats.insert_count, 2);

    // 执行一些搜索
    let _ = index.query_with(&"apple".to_string(), |_| Some(()));
    let _: Vec<()> = index.range_query_with(RangeQuery::Ge("a".to_string()), |_, _| (true, vec![]));

    // 查询不再计数：共享原子计数器会让并发读互相争抢缓存行
    let stats = index.stats();
    assert_eq!(stats.query_count, 0);

    // 删除一些数据
    let _ = index.remove(1, "apple".to_string(), now_ms());

    // 检查删除后的统计信息
    let stats = index.stats();
    assert_eq!(stats.num_elements, 1);
    assert_eq!(stats.delete_count, 1);
}

#[test]
fn test_insert_array_uses_correct_bucket_for_existing_postings() {
    // Regression test: insert_array Occupied branch must track size in the
    // posting's actual bucket, not the current max_bucket_id.
    let config = BTreeConfig {
        bucket_overload_size: 80, // small to force migration
        allow_duplicates: true,
    };
    let index = BTreeIndex::new("bucket_track".to_string(), Some(config));

    // Fill bucket 0 until a migration happens (creates bucket 1+). The
    // `anchor` key shares the bucket so that `alpha` migrates instead of
    // growing in place as a sole occupant would.
    index.insert(1, "anchor".to_string(), now_ms()).unwrap();
    let mut doc = 1u64;
    while index.stats().max_bucket_id == 0 && doc < 200 {
        index.insert(doc, "alpha".to_string(), now_ms()).unwrap();
        doc += 1;
    }
    let bucket_after_migration = index.stats().max_bucket_id;
    assert!(bucket_after_migration > 0, "migration should have occurred");

    // "alpha" now lives in the migrated bucket (> 0).
    // Insert a new value "beta" via single insert so it lands in the current max bucket.
    index.insert(1, "beta".to_string(), now_ms()).unwrap();

    // Now use insert_array to add a doc to BOTH "alpha" and "beta".
    // The fix ensures "alpha"'s size_increase is attributed to its actual bucket,
    // not the current max_bucket_id.
    let result = index.insert_array(999, vec!["alpha".to_string(), "beta".to_string()], now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);

    // Verify both postings contain doc 999.
    let alpha_ids = index
        .query_with(&"alpha".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    assert!(alpha_ids.contains(&999));
    let beta_ids = index
        .query_with(&"beta".to_string(), |ids| Some(ids.clone()))
        .unwrap();
    assert!(beta_ids.contains(&999));
}

#[test]
fn test_insert_array_enforces_unique_in_occupied_branch() {
    // Regression test: insert_array Occupied branch must re-check allow_duplicates
    // atomically while holding the entry lock, matching insert() behaviour.
    let config = BTreeConfig {
        bucket_overload_size: 1024,
        allow_duplicates: false,
    };
    let unique_index = BTreeIndex::new("unique_array".to_string(), Some(config));

    // Insert doc 1 with "apple" via single insert.
    unique_index
        .insert(1, "apple".to_string(), now_ms())
        .unwrap();

    // insert_array with a different doc_id for the same field_value should fail.
    let result = unique_index.insert_array(2, vec!["apple".to_string()], now_ms());
    assert!(result.is_err());
    match result {
        Err(BTreeError::AlreadyExists { .. }) => {}
        other => panic!("Expected AlreadyExists, got: {other:?}"),
    }

    // insert_array with the SAME doc_id should be idempotent (no error, 0 inserted).
    let result = unique_index.insert_array(1, vec!["apple".to_string()], now_ms());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn test_range_keys_or_returns_sorted_order() {
    let index = create_populated_index();
    // keys: apple < banana < cherry < date < eggplant

    // Or of two non-overlapping ranges in reverse declaration order.
    // Previously would return keys in subquery order (eggplant first)
    // due to FxHashSet dedup; now must return global B-tree order.
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Ge("eggplant".to_string())),
        Box::new(RangeQuery::Le("banana".to_string())),
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    assert_eq!(
        results,
        vec!["apple", "banana", "eggplant"],
        "Or query must return keys in global B-tree order"
    );
}

#[test]
fn test_range_keys_or_deduplicates() {
    let index = create_populated_index();
    // Overlapping ranges: banana..=cherry and apple..=cherry
    let query = RangeQuery::Or(vec![
        Box::new(RangeQuery::Between(
            "banana".to_string(),
            "cherry".to_string(),
        )),
        Box::new(RangeQuery::Between(
            "apple".to_string(),
            "cherry".to_string(),
        )),
    ]);

    let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
    // Should be deduplicated and sorted
    assert_eq!(results, vec!["apple", "banana", "cherry"]);
}

#[test]
fn test_insert_array_grows_bucket_size_for_existing_postings() {
    // Regression test: insert_array must accumulate the per-doc_id size delta
    // for existing postings into the bucket size; otherwise buckets silently
    // exceed bucket_overload_size and never split.
    let config = BTreeConfig {
        bucket_overload_size: 1024,
        allow_duplicates: true,
    };
    let index: BTreeIndex<u64, String> = BTreeIndex::new("size_growth".to_string(), Some(config));

    // Seed three field values into bucket 0.
    index
        .insert_array(
            1,
            vec!["a".to_string(), "b".to_string(), "c".to_string()],
            now_ms(),
        )
        .unwrap();
    let initial_size = index.buckets.get(&0).unwrap().size;
    assert!(initial_size > 0);

    // Add many additional doc_ids to all three EXISTING postings via insert_array.
    for doc_id in 2u64..50 {
        index
            .insert_array(
                doc_id,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                now_ms(),
            )
            .unwrap();
    }

    let grown_size = index.buckets.get(&0).unwrap().size;
    // Bucket size must reflect the new doc_ids, not stay flat.
    assert!(
        grown_size > initial_size,
        "bucket size should grow when doc_ids are appended via insert_array \
         (initial={initial_size}, after={grown_size})"
    );
}

#[test]
fn test_insert_migration_subtracts_previous_posting_size_from_source_bucket() {
    let config = BTreeConfig {
        bucket_overload_size: 128,
        allow_duplicates: true,
    };
    let index: BTreeIndex<u64, String> =
        BTreeIndex::new("single_insert_migration_size".to_string(), Some(config));

    index.insert(1, "anchor".to_string(), now_ms()).unwrap();
    for doc_id in 1u64..=23 {
        index
            .insert(doc_id, "moving".to_string(), now_ms())
            .unwrap();
    }

    let moving_key = "moving".to_string();
    let previous_posting_size = {
        let posting = index.postings.get(&moving_key).unwrap();
        posting_entry_size(&moving_key, &*posting)
    };

    let forced_source_size = {
        let mut bucket = index.buckets.get_mut(&0).unwrap();
        bucket.size = index.config.bucket_overload_size - 1;
        bucket.size
    };

    index.insert(24, moving_key.clone(), now_ms()).unwrap();

    let moved_posting = index.postings.get(&moving_key).unwrap();
    assert_ne!(
        moved_posting.bucket_id, 0,
        "posting should migrate to a new bucket"
    );

    let source_bucket = index.buckets.get(&0).unwrap();
    assert_eq!(
        source_bucket.size,
        forced_source_size.saturating_sub(previous_posting_size),
        "source bucket must reclaim the migrated posting's pre-insert size"
    );
    assert!(!source_bucket.fields.contains(&moving_key));
}

#[tokio::test]
async fn test_compact_buckets() {
    // Create an index with a tiny bucket limit to force excessive splitting,
    // simulating the fragmentation caused by the old bug.
    let config = BTreeConfig {
        bucket_overload_size: 50,
        allow_duplicates: true,
    };
    let index: BTreeIndex<u64, String> = BTreeIndex::new("compact_test".to_string(), Some(config));

    let values: Vec<String> = (0..30).map(|i| format!("value_{i:03}")).collect();
    for (i, v) in values.iter().enumerate() {
        index.insert(i as u64, v.clone(), now_ms()).unwrap();
    }

    let before = index.stats();
    let bucket_count_before = before.max_bucket_id + 1;
    println!("Before compact: {} buckets", bucket_count_before);
    assert!(bucket_count_before > 2, "should have multiple buckets");

    // Serialize fragmented index
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let objects_before: Vec<BucketObject> = store.buckets.keys().copied().collect();

    // Reload with a large bucket limit
    let mut loaded: BTreeIndex<u64, String> =
        BTreeIndex::load_metadata(&store.metadata[..]).unwrap();
    loaded.config.bucket_overload_size = 1024 * 512;
    loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
    loaded
        .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
        .await
        .unwrap();

    // Capture query results before compaction
    let queries: Vec<&str> = vec!["value_000", "value_010", "value_020"];
    let results_before: Vec<Option<Vec<u64>>> = queries
        .iter()
        .map(|q| loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec())))
        .collect();

    // Compact
    let (old, new) = loaded.compact_buckets();
    println!("Compacted: {} -> {} buckets", old, new);
    assert!(
        new < old,
        "compaction should reduce bucket count significantly"
    );
    assert!(
        new <= 2,
        "with 512K limit all postings should fit in 1-2 buckets, got {}",
        new,
    );

    // Verify query results are unchanged
    for (i, q) in queries.iter().enumerate() {
        let result = loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec()));
        assert_eq!(
            results_before[i], result,
            "query '{}' result changed after compaction",
            q
        );
    }

    // Verify flush + reload works; every pre-compaction object is
    // replaced or dropped, and the manifest commit retires it.
    let mut store2 = store.clone();
    let outcome = flush_to(&loaded, &mut store2, 100).await;
    assert!(outcome.saved);
    for object in &objects_before {
        assert!(
            outcome.obsolete.contains(object),
            "pre-compaction {object:?} must be reported obsolete"
        );
        assert!(
            !store2.buckets.contains_key(object),
            "pre-compaction {object:?} must be deleted from the store"
        );
    }

    let final_loaded: BTreeIndex<u64, String> = load_from(&store2).await;

    assert_eq!(
        final_loaded.stats().num_elements,
        loaded.stats().num_elements
    );
    for q in &queries {
        let orig = loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec()));
        let reloaded = final_loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec()));
        assert_eq!(orig, reloaded, "query '{}' mismatch after reload", q);
    }
}

#[tokio::test]
async fn test_load_metadata_and_bucket_error_paths() {
    match BTreeIndex::<u64, String>::load_metadata(&b"not cbor"[..]) {
        Err(BTreeError::Serialization { .. }) => {}
        Err(other) => panic!("expected metadata serialization error, got {other:?}"),
        Ok(_) => panic!("expected metadata serialization error"),
    }

    let metadata = BTreeMetadata {
        name: "load_errors".to_string(),
        config: BTreeConfig::default(),
        stats: BTreeStats {
            version: 7,
            max_bucket_id: 0,
            query_count: 3,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut metadata_buf,
    )
    .unwrap();

    let mut generic_error_index: BTreeIndex<u64, String> =
        BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
    assert_eq!(generic_error_index.stats().query_count, 3);
    let err = generic_error_index
        .load_buckets(async |_| Err::<Option<Vec<u8>>, _>("bucket load failed".into()))
        .await
        .unwrap_err();
    assert!(matches!(err, BTreeError::Generic { .. }));

    let mut serialization_error_index: BTreeIndex<u64, String> =
        BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
    let err = serialization_error_index
        .load_buckets(async |_| Ok(Some(b"not a bucket".to_vec())))
        .await
        .unwrap_err();
    assert!(matches!(err, BTreeError::Serialization { .. }));
}

#[tokio::test]
async fn test_load_buckets_reconciles_duplicate_postings_from_newer_bucket() {
    let metadata = BTreeMetadata {
        name: "duplicate_load".to_string(),
        config: BTreeConfig {
            bucket_overload_size: 256,
            allow_duplicates: true,
        },
        stats: BTreeStats {
            version: 4,
            max_bucket_id: 1,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut metadata_buf,
    )
    .unwrap();

    let mut old_postings = FxHashMap::default();
    old_postings.insert("same".to_string(), (0, 1, vec![1_u64].into()));
    let mut old_bucket = Vec::new();
    cbor2::to_writer(
        &BucketOwned {
            postings: old_postings,
        },
        &mut old_bucket,
    )
    .unwrap();

    let mut new_postings = FxHashMap::default();
    new_postings.insert("same".to_string(), (1, 2, vec![2_u64].into()));
    let mut new_bucket = Vec::new();
    cbor2::to_writer(
        &BucketOwned {
            postings: new_postings,
        },
        &mut new_bucket,
    )
    .unwrap();

    let mut loaded: BTreeIndex<u64, String> = BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
    loaded
        .load_buckets(async |object| {
            Ok(match object.bucket_id {
                0 => Some(old_bucket.clone()),
                1 => Some(new_bucket.clone()),
                _ => None,
            })
        })
        .await
        .unwrap();

    assert_eq!(
        loaded.query_with(&"same".to_string(), |ids| Some(ids.clone())),
        Some(vec![2])
    );
    assert_eq!(loaded.postings.get("same").unwrap().bucket_id, 1);
    let old_bucket = loaded.buckets.get(&0).unwrap();
    assert!(
        old_bucket.dirty,
        "stale source bucket should be marked dirty"
    );
    assert!(!old_bucket.fields.contains("same"));
    assert!(loaded.has_dirty_buckets());
}

#[tokio::test]
async fn test_flush_error_and_noop_paths() {
    // A failing metadata writer surfaces as an error and commits nothing.
    let index = create_test_index();
    let err = index
        .flush(FailingWriter, now_ms(), |_, _| std::future::ready(Ok(())))
        .await
        .unwrap_err();
    assert!(matches!(err, BTreeError::Generic { .. }));
    assert!(index.has_pending_metadata_flush());

    // A fresh index flushes once, then reports nothing to do.
    let fresh = create_test_index();
    assert!(
        fresh
            .flush(Vec::new(), now_ms(), |_, _| std::future::ready(Ok(())))
            .await
            .unwrap()
            .saved
    );
    assert!(
        !fresh
            .flush(Vec::new(), now_ms(), |_, _| std::future::ready(Ok(())))
            .await
            .unwrap()
            .saved
    );

    // A failing bucket writer keeps everything dirty and retryable.
    let dirty = create_test_index();
    dirty.insert(1, "apple".to_string(), now_ms()).unwrap();
    let err = dirty
        .flush(Vec::new(), now_ms(), |_, _| {
            std::future::ready(Err::<(), BoxError>("write failed".into()))
        })
        .await
        .unwrap_err();
    assert!(matches!(err, BTreeError::Generic { .. }));
    assert!(dirty.has_dirty_buckets());
}

#[test]
fn test_compact_buckets_repairs_empty_multi_bucket_index() {
    let index: BTreeIndex<u64, String> =
        BTreeIndex::new("empty_compact".to_string(), Some(BTreeConfig::default()));
    index.buckets.insert(1, BucketState::default());
    index.max_bucket_id.store(1, Ordering::Relaxed);
    assert_eq!(index.compact_buckets(), (2, 1));
    assert_eq!(index.stats().max_bucket_id, 0);
    assert!(index.has_dirty_buckets());
    assert_eq!(index.buckets.get(&0).unwrap().dirty_version, 1);
}

#[test]
fn test_prefix_query_includes_keys_containing_char_max() {
    // Regression test: the old implementation used "prefix + char::MAX" as
    // an inclusive upper bound, which missed keys like "app\u{10FFFF}x".
    let index = create_test_index();

    let plain = "app".to_string();
    let with_max = format!("app{}", char::MAX);
    let with_max_suffix = format!("app{}x", char::MAX);

    index.insert(1, plain.clone(), now_ms()).unwrap();
    index.insert(2, with_max.clone(), now_ms()).unwrap();
    index.insert(3, with_max_suffix.clone(), now_ms()).unwrap();
    index.insert(4, "apz".to_string(), now_ms()).unwrap();

    let results = index.prefix_query_with("app", |k, _| (true, Some(k.to_string())));
    assert_eq!(
        results,
        vec![plain, with_max, with_max_suffix],
        "prefix query must cover every key starting with the prefix"
    );
}

#[test]
fn test_load_metadata_caps_corrupted_num_elements_preallocation() {
    // A corrupted/hostile num_elements must not drive a giant
    // pre-allocation (or a capacity-overflow panic) on load.
    let metadata = BTreeMetadata {
        name: "huge".to_string(),
        config: BTreeConfig::default(),
        stats: BTreeStats {
            version: 1,
            num_elements: u64::MAX,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };
    let mut buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut buf,
    )
    .unwrap();

    let index = BTreeIndex::<u64, String>::load_metadata(&buf[..]).unwrap();
    assert_eq!(index.len(), 0);
    assert!(index.insert(1, "a".to_string(), now_ms()).is_err());
    assert_eq!(index.len(), 0);
}

#[test]
fn test_chaos_concurrent_insert_remove_no_phantom_btree_keys() {
    // Regression test for the phantom-btree-key race: an insert that
    // creates a posting races with a remove that deletes the posting
    // before the insert has added the key to the btree; the stale key
    // must not survive in the btree.
    //
    // Plain OS threads (not tokio tasks): the remover busy-spins to stay
    // temporally adjacent to the inserter, and a busy-spinning tokio task
    // would starve the inserter task via the worker's non-stealable LIFO
    // slot, while OS threads are preempted fairly.
    for _ in 0..5 {
        let index = BTreeIndex::<u64, String>::new(
            "phantom_chaos".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: 4096,
                allow_duplicates: true,
            }),
        );

        let n_keys = 3000u64;
        let barrier = std::sync::Barrier::new(2);

        std::thread::scope(|s| {
            // Inserter: each (doc_id, key) pair is inserted exactly once.
            s.spawn(|| {
                barrier.wait();
                for i in 0..n_keys {
                    index.insert(i, format!("k{i}"), now_ms()).unwrap();
                }
            });

            // Remover: spins until each pair is actually removed, staying
            // right on the inserter's heels so the remove lands inside
            // insert's create-posting → add-btree-key window as often as
            // possible. Every key therefore ends fully removed.
            s.spawn(|| {
                barrier.wait();
                for i in 0..n_keys {
                    let key = format!("k{i}");
                    let mut spins = 0u64;
                    while !index.remove(i, key.clone(), now_ms()) {
                        spins += 1;
                        assert!(spins < 100_000_000, "remover starved at ({i}, {key})");
                        if spins.is_multiple_of(64) {
                            // Guarantee inserter progress even on a
                            // single-core machine.
                            std::thread::yield_now();
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                }
            });
        });

        // Every pair was inserted once and removed once, so both postings
        // and btree must be empty; any leftover btree key is a phantom.
        assert_eq!(index.len(), 0);
        assert_eq!(
            index.keys(None, None),
            Vec::<String>::new(),
            "phantom btree keys without postings survived"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_chaos_unique_insert_array_keeps_bookkeeping_consistent() {
    // Regression test: when a unique-index insert_array hits a concurrent
    // uniqueness conflict mid-loop, the values inserted before the conflict
    // must keep consistent bookkeeping (btree keys + bucket tracking) so
    // they survive flush + reload.
    for _ in 0..10 {
        let index = Arc::new(BTreeIndex::<u64, String>::new(
            "unique_chaos".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: 256,
                allow_duplicates: false,
            }),
        ));
        let keys: Vec<String> = (0..50).map(|i| format!("k_{i:02}")).collect();
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for doc_id in 1..=2u64 {
            let index = index.clone();
            let keys = keys.clone();
            let b = barrier.clone();
            handles.push(tokio::spawn(async move {
                b.wait().await;
                // One of the two calls may fail with AlreadyExists; the
                // applied prefix must still be consistent.
                let _ = index.insert_array(doc_id, keys, now_ms());
            }));
        }
        futures::future::try_join_all(handles).await.unwrap();

        let btree_keys = index.keys(None, None);
        for key in &btree_keys {
            assert!(index.postings.contains_key(key));
        }
        assert_eq!(
            btree_keys.len(),
            index.postings.len(),
            "every applied posting must have a btree key"
        );

        let mut store = MemStore::default();
        flush_to(&index, &mut store, now_ms()).await;

        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert_eq!(
            loaded.len(),
            index.len(),
            "postings lost across flush + reload"
        );
        for key in &btree_keys {
            let original = index.query_with(key, |ids| Some(ids.clone()));
            let reloaded = loaded.query_with(key, |ids| Some(ids.clone()));
            assert_eq!(original, reloaded, "posting for {key} differs after reload");
        }
    }
}

#[tokio::test]
async fn test_load_buckets_skips_empty_posting_ghost_key() {
    // Regression test: a crash between "remove() empties the posting" and
    // the next repairing flush can persist a bucket containing an empty
    // posting. Loading it must not register a "ghost" key.
    let metadata = BTreeMetadata {
        name: "ghost".to_string(),
        config: BTreeConfig {
            bucket_overload_size: 256,
            allow_duplicates: true,
        },
        stats: BTreeStats {
            version: 3,
            max_bucket_id: 0,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut metadata_buf,
    )
    .unwrap();

    let mut postings = FxHashMap::default();
    postings.insert("alive".to_string(), (0u32, 1u64, vec![1u64].into()));
    postings.insert(
        "ghost".to_string(),
        (0u32, 2u64, PostingList::<u64>::default()),
    );
    let mut bucket_buf = Vec::new();
    cbor2::to_writer(&BucketOwned { postings }, &mut bucket_buf).unwrap();

    let mut loaded: BTreeIndex<u64, String> = BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
    loaded
        .load_buckets(async |object| {
            Ok(if object.bucket_id == 0 {
                Some(bucket_buf.clone())
            } else {
                None
            })
        })
        .await
        .unwrap();

    assert_eq!(loaded.len(), 1, "empty posting must not count as a key");
    assert_eq!(loaded.keys(None, None), vec!["alive".to_string()]);
    assert_eq!(
        loaded.query_with(&"ghost".to_string(), |ids| Some(ids.clone())),
        None
    );
    let not_alive: Vec<String> = loaded.range_query_with(
        RangeQuery::Not(Box::new(RangeQuery::Eq("alive".to_string()))),
        |k, _| (true, vec![k.clone()]),
    );
    assert!(
        not_alive.is_empty(),
        "Not query must not surface the ghost key"
    );
    assert!(
        loaded.has_dirty_buckets(),
        "bucket containing the ghost must be loaded dirty to self-heal"
    );

    // The self-heal flush rewrites bucket 0 without the ghost posting.
    let mut repaired: HashMap<u32, Vec<u8>> = Default::default();
    {
        let repaired = &mut repaired;
        loaded
            .flush(Vec::new(), now_ms(), |object, data| {
                repaired.insert(object.bucket_id, data);
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
    }
    let bucket: BucketOwned<u64, String> =
        cbor2::from_reader(&repaired.get(&0).unwrap()[..]).unwrap();
    assert!(bucket.postings.contains_key("alive"));
    assert!(
        !bucket.postings.contains_key("ghost"),
        "repaired bucket must not contain the empty posting"
    );
    assert!(!loaded.has_dirty_buckets());
}

#[tokio::test]
async fn test_load_buckets_empty_posting_tombstones_stale_lower_bucket_copy() {
    // A migrated posting that was emptied and sampled into the higher
    // bucket acts as a tombstone: it must also drop the stale non-empty
    // copy loaded from the lower (older) bucket.
    let metadata = BTreeMetadata {
        name: "tombstone".to_string(),
        config: BTreeConfig {
            bucket_overload_size: 256,
            allow_duplicates: true,
        },
        stats: BTreeStats {
            version: 5,
            max_bucket_id: 1,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };
    let mut metadata_buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut metadata_buf,
    )
    .unwrap();

    let mut old_postings = FxHashMap::default();
    old_postings.insert("same".to_string(), (0u32, 1u64, vec![1u64].into()));
    let mut old_bucket = Vec::new();
    cbor2::to_writer(
        &BucketOwned {
            postings: old_postings,
        },
        &mut old_bucket,
    )
    .unwrap();

    let mut new_postings = FxHashMap::default();
    new_postings.insert(
        "same".to_string(),
        (1u32, 2u64, PostingList::<u64>::default()),
    );
    let mut new_bucket = Vec::new();
    cbor2::to_writer(
        &BucketOwned {
            postings: new_postings,
        },
        &mut new_bucket,
    )
    .unwrap();

    let mut loaded: BTreeIndex<u64, String> = BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
    loaded
        .load_buckets(async |object| {
            Ok(match object.bucket_id {
                0 => Some(old_bucket.clone()),
                1 => Some(new_bucket.clone()),
                _ => None,
            })
        })
        .await
        .unwrap();

    assert_eq!(loaded.len(), 0, "tombstoned key must not survive reload");
    assert!(loaded.keys(None, None).is_empty());
    assert_eq!(
        loaded.query_with(&"same".to_string(), |ids| Some(ids.clone())),
        None
    );
    let stale_bucket = loaded.buckets.get(&0).unwrap();
    assert!(
        stale_bucket.dirty,
        "stale lower bucket must be marked dirty"
    );
    assert!(!stale_bucket.fields.contains("same"));
    drop(stale_bucket);
    assert!(loaded.has_dirty_buckets());
}

#[tokio::test]
async fn test_flush_filters_transiently_empty_posting() {
    // A posting that is empty at serialization time (e.g. loaded legacy
    // data mid-repair) must not be persisted: an empty posting on disk
    // would resurrect as a ghost key.
    let index = create_test_index();
    index.insert(1, "a".to_string(), now_ms()).unwrap();
    index.insert(2, "b".to_string(), now_ms()).unwrap();
    {
        let mut posting = index.postings.get_mut(&"a".to_string()).unwrap();
        posting.docs.swap_remove_if(|id| *id == 1);
    }

    let mut written: HashMap<u32, Vec<u8>> = Default::default();
    {
        let written = &mut written;
        index
            .flush(Vec::new(), now_ms(), |object, data| {
                written.insert(object.bucket_id, data);
                std::future::ready(Ok(()))
            })
            .await
            .unwrap();
    }

    let bucket: BucketOwned<u64, String> =
        cbor2::from_reader(&written.get(&0).unwrap()[..]).unwrap();
    assert!(
        !bucket.postings.contains_key("a"),
        "transiently empty posting must not be persisted"
    );
    assert!(bucket.postings.contains_key("b"));
}

/// A posting owned by bucket N must be written **only** into bucket N's
/// object, even while a higher-numbered bucket still lists its field value
/// (a leftover of a migration or of compaction). Persisting it into both
/// makes the stale copy win on reload — `load_buckets` forces
/// `posting.bucket_id = i` walking buckets in ascending id order — which
/// resurrects the doc set that was current when the non-owning bucket was
/// last written and drops everything appended to the posting since.
#[tokio::test]
async fn test_flush_skips_posting_listed_by_a_non_owning_bucket() {
    let index = create_test_index();
    let mut store = MemStore::default();
    let apple = "apple".to_string();
    let banana = "banana".to_string();

    // Bucket 0 owns "apple", bucket 1 owns "banana".
    index.insert(1, apple.clone(), now_ms()).unwrap();
    index.max_bucket_id.store(1, Ordering::Relaxed);
    index.insert(10, banana.clone(), now_ms()).unwrap();
    assert_eq!(index.postings.get(&apple).unwrap().bucket_id, 0);
    assert_eq!(index.postings.get(&banana).unwrap().bucket_id, 1);

    // Corrupt the packing metadata the way an interrupted migration can:
    // bucket 1 also lists "apple", whose posting bucket 0 owns.
    index
        .buckets
        .get_mut(&1)
        .unwrap()
        .fields
        .insert(apple.clone());

    flush_to(&index, &mut store, now_ms()).await;
    assert!(!index.has_dirty_buckets());

    // Appending to the posting dirties its owner only, so the stale
    // bucket 1 object stays pinned at the generation committed above.
    index.insert(2, apple.clone(), now_ms()).unwrap();
    assert!(index.buckets.get(&0).unwrap().dirty, "owner must be dirty");
    assert!(
        !index.buckets.get(&1).unwrap().dirty,
        "the non-owning bucket must not be rewritten"
    );
    flush_to(&index, &mut store, now_ms()).await;

    // The non-owning bucket's object must not carry the foreign posting.
    let generation = *index.metadata().buckets.get(&1).unwrap();
    let bucket1: BucketOwned<u64, String> = cbor2::from_reader(
        &store.buckets[&BucketObject {
            bucket_id: 1,
            generation,
        }][..],
    )
    .unwrap();
    assert!(
        !bucket1.postings.contains_key(&apple),
        "a bucket must never persist a posting owned by another bucket"
    );
    assert!(bucket1.postings.contains_key(&banana));

    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(
        loaded.query_with(&apple, |ids| Some(ids.clone())),
        Some(vec![1, 2]),
        "the stale copy in the higher-numbered bucket must not win on load"
    );
    assert_eq!(loaded.postings.get(&apple).unwrap().bucket_id, 0);
}

#[test]
fn test_compact_buckets_restores_ownership_invariants() {
    let index = create_test_index();
    for i in 0..200u64 {
        index.insert(i, format!("key-{i:03}"), now_ms()).unwrap();
    }
    // Fragment the buckets before compacting.
    for i in (0..200u64).step_by(2) {
        assert!(index.remove(i, format!("key-{i:03}"), now_ms()));
    }

    let (old_count, new_count) = index.compact_buckets();
    assert!(new_count <= old_count);
    assert_eq!(
        index.stats().max_bucket_id as usize,
        new_count - 1,
        "max_bucket_id must match the compacted bucket range"
    );
    assert_eq!(index.buckets.len(), new_count);

    // Every bucket's tracked field values point back at that bucket, and
    // every posting is tracked by exactly one bucket.
    let mut tracked = 0usize;
    for bucket in index.buckets.iter() {
        let id = *bucket.key();
        assert!(
            (id as usize) < new_count,
            "bucket id beyond compacted range"
        );
        assert!(
            bucket.dirty,
            "compacted buckets must be dirty for persistence"
        );
        for fv in bucket.fields.iter() {
            let posting = index
                .postings
                .get(fv)
                .expect("bucket must track only live postings");
            assert_eq!(posting.bucket_id, id, "posting bucket id must match owner");
        }
        tracked += bucket.fields.len();
    }
    assert_eq!(
        tracked,
        index.postings.len(),
        "every posting must be tracked by exactly one bucket"
    );
}

/// Compaction must exclude mutations, not merely be documented as
/// requiring the caller to do so: it holds the mutation gate exclusively
/// while every mutator holds it shared.
#[test]
fn test_compaction_excludes_mutations() {
    let index = Arc::new(BTreeIndex::new(
        "compact_exclusion".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE,
            allow_duplicates: true,
        }),
    ));
    for id in 0..8_u64 {
        index
            .insert(id, format!("key-{id}-{}", "x".repeat(96)), now_ms())
            .unwrap();
    }
    assert!(index.buckets.len() > 1);

    let mutation_in_progress = index.mutation_gate.read();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let compact_index = index.clone();
    let compact = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = compact_index.compact_buckets();
        done_tx.send(result).unwrap();
    });

    started_rx.recv().unwrap();
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "compaction ran while a mutation held the shared gate"
    );
    drop(mutation_in_progress);
    let (old_count, new_count) = done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    compact.join().unwrap();
    assert!(new_count <= old_count);
}

/// Regression: `compact_buckets` snapshots `postings`, clears `buckets`
/// and re-bins the snapshot. A posting created by a concurrent `insert`
/// after the snapshot ended up in no bucket at all — and only bucket
/// contents are serialized — so `insert` returned `Ok` while the value
/// silently vanished from the durable index on the next flush.
#[tokio::test]
async fn test_compaction_never_loses_concurrent_inserts() {
    let index = Arc::new(BTreeIndex::<u64, String>::new(
        "compact_concurrent_insert".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE,
            allow_duplicates: true,
        }),
    ));
    // Seed enough postings that each compaction has real work to do.
    for id in 0..64u64 {
        index.insert(id, format!("seed-{id:04}"), now_ms()).unwrap();
    }

    const WRITES: u64 = 400;
    let writer_index = index.clone();
    let writer = std::thread::spawn(move || {
        for id in 0..WRITES {
            writer_index
                .insert(1_000 + id, format!("live-{id:04}"), now_ms())
                .unwrap();
        }
    });
    let mut compactions = 0usize;
    while !writer.is_finished() {
        index.compact_buckets();
        compactions += 1;
    }
    writer.join().unwrap();
    assert!(compactions > 0, "no compaction overlapped the writer");

    // A flush persists bucket contents only, so a posting that no bucket
    // lists disappears on reload even though `insert` reported success.
    let mut store = MemStore::default();
    flush_to(&index, &mut store, now_ms()).await;
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    let missing: Vec<String> = (0..WRITES)
        .map(|id| format!("live-{id:04}"))
        .filter(|key| loaded.query_with(key, |ids| Some(ids.clone())).is_none())
        .collect();
    assert!(
        missing.is_empty(),
        "{} concurrently inserted values were lost, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    assert_eq!(loaded.len(), index.len());
}

#[test]
fn test_range_query_depth_cap() {
    let max_depth = RangeQuery::<String>::MAX_DEPTH;

    assert_eq!(RangeQuery::Eq("a".to_string()).depth(), 1);
    assert_eq!(
        RangeQuery::Not(Box::new(RangeQuery::Eq("a".to_string()))).depth(),
        2
    );
    assert_eq!(
        RangeQuery::And(vec![
            Box::new(RangeQuery::Eq("a".to_string())),
            Box::new(RangeQuery::Not(Box::new(RangeQuery::Gt("b".to_string())))),
        ])
        .depth(),
        3
    );

    let index = create_populated_index();

    // Exactly at the cap: still evaluated (odd number of Nots ->
    // complement of Eq("apple")).
    let mut at_cap = RangeQuery::Eq("apple".to_string());
    for _ in 0..(max_depth - 1) {
        at_cap = RangeQuery::Not(Box::new(at_cap));
    }
    assert_eq!(at_cap.depth(), max_depth);
    let keys: Vec<String> = index.range_query_with(at_cap, |k, _| (true, vec![k.clone()]));
    assert_eq!(
        keys,
        vec![
            "banana".to_string(),
            "cherry".to_string(),
            "date".to_string(),
            "eggplant".to_string(),
        ]
    );

    // Far over the cap: rejected with an empty result instead of
    // recursing 4000+ frames deep.
    let mut over = RangeQuery::Eq("apple".to_string());
    for _ in 0..4096 {
        over = RangeQuery::Not(Box::new(over));
    }
    assert_eq!(over.depth(), 4097);
    let keys: Vec<String> = index.range_query_with(over, |k, _| (true, vec![k.clone()]));
    assert!(keys.is_empty(), "over-deep query must be rejected");

    // try_convert_from rejects over-deep queries up-front.
    let mut deep = RangeQuery::Eq("ok".to_string());
    for _ in 0..max_depth {
        deep = RangeQuery::Not(Box::new(deep));
    }
    let err = RangeQuery::<TestKey>::try_convert_from(deep).unwrap_err();
    assert!(err.to_string().contains("depth"), "unexpected error: {err}");

    let shallow = RangeQuery::Not(Box::new(RangeQuery::Eq("ok".to_string())));
    assert!(RangeQuery::<TestKey>::try_convert_from(shallow).is_ok());
}

#[test]
fn test_range_query_depth_cap_logs_warning() {
    install_capture_logger();

    let index = create_populated_index();
    let over_depth = RangeQuery::<String>::MAX_DEPTH + 1;
    let mut over = RangeQuery::Eq("apple".to_string());
    for _ in 0..over_depth {
        over = RangeQuery::Not(Box::new(over));
    }
    let keys: Vec<String> = index.range_query_with(over, |k, _| (true, vec![k.clone()]));
    assert!(keys.is_empty(), "over-deep query must be rejected");

    let captured = CAPTURED_LOGS.lock().unwrap().clone();
    assert!(
        captured.iter().any(|msg| {
            msg.contains("exceeds the maximum")
                && msg.contains(index.name())
                && msg.contains(&(RangeQuery::<String>::MAX_DEPTH + 1).to_string())
        }),
        "expected a depth-cap warning containing the index name and depth, got: {captured:?}"
    );
}

#[test]
fn test_concurrent_appends_to_same_posting_with_migrations() {
    // Regression test for the removed debug_assert in
    // previous_posting_size_after_append: two threads appending to the
    // same field value while bucket migrations run means the popped doc
    // id is not necessarily this thread's; that used to panic in debug
    // builds.
    let index = BTreeIndex::<u64, String>::new(
        "hot_fv".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: 128,
            allow_duplicates: true,
        }),
    );
    let n = 400u64;
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|s| {
        for t in 0..2u64 {
            let index = &index;
            let barrier = &barrier;
            s.spawn(move || {
                barrier.wait();
                for i in 0..n {
                    assert!(
                        index
                            .insert(t * n + i, "hot".to_string(), now_ms())
                            .unwrap()
                    );
                }
            });
        }
    });

    let len = index
        .query_with(&"hot".to_string(), |ids| Some(ids.len()))
        .unwrap();
    assert_eq!(len, (2 * n) as usize);
}

#[tokio::test]
async fn test_flush_metadata_commit_failure_then_retry() {
    let index = create_test_index();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();

    let err = index
        .flush_owned_with(
            now_ms(),
            |_| std::future::ready(Err::<(), BoxError>("upload failed".into())),
            |_, _| std::future::ready(Ok(())),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, BTreeError::Generic { .. }));
    assert!(
        index.has_pending_metadata_flush(),
        "failed commit must keep the metadata version pending"
    );
    assert!(
        index.has_dirty_buckets(),
        "failed commit must keep buckets dirty"
    );

    let outcome = index
        .flush_owned_with(
            now_ms(),
            |data| {
                assert!(!data.is_empty());
                std::future::ready(Ok(()))
            },
            |_, _| std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    assert!(
        outcome.saved,
        "retry after failure must persist a complete snapshot"
    );
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_buckets());

    let again = index
        .flush_owned_with(
            now_ms(),
            |_| std::future::ready(Ok(())),
            |_, _| std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    assert!(!again.saved, "already-saved state must be skipped");
}

/// Cancellation (the flush future is dropped at an await point) before
/// the manifest commit must leave everything retryable.
#[tokio::test]
async fn test_cancelled_flush_before_commit_stays_retryable() {
    let index = Arc::new(create_test_index());
    index.insert(1, "apple".to_string(), now_ms()).unwrap();

    let entered = Arc::new(tokio::sync::Notify::new());
    let task_index = index.clone();
    let task_entered = entered.clone();
    let task = tokio::spawn(async move {
        task_index
            .flush_owned_with(
                now_ms(),
                move |_| {
                    let task_entered = task_entered.clone();
                    async move {
                        task_entered.notify_one();
                        std::future::pending::<Result<(), BoxError>>().await
                    }
                },
                |_, _| std::future::ready(Ok(())),
            )
            .await
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    assert!(index.has_pending_metadata_flush());
    assert!(index.has_dirty_buckets());
    let mut store = MemStore::default();
    assert!(flush_to(index.as_ref(), &mut store, now_ms()).await.saved);
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_buckets());
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(
        loaded.query_with(&"apple".to_string(), |ids| Some(ids.clone())),
        Some(vec![1])
    );
}

#[tokio::test]
async fn test_flush_owned_with_round_trip() {
    let index = create_test_index();
    index.insert(1, "apple".to_string(), now_ms()).unwrap();

    let mut store = MemStore::default();
    let outcome = flush_to(&index, &mut store, now_ms()).await;
    assert!(outcome.saved);

    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(
        loaded.query_with(&"apple".to_string(), |ids| Some(ids.clone())),
        Some(vec![1])
    );

    let idle = index
        .flush_owned_with(
            now_ms(),
            |_| std::future::ready(Ok(())),
            |_, _| std::future::ready(Ok(())),
        )
        .await
        .unwrap();
    assert!(!idle.saved, "fully persisted index must short-circuit");
}

#[test]
fn test_bucket_overload_size_is_clamped() {
    let index = BTreeIndex::<u64, String>::new(
        "clamped".to_string(),
        Some(BTreeConfig {
            bucket_overload_size: 0,
            allow_duplicates: true,
        }),
    );
    assert_eq!(
        index.metadata().config.bucket_overload_size,
        BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE
    );

    // Persisted metadata carrying a degenerate value is clamped on load.
    let metadata = BTreeMetadata {
        name: "clamped_load".to_string(),
        config: BTreeConfig {
            bucket_overload_size: 1,
            allow_duplicates: true,
        },
        stats: BTreeStats {
            version: 1,
            ..Default::default()
        },
        buckets: BTreeMap::new(),
    };
    let mut buf = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut buf,
    )
    .unwrap();
    let loaded = BTreeIndex::<u64, String>::load_metadata(&buf[..]).unwrap();
    assert_eq!(
        loaded.metadata().config.bucket_overload_size,
        BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE
    );
}

#[test]
fn test_insert_rejects_unserializable_values_without_mutation() {
    // PK whose serialization fails: rejected before any mutation.
    let index = BTreeIndex::<Flaky, String>::new("bad_pk".to_string(), None);
    let err = index
        .insert(Flaky(1, true), "k".to_string(), now_ms())
        .unwrap_err();
    assert!(matches!(err, BTreeError::Serialization { .. }));
    assert_eq!(index.len(), 0);
    assert!(index.keys(None, None).is_empty());
    assert!(!index.has_dirty_buckets());
    assert!(
        index
            .insert(Flaky(1, false), "k".to_string(), now_ms())
            .unwrap()
    );

    // FV whose serialization fails: rejected before the posting exists.
    let index = BTreeIndex::<u64, Flaky>::new("bad_fv".to_string(), None);
    let err = index.insert(1, Flaky(2, true), now_ms()).unwrap_err();
    assert!(matches!(err, BTreeError::Serialization { .. }));
    assert_eq!(index.len(), 0);
    assert!(index.keys(None, None).is_empty());
    assert!(!index.has_dirty_buckets());
    assert!(index.insert(1, Flaky(2, false), now_ms()).unwrap());
}

#[test]
fn test_insert_array_defers_serialization_error_and_keeps_applied_values() {
    let index = BTreeIndex::<u64, Flaky>::new("bad_fv_array".to_string(), None);
    let err = index
        .insert_array(
            1,
            vec![Flaky(1, false), Flaky(2, true), Flaky(3, false)],
            now_ms(),
        )
        .unwrap_err();
    assert!(matches!(err, BTreeError::Serialization { .. }));

    // The value applied before the failure keeps consistent bookkeeping.
    assert_eq!(index.len(), 1);
    assert_eq!(index.keys(None, None), vec![Flaky(1, false)]);
    assert_eq!(
        index.query_with(&Flaky(1, false), |ids| Some(ids.clone())),
        Some(vec![1])
    );
    // The failing value and the values after it were not applied.
    assert!(
        index
            .query_with(&Flaky(2, true), |ids| Some(ids.clone()))
            .is_none()
    );
    assert!(
        index
            .query_with(&Flaky(3, false), |ids| Some(ids.clone()))
            .is_none()
    );
}

#[tokio::test]
async fn clean_legacy_index_upgrades_on_first_flush_without_a_mutation() {
    let index = create_populated_index();
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let metadata = index.metadata();
    let mut legacy = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            metadata: &metadata,
        },
        &mut legacy,
    )
    .unwrap();
    store.metadata = legacy;
    store.buckets = store
        .buckets
        .into_iter()
        .map(|(object, data)| {
            (
                BucketObject {
                    bucket_id: object.bucket_id,
                    generation: 0,
                },
                data,
            )
        })
        .collect();
    let loaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert!(!loaded.has_dirty_buckets());
    assert!(loaded.has_pending_metadata_flush());
    assert!(flush_to(&loaded, &mut store, 2).await.saved);
    let metadata_only = BTreeIndex::<u64, String>::load_metadata(&store.metadata[..]).unwrap();
    assert!(!metadata_only.legacy_format);
    let reloaded: BTreeIndex<u64, String> = load_from(&store).await;
    assert_eq!(reloaded.keys(None, None), index.keys(None, None));
    assert!(!reloaded.has_pending_metadata_flush());
}

#[test]
fn explicit_null_manifest_is_not_legacy_metadata() {
    let index = create_test_index();
    let mut metadata = serde_json::to_value(index.metadata()).unwrap();
    metadata["buckets"] = serde_json::Value::Null;
    let mut bytes = Vec::new();
    cbor2::to_writer(&serde_json::json!({"metadata":metadata}), &mut bytes).unwrap();
    assert!(BTreeIndex::<u64, String>::load_metadata(&bytes[..]).is_err());
}

fn small_u64_index(name: &str) -> BTreeIndex<u64, u64> {
    let config = BTreeConfig {
        bucket_overload_size: 256,
        allow_duplicates: true,
    };
    BTreeIndex::new(name.to_string(), Some(config))
}

#[tokio::test]
async fn flush_drops_emptied_buckets_from_the_manifest() {
    let index = small_u64_index("retention");
    for i in 0..200u64 {
        index.insert(i, i, 1).unwrap();
    }
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let before = index.metadata().buckets.len();
    assert!(before > 4, "fixture needs several buckets, got {before}");

    // Retire the oldest keys: the buckets that held them empty out entirely.
    for i in 0..190u64 {
        assert!(index.remove(i, i, 2));
    }
    let outcome = flush_to(&index, &mut store, 2).await;
    let manifest = index.metadata().buckets;
    assert!(manifest.len() < before);
    assert_eq!(
        outcome.obsolete.len(),
        before,
        "every old object was replaced or retired"
    );
    assert_eq!(
        store.buckets.len(),
        manifest.len(),
        "no empty object was written"
    );
    for (bucket_id, generation) in &manifest {
        let object = BucketObject {
            bucket_id: *bucket_id,
            generation: *generation,
        };
        let bucket: BucketOwned<u64, u64> =
            cbor2::from_reader(&store.buckets[&object][..]).unwrap();
        assert!(!bucket.postings.is_empty(), "{object:?} is empty");
    }
    let reloaded: BTreeIndex<u64, u64> = load_from(&store).await;
    assert_eq!(reloaded.keys(None, None), (190..200).collect::<Vec<_>>());

    // Emptying the index leaves a modern empty manifest that reloads cleanly.
    for i in 190..200u64 {
        assert!(index.remove(i, i, 3));
    }
    assert!(flush_to(&index, &mut store, 3).await.saved);
    assert!(index.metadata().buckets.is_empty());
    assert!(store.buckets.is_empty());
    let reloaded: BTreeIndex<u64, u64> = load_from(&store).await;
    assert!(reloaded.is_empty());
    assert_eq!(reloaded.load_state(), LoadState::Ready);
    assert!(reloaded.insert(1, 1, 4).unwrap());
    assert!(flush_to(&reloaded, &mut store, 4).await.saved);
    let reloaded: BTreeIndex<u64, u64> = load_from(&store).await;
    assert_eq!(reloaded.keys(None, None), vec![1]);
}

#[tokio::test]
async fn empty_bucket_objects_left_by_older_releases_are_retired() {
    let index = small_u64_index("stale_empty");
    for i in 0..100u64 {
        index.insert(i, i, 1).unwrap();
    }
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    assert!(index.metadata().buckets.len() > 1);

    // Older releases rewrote an emptied bucket as an empty payload and kept
    // referencing it from the manifest.
    let object = *store.buckets.keys().find(|o| o.bucket_id == 0).unwrap();
    let emptied = index.buckets.get(&0).unwrap().fields.len();
    let mut empty = Vec::new();
    cbor2::to_writer(
        &BucketOwned::<u64, u64> {
            postings: FxHashMap::default(),
        },
        &mut empty,
    )
    .unwrap();
    store.buckets.insert(object, empty);

    let loaded: BTreeIndex<u64, u64> = load_from(&store).await;
    assert_eq!(loaded.len(), 100 - emptied);
    assert!(loaded.metadata().buckets.contains_key(&0));

    assert!(loaded.insert(1_000, 1_000, 2).unwrap());
    let outcome = flush_to(&loaded, &mut store, 2).await;
    assert!(outcome.obsolete.contains(&object));
    assert!(!loaded.metadata().buckets.contains_key(&0));
    assert!(!store.buckets.contains_key(&object));
    let reloaded: BTreeIndex<u64, u64> = load_from(&store).await;
    assert_eq!(reloaded.keys(None, None), loaded.keys(None, None));
}

#[tokio::test]
async fn failed_bucket_load_keeps_loaded_postings_range_queryable() {
    let index = small_u64_index("interrupted");
    for i in 0..100u64 {
        index.insert(i, i, 1).unwrap();
    }
    let mut store = MemStore::default();
    flush_to(&index, &mut store, 1).await;
    let manifest = index.metadata().buckets;
    assert!(manifest.len() > 2);
    let (&bucket_id, &generation) = manifest.iter().nth(1).unwrap();
    let unavailable = BucketObject {
        bucket_id,
        generation,
    };

    let mut loaded = BTreeIndex::<u64, u64>::load_metadata(&store.metadata[..]).unwrap();
    let result = loaded
        .load_buckets(async |object| {
            if object == unavailable {
                Err::<Option<Vec<u8>>, _>("bucket store unavailable".into())
            } else {
                Ok(store.buckets.get(&object).cloned())
            }
        })
        .await;
    assert!(result.is_err());
    assert_eq!(loaded.load_state(), LoadState::Partial);
    assert!(!loaded.is_empty());
    assert_eq!(
        loaded.keys(None, None).len(),
        loaded.len(),
        "every loaded posting keeps its ordered key"
    );

    loaded
        .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
        .await
        .unwrap();
    assert_eq!(loaded.keys(None, None), (0..100).collect::<Vec<_>>());
}

#[test]
fn postings_do_not_keep_the_legacy_update_counter() {
    // Bucket id plus alignment padding, then the id list: no counter field.
    assert_eq!(
        std::mem::size_of::<Posting<u64>>(),
        8 + std::mem::size_of::<PostingList<u64>>()
    );
    // Older releases persisted a real counter; it is ignored on load and
    // written back as a constant, keeping the triple decodable by them.
    let stored: StoredPosting<u64> = (7, 42, PostingList::from(vec![1, 2]));
    let posting = Posting::from(stored);
    let mut bytes = Vec::new();
    cbor2::to_writer(&posting, &mut bytes).unwrap();
    let decoded: (u32, u64, Vec<u64>) = cbor2::from_reader(&bytes[..]).unwrap();
    assert_eq!(decoded, (7, 0, vec![1, 2]));
}
