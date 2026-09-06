use anda_db_utils::UniqueVec;
use std::hint::black_box;
mod support;
fn main() {
    support::header();
    for (label, distinct) in [("unique", 100_000), ("half", 50_000), ("duplicate", 100)] {
        let data: Vec<u64> = (0..100_000).map(|i| i % distinct).collect();
        support::measure(
            &format!("u64/{label}/from_iter"),
            || {},
            || {
                black_box(data.iter().copied().collect::<UniqueVec<_>>());
            },
        );
        support::measure(
            &format!("u64/{label}/from_vec"),
            || {},
            || {
                black_box(UniqueVec::from(data.clone()));
            },
        );
        let strings: Vec<String> = data
            .iter()
            .map(|i| format!("key-{i:016}-owned-payload"))
            .collect();
        support::measure(
            &format!("string/{label}"),
            || {},
            || {
                black_box(strings.iter().cloned().collect::<UniqueVec<_>>());
            },
        );
        let encoded = serde_json::to_vec(&strings).unwrap();
        support::measure(
            &format!("serde/{label}"),
            || {},
            || {
                black_box(serde_json::from_slice::<UniqueVec<String>>(&encoded).unwrap());
            },
        );
    }
}
