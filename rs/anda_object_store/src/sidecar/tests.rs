use super::*;

#[test]
fn generation_ids_are_unique_and_carry_timestamps() {
    let a = new_generation();
    let b = new_generation();
    assert_ne!(a, b);

    let ts = generation_timestamp_ms(&a).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!(
        ts <= now && ts + 60_000 > now,
        "timestamp {ts} vs now {now}"
    );

    assert_eq!(generation_timestamp_ms("not-a-generation"), None);
    assert_eq!(generation_timestamp_ms("0123"), None);
    assert_eq!(generation_timestamp_ms("zzzzzzzzzzzzzzzz-00000000"), None);
}
