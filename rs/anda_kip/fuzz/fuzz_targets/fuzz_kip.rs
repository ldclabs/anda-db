#![no_main]

use libfuzzer_sys::fuzz_target;

// The combined entry point used by servers: must never panic or hang.
fuzz_target!(|data: &str| {
    let _ = anda_kip::parse_kip(data);
    let _ = anda_kip::parse_json(data);
});
