use std::{env, fs, path::Path, process};

use anda_kip::parse_kip;

/// A simple CLI tool to parse .kip files and report syntax errors.
/// Build: `cargo build --bin kip_cli --release`
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: kip_cli <file_or_dir> [file_or_dir ...]");
        process::exit(1);
    }

    let mut has_error = false;

    for arg in &args[1..] {
        let path = Path::new(arg);
        if !path.exists() {
            eprintln!("[ERROR] Path not found: {}", path.display());
            has_error = true;
            continue;
        }

        if path.is_dir() {
            has_error |= !parse_dir(path);
        } else {
            has_error |= !parse_file(path);
        }
    }

    if has_error {
        process::exit(1);
    }
}

fn parse_dir(dir: &Path) -> bool {
    let mut ok = true;
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("[ERROR] Failed to read directory {}: {}", dir.display(), e);
            return false;
        }
    };

    let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            ok &= parse_dir(&path);
        } else if path.extension().is_some_and(|ext| ext == "kip") {
            ok &= parse_file(&path);
        }
    }
    ok
}

fn parse_file(path: &Path) -> bool {
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ERROR] Failed to read {}: {}", path.display(), e);
            return false;
        }
    };

    match parse_kip(&content) {
        Ok(cmd) => {
            println!("[OK] {} — parsed as {:?}", path.display(), cmd_kind(&cmd));
            true
        }
        Err(e) => {
            eprintln!("[ERROR] {} — syntax error:\n{}", path.display(), e);
            false
        }
    }
}

fn cmd_kind(cmd: &anda_kip::ast::Command) -> &'static str {
    match cmd {
        anda_kip::ast::Command::Kql(_) => "KQL",
        anda_kip::ast::Command::Kml(_) => "KML",
        anda_kip::ast::Command::Meta(_) => "META",
    }
}
