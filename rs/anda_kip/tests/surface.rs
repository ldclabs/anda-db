//! The crate's public surface, pinned.
//!
//! `lib.rs` re-exports every module with `pub use module::*`, so every
//! top-level `pub` item under `src/` is public API, whether or not anyone
//! meant it to be. This test lists those items from the sources and compares
//! the list with `tests/fixtures/public_surface.txt`: a change to the surface
//! is a change to that file, made on purpose. `ts/kip-do` pins its barrel the
//! same way (`test/surface.test.ts`).
//!
//! Only items at column zero count — `impl` methods, and the enums the
//! `wire_enum!` invocations expand to, are inside a block — and a file is read
//! up to its `#[cfg(test)]` module. Rewrite the snapshot with
//! `UPDATE_SURFACE=1 cargo test -p anda_kip --test surface`.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

const KINDS: &[&str] = &[
    "fn", "struct", "enum", "trait", "type", "const", "static", "mod", "use",
];

fn collect(dir: &Path, root: &Path, out: &mut BTreeSet<String>) {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .expect("a readable source directory")
        .map(|entry| entry.expect("a readable entry").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect(&path, root, out);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let file = path
            .strip_prefix(root)
            .expect("a path under src/")
            .display()
            .to_string();
        let text = fs::read_to_string(&path).expect("a readable source file");
        for line in text.lines() {
            if line.starts_with("#[cfg(test)]") {
                break;
            }
            let Some(rest) = line.strip_prefix("pub ") else {
                continue;
            };
            if rest.starts_with("use ") {
                out.insert(format!("{file}: {}", rest.trim_end()));
                continue;
            }
            let mut words = rest.split_whitespace();
            let mut kind = words.next().unwrap_or_default();
            while matches!(kind, "async" | "unsafe" | "const" | "extern") && !KINDS.contains(&kind)
                || (kind == "const" && rest.starts_with("const fn"))
            {
                kind = words.next().unwrap_or_default();
            }
            if !KINDS.contains(&kind) {
                continue;
            }
            let name: String = words
                .next()
                .unwrap_or_default()
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            out.insert(format!("{file}: {kind} {name}"));
        }
    }
}

#[test]
fn the_public_surface_is_the_one_on_record() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut items = BTreeSet::new();
    collect(&root, &root, &mut items);
    let rendered: String = items.iter().map(|item| format!("{item}\n")).collect();
    let snapshot = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/public_surface.txt");
    if std::env::var_os("UPDATE_SURFACE").is_some() {
        fs::write(&snapshot, &rendered).expect("a writable snapshot");
        return;
    }
    let recorded = fs::read_to_string(&snapshot).unwrap_or_default();
    let recorded: BTreeSet<&str> = recorded.lines().collect();
    let current: BTreeSet<&str> = rendered.lines().collect();
    let added: Vec<&&str> = current.difference(&recorded).collect();
    let removed: Vec<&&str> = recorded.difference(&current).collect();
    assert!(
        added.is_empty() && removed.is_empty(),
        "the public surface changed.\nadded:\n  {}\nremoved:\n  {}\n\nIf that is intended, run \
         `UPDATE_SURFACE=1 cargo test -p anda_kip --test surface` and commit the snapshot.",
        added
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("\n  "),
        removed
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("\n  "),
    );
}
