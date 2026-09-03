//! Every `§N` in this repository points at a section the Specification has.
//!
//! The code is dense with `§` citations, and they are load-bearing: an agent or
//! a reviewer follows one to find out why a refusal exists. A number the
//! Specification does not have sends them nowhere, and the way that happens is
//! never a typo — it is a citation left behind by a renumbering, most often
//! from the `design/` notes, which each start at §1 and go far past §104.
//!
//! This test catches only the half a machine can be sure about: a section
//! number *out of range*. A citation that is in range and points at the wrong
//! section is indistinguishable from a right one without reading it, and a
//! script that rewrote those would produce confidently wrong references —
//! worse than obviously stale ones. Those stay a per-file human pass.
//!
//! The out-of-range half is worth a test because it recurs: it was cleaned up
//! once, and a generated file quietly reintroduced a `240.18` from a design note
//! (spelled without the section sign here, so this file does not trip its own
//! test).

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

/// The Specification's last section (`# 104. …`), read from the Specification.
fn last_section() -> usize {
    include_str!("../../anda_kip/SPECIFICATION.md")
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("# ")?;
            let number = rest.split('.').next()?;
            number.parse::<usize>().ok()
        })
        .max()
        .expect("the Specification numbers its sections")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root is reachable")
}

/// Every source file the citation rule applies to.
fn sources() -> Vec<PathBuf> {
    let root = repo_root();
    let roots = [
        "rs/anda_kip/src",
        "rs/anda_kip/tests",
        "rs/anda_cognitive_nexus/src",
        "rs/anda_cognitive_nexus/tests",
        "ts/kip-do/src",
        "ts/kip-do/test",
        // The generators too, and not only what they generate: the `240.18`
        // this test exists for lived in a codegen script, and a citation
        // caught only after someone re-runs codegen is caught one step late.
        "ts/kip-do/scripts",
    ];
    let mut out = Vec::new();
    for relative in roots {
        walk(&root.join(relative), &mut out);
    }
    out.sort();
    out
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path
            .extension()
            .is_some_and(|ext| ext == "rs" || ext == "ts" || ext == "mjs")
        {
            out.push(path);
        }
    }
}

#[test]
fn no_citation_points_past_the_last_section() {
    let last = last_section();
    let root = repo_root();
    let mut offenders: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for path in sources() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (line_number, line) in text.lines().enumerate() {
            for citation in line.match_indices('§') {
                let digits: String = line[citation.0 + '§'.len_utf8()..]
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect();
                let Ok(section) = digits.parse::<usize>() else {
                    continue;
                };
                if section > last {
                    let relative = path
                        .strip_prefix(&root)
                        .unwrap_or(&path)
                        .display()
                        .to_string();
                    offenders
                        .entry(relative)
                        .or_default()
                        .push(format!("{}: §{section}", line_number + 1));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "these citations point past §{last}, the Specification's last section — a reader who \
         follows one arrives nowhere, and the usual source is a `design/` note's own numbering:\
         \n{}",
        offenders
            .iter()
            .map(|(file, hits)| format!("  {file}\n    {}", hits.join("\n    ")))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
