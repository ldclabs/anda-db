//! Conformance against the bundled LLM-facing syntax card.
//!
//! `KIPSyntax.md` is the official condensation of KIP 2.0 for a model to read in
//! context, and every ```` ```kip ```` block in it is meant to be exactly one
//! executable command. That makes it a ready-made corpus covering every
//! statement family — the same corpus the reference toolkit
//! `@ldclabs/kip-lang` checks itself against, so a divergence here is a
//! divergence between the two implementations.

use anda_kip::{Command, KIP_SYNTAX, parse_kip};

/// Extracts the bodies of fenced code blocks tagged with `language`.
fn fenced_blocks(markdown: &str, language: &str) -> Vec<String> {
    let open = format!("```{language}");
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;

    for line in markdown.lines() {
        match &mut current {
            Some(body) => {
                if line.trim_end() == "```" {
                    blocks.push(std::mem::take(body));
                    current = None;
                } else {
                    body.push_str(line);
                    body.push('\n');
                }
            }
            None => {
                if line.trim_end() == open {
                    current = Some(String::new());
                }
            }
        }
    }
    blocks
}

#[test]
fn every_kip_block_in_the_syntax_card_is_one_executable_command() {
    let blocks = fenced_blocks(KIP_SYNTAX, "kip");
    assert!(
        blocks.len() >= 10,
        "expected the syntax card to carry executable examples, found {} blocks",
        blocks.len()
    );

    let mut failures = Vec::new();
    for (index, source) in blocks.iter().enumerate() {
        if let Err(err) = parse_kip(source) {
            failures.push(format!(
                "--- kip block {} ---\n{}\n=> {err}\n   hint: {}",
                index + 1,
                source.trim_end(),
                err.effective_hint()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} syntax-card examples failed to parse:\n\n{}",
        failures.len(),
        blocks.len(),
        failures.join("\n\n")
    );
}

#[test]
fn the_syntax_card_covers_all_three_surfaces() {
    let mut kql = 0;
    let mut kml = 0;
    let mut meta = 0;
    for source in fenced_blocks(KIP_SYNTAX, "kip") {
        match parse_kip(&source).expect("already checked above") {
            Command::Kql(_) => kql += 1,
            Command::Kml(_) => kml += 1,
            Command::Meta(_) => meta += 1,
        }
    }
    assert!(
        kql > 0 && kml > 0 && meta > 0,
        "{kql} KQL, {kml} KML, {meta} META"
    );
}

#[test]
fn every_command_embedded_in_a_request_example_parses() {
    let mut checked = 0;
    for (index, source) in fenced_blocks(KIP_SYNTAX, "json").iter().enumerate() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(source) else {
            continue; // not every json block is a whole envelope
        };
        if value.get("kip").and_then(|v| v.as_str()) != Some("2.0") {
            continue;
        }
        let Some(operations) = value.get("operations").and_then(|v| v.as_array()) else {
            continue;
        };
        for (op_index, operation) in operations.iter().enumerate() {
            let Some(command) = operation.get("command").and_then(|v| v.as_str()) else {
                continue;
            };
            parse_kip(command).unwrap_or_else(|err| {
                panic!(
                    "json block {}, operation {}: {command}\n=> {err}",
                    index + 1,
                    op_index + 1
                )
            });
            checked += 1;
        }
    }
    assert!(checked > 0, "the syntax card must carry a request example");
}
