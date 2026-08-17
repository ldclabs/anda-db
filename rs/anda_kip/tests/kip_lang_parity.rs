//! Differential test against the reference KIP 2.0 language toolkit.
//!
//! `anda_kip::ast` claims to be field-for-field compatible with `exec-ast.ts`
//! from [`@ldclabs/kip-lang`](https://github.com/ldclabs/KIP/tree/main/packages/kip-lang).
//! That claim is only worth something if it is checked: the fixture holds
//! command → AST pairs produced by that implementation, and every one must
//! decode here to a byte-identical tree.
//!
//! A failure is an interoperability bug. `ts/kip-do` and `anda_kip_wasm` both
//! move ASTs across the language boundary, so a silent divergence would show up
//! as an engine mis-executing a command that parsed "fine" on the other side.
//!
//! See `tests/fixtures/README.md` for how to regenerate the fixture.

use serde_json::Value;

const FIXTURE: &str = include_str!("fixtures/kip_lang_ast.json");

#[derive(serde::Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}

#[derive(serde::Deserialize)]
struct Case {
    command: String,
    ast: Value,
}

#[test]
fn every_reference_ast_decodes_identically() {
    let fixture: Fixture = serde_json::from_str(FIXTURE).expect("fixture is valid JSON");
    assert!(
        fixture.cases.len() >= 70,
        "the corpus must keep covering every statement family, found {}",
        fixture.cases.len()
    );

    let mut divergences = Vec::new();
    for case in &fixture.cases {
        match anda_kip::parse_kip(&case.command) {
            Ok(command) => {
                let ours = serde_json::to_value(&command).expect("AST serializes");
                if ours != case.ast {
                    divergences.push(format!(
                        "--- {}\n  anda_kip : {}\n  kip-lang : {}",
                        case.command,
                        serde_json::to_string(&ours).unwrap(),
                        serde_json::to_string(&case.ast).unwrap(),
                    ));
                }
            }
            Err(err) => divergences.push(format!(
                "--- {}\n  anda_kip rejected what kip-lang accepted: {err}",
                case.command
            )),
        }
    }

    assert!(
        divergences.is_empty(),
        "{} of {} commands diverge from the reference implementation:\n\n{}",
        divergences.len(),
        fixture.cases.len(),
        divergences.join("\n\n")
    );
}

#[test]
fn the_reference_asts_round_trip_through_our_types() {
    // The fixture is the other implementation's output, so decoding it into our
    // types and re-encoding proves the compatibility runs both ways: an engine
    // can consume an AST produced by `kip-lang` without going through text.
    let fixture: Fixture = serde_json::from_str(FIXTURE).expect("fixture is valid JSON");
    for case in &fixture.cases {
        let decoded: anda_kip::Command = serde_json::from_value(case.ast.clone())
            .unwrap_or_else(|err| panic!("{}: {err}", case.command));
        let re_encoded = serde_json::to_value(&decoded).expect("AST serializes");
        assert_eq!(re_encoded, case.ast, "re-encoding changed {}", case.command);
    }
}

#[test]
fn the_corpus_covers_all_three_surfaces() {
    use anda_kip::Command;

    let fixture: Fixture = serde_json::from_str(FIXTURE).expect("fixture is valid JSON");
    let (mut kql, mut kml, mut meta) = (0, 0, 0);
    for case in &fixture.cases {
        match anda_kip::parse_kip(&case.command).expect("corpus parses") {
            Command::Kql(_) => kql += 1,
            Command::Kml(_) => kml += 1,
            Command::Meta(_) => meta += 1,
        }
    }
    assert!(kql >= 3, "only {kql} KQL commands in the corpus");
    assert!(kml >= 10, "only {kml} KML commands in the corpus");
    assert!(meta >= 30, "only {meta} META commands in the corpus");
}
