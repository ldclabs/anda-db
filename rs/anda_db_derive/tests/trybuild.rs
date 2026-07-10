//! UI tests: compile the macro-generated code for real.
//!
//! Token-string assertions in the unit tests cannot catch generated code
//! that fails to compile (e.g. the old `<T>::field_type()` fallback for bare
//! generic fields) or misleading diagnostics caused by name shadowing.
//! These tests do.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_*.rs");
    t.compile_fail("tests/ui/fail_*.rs");
}
