fn main() {
    // With pyo3's `extension-module` feature, libpython is not linked; on macOS the
    // cdylib needs `-undefined dynamic_lookup` so `cargo build/run` works outside maturin.
    pyo3_build_config::add_extension_module_link_args();
}
