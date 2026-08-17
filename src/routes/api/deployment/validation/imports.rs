use wasmtime::Module;

pub fn validate_wasm_imports(module: &Module) -> anyhow::Result<()> {
    for import in module.imports() {
        return Err(anyhow::anyhow!("Unsupported import: {:?}", import));
    }
    Ok(())
}