use wasmtime::{Module, ExternType};

use super::core::ALLOWED;

pub fn validate_wasm_imports(module: &Module) -> anyhow::Result<()> {
    for import in module.imports() {
        let name = import.name();
        match import.ty() {
            ExternType::Func(_) => {
                if !ALLOWED.contains(&name) {
                    return Err(anyhow::anyhow!("Unsupported import: {:?}", import));
                }
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported import: {:?}", import));
            }
        }
    }
    Ok(())
}