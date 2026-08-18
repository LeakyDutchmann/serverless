use super::memory::validate_memory_exported;
use super::functions::{validate_alloc, validate_main};

use wasmtime::{Module, ExternType};

pub fn validate_wasm_exports(module: &Module) -> anyhow::Result<()> {
    let mut has_memory = false;
    let mut has_alloc = false;
    let mut has_main = false;
    let mut has_forbiden = false;
    for export in module.exports() {
        let name = export.name();
        match export.ty() {
            ExternType::Memory(kind) => {
                if has_memory {
                    has_forbiden = true;
                    break;
                }
                let result = validate_memory_exported(kind, name);
                match result {
                    Ok(_) => {
                        has_memory = true;
                    }
                    Err(e) => {
                        has_forbiden = true;
                        eprintln!("{}", e);
                        break;
                    }
                }
                
            }
            ExternType::Func(kind) => {
                match name {
                    "alloc" => {
                        if has_alloc {
                            has_forbiden = true;
                            break;
                        }
                        let result = validate_alloc(kind);
                        match result {
                            Ok(_) => {
                                has_alloc = true;
                            }
                            Err(e) => {
                                has_forbiden = true;
                                eprintln!("{}", e);
                                break;
                            }
                        }
                    },
                    "main" => {
                        if has_main {
                            has_forbiden = true;
                            break;
                        }
                        match validate_main(kind) {
                            Ok(_) => {
                                has_main = true;
                            }
                            Err(e) => {
                                has_forbiden = true;
                                eprintln!("{}", e);
                                break;
                            }
                        }
                    }
                    _ => {  
                        println!("Wasm module exports unexpected function: {}. Ignoring.", name);
                        continue;
                    }
                }
            }
            unexpected => {
                println!("Wasm module contains unexpected exports: {:?}", unexpected);
                has_forbiden = true;
                break;
            }
        }
    }
    if has_forbiden {
        return Err(anyhow::anyhow!("Wasm module contains forbidden exports"));
    }
    Ok(())
}