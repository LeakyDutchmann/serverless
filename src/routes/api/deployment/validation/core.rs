use wasmtime::{Engine, Module, Store, Instance};
use super::functions::validate_alloc_pointer;
use super::imports::validate_wasm_imports;
use super::exports::validate_wasm_exports;

pub async fn validate_wasm_module(engine: &Engine, module: &Module) -> anyhow::Result<()>{
    match validate_wasm_exports(&module) {
        Ok(_) => {
            
        }
        Err(e) => {
            eprintln!("{}", e);
            return Err(anyhow::anyhow!("{}", e));
        }
    }
    match validate_wasm_imports(&module) {
        Ok(_) => {
            
        }
        Err(e) => {
            eprintln!("{}", e);
            return Err(anyhow::anyhow!("{}", e));
        }
    }
    let mut store = Store::new(&engine, ());
    let instance = match Instance::new(&mut store, &module, &[]) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("failed to instantiate module: {}", e);
            return Err(anyhow::anyhow!("failed to instantiate module: {}", e));
        }
    };
    let alloc = match instance.get_typed_func::<u32, u32>(&mut store, "alloc") {
        Ok(a) => a,
        Err(r) => {
            eprintln!("{}", r);
            return Err(anyhow::anyhow!("{}", r));
        }
    };
    let ptr = alloc.call(&mut store, 32);
    if ptr.is_err() {
        let e = ptr.unwrap_err();
        eprintln!("Error while testing module: {}", e);
        return Err(anyhow::anyhow!("{}", e));
    }
    let ptr = ptr.unwrap();
    match validate_alloc_pointer(ptr, &instance, &mut store) {
        Ok(_) => {Ok(())}
        Err(e) => {
            return Err(anyhow::anyhow!("{}", e));
        }
    }
}