use wasmtime::{FuncType, ValType};
use wasmtime::{Instance, Store};

pub fn validate_alloc(kind: FuncType) -> anyhow::Result<()> {
    let params: Vec<ValType> = kind.params().collect();
    let returns: Vec<ValType> = kind.results().collect();
    if params.len() != 1 {
        return Err(anyhow::anyhow!("Alloc params must contain exactly one element of type i32"));
    }
    if returns.len() != 1 {
        return Err(anyhow::anyhow!("Alloc return type must be exactly one element of type i32"));
    }
    match params[0] {
        ValType::I32 => {}
        _ => {
            return Err(anyhow::anyhow!("Alloc parameter should be of type i32. Type found: {}", params[0]));
        }
    }
    match returns[0] {
        ValType::I32 => {}
        _ => {
            return Err(anyhow::anyhow!("Alloc return type i32. Type found: {}", params[0]))
        }
    }
    Ok(())
}

pub fn validate_alloc_pointer(ptr: u32, instance: &Instance, mut store: &mut Store<()>) -> anyhow::Result<()> {
    if ptr == 0 {
        return Err(anyhow::anyhow!("Alloc call returned pointer which = 0"));
    }
    let memory = instance.get_memory(&mut store, "memory").unwrap();
    let size = memory.data_size(&store) as u32;
    if ptr >= size {
        return Err(anyhow::anyhow!("alloc returned pointer outside memory"));
    }
    if ptr + 32 > size {
        return Err(anyhow::anyhow!("alloc returned region that exceeds memory"));
    }
    let result = memory.write(&mut store, ptr as usize, &[1, 3, 4]);
    if result.is_err() {
        return Err(anyhow::anyhow!("failed to write into memory"));
    }
    Ok(())
}