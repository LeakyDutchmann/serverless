use wasmtime::{Engine, Instance, Module, Store, Linker, Caller};
use std::sync::Arc;
use tokio::sync::RwLock;
use super::init::MemoryUsage;


pub fn provide_imports(mut linker: &mut Linker<MemoryUsage>) {
    provide_logging(&mut linker);
    
}

pub fn provide_logging(linker: &mut Linker<MemoryUsage>) {
    linker.func_wrap("host", "log", |mut caller: Caller<'_, MemoryUsage>, ptr: i32, len: i32 |  {
        if len <= 0 {
            return;
        }
        let mut buffer = vec![0u8; len as usize];
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let metrics = caller.data_mut();
        metrics.peak = m_usage.max(metrics.peak);
        metrics.min = m_usage.min(metrics.min);
        let _ = memory.read(&mut caller, ptr as usize, &mut buffer).unwrap();
        println!("WASMLOG: {}", String::from_utf8_lossy(&buffer[..len as usize]));
    });
}

pub async fn run_wasm(instance: Instance, mut store: &mut Store<MemoryUsage>, input: &[u8]) -> Result<Vec<u8>, String>{
    let memory = match instance.get_memory(&mut store, "memory") {
        Some(memory) => memory,
        None => {
            return Err("No memory found in wasm module".to_string());
        }
    };
    let ptr = match get_alloc_ptr(instance, &mut store, &input) {
        Ok(ptr) => ptr,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    match memory.write(&mut store, ptr as usize, &input) {
        Ok(_) => {}
        Err(e) => {
            return Err(e.to_string());
        }
    }
    let main = match instance.get_typed_func::<(u32, u32), (u32, u32)>(&mut store, "main") {
        Ok(main) => main,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    let result = main.call(&mut store, (ptr, input.len() as u32));
    if result.is_err() {
        return Err(result.err().unwrap().to_string());
    }
    let (new_ptr, len) = result.unwrap();
    let mut buffer = vec![0u8; len as usize];
    let func_result = memory.read(&mut store, new_ptr as usize, &mut buffer);
    match func_result {
        Ok(_) => {
            return Ok(buffer);
        }
        Err(e) => {
            return Err(e.to_string())
        }
    }
}

pub fn create_wasm_instance(engine: &Engine, wasm: &[u8], mut store: &mut Store<MemoryUsage>) -> anyhow::Result<Instance> {
    let module = match Module::new(&engine, wasm) {
        Ok(module) => module,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to create wasm module: {}", e));
        }
    };
    let mut linker = Linker::new(&engine);
    provide_imports(&mut linker);
    
    let instance = match linker.instantiate(&mut store, &module) {
        Ok(instance) => instance,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to create instance wasm module: {}", e));
        }
    };
    Ok(instance)  
}

pub fn get_alloc_ptr(instance: Instance, mut store: &mut Store<MemoryUsage>, input: &[u8]) -> anyhow::Result<u32>{
    let alloc = match instance.get_typed_func::<u32, u32>(&mut store, "alloc") {
        Ok(alloc) => alloc,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to get alloc function: {}", e));
        }
    };
    let len = input.len() as u32;
    let ptr = match alloc.call(&mut store, len) {
        Ok(ptr) => ptr,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to get pointer: {}", e));
        }
    };
    Ok(ptr)
}