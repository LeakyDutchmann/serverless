use wasmtime::{Engine, Instance, Module, Store};

pub async fn run_wasm(instance: Instance, mut store: &mut Store<()>, input: &[u8]) -> Result<Vec<u8>, String>{
    let ptr = match get_alloc_ptr(instance, &mut store, &input) {
        Ok(ptr) => ptr,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    let memory = match instance.get_memory(&mut store, "memory") {
        Some(memory) => memory,
        None => {
            return Err("No memory found in wasm module".to_string());
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

pub fn create_wasm_instance(engine: &Engine, wasm: &[u8], mut store: &mut Store<()>) -> anyhow::Result<Instance> {
    let module = match Module::new(&engine, wasm) {
        Ok(module) => module,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to create wasm module: {}", e));
        }
    };
    let instance = match Instance::new(&mut store, &module, &[]) {
        Ok(instance) => instance,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to create instance wasm module: {}", e));
        }
    };
    Ok(instance)  
}

pub fn get_alloc_ptr(instance: Instance, mut store: &mut Store<()>, input: &[u8]) -> anyhow::Result<u32>{
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