use wasmtime::{Engine, Module, Store, Linker};
use super::functions::validate_alloc_pointer;
use super::imports::validate_wasm_imports;
use super::exports::validate_wasm_exports;
pub use crate::workers::main_loop::wasm_imports::model::{CallerTable, provide_imports};

pub const ALLOWED: &[&str] = &[
    // custom host functions
    "log",
    "now_ms",
    "random_u64",
    "http_fetch",
    "kv_get",
    "kv_set",
    "result_write",

    // wasi:http/types — incoming
    "incoming-response.status",
    "incoming-response.headers",
    "incoming-response.body",
    "incoming-body.stream",
    "incoming-body.finish",
    "incoming-body.trailers.get",
    "incoming-body.trailers.drop",
    "incoming-body.future-trailers.drop",
    "incoming-body.trailers.poll",
    "future-incoming-response.poll",
    "drop-incoming-body",
    "drop-incoming-response",

    // wasi:http/types — outgoing
    "new-outgoing-request",
    "outgoing-request.set-method",
    "outgoing-request.set-scheme",
    "outgoing-request.append-header",
    "outgoing-request.set-authority",
    "outgoing-request.body",
    "outgoing-body.stream",
    "outgoing-body.append-trailer",
    "outgoing-body.finish",
    "drop-outgoing-request",
    "drop-outgoing-body",
    "handle",

    // wasi:io/streams
    "read",
    "write",

    // stream drops
    "output-stream.drop",
    "input-stream.drop",
];

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
    let mut store = Store::new(&engine, CallerTable::new());
    let mut linker = Linker::new(&engine);
    match provide_imports(&mut linker) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{}", e);
            return Err(anyhow::anyhow!("failed to provide imports to module: {}", e));
        }
    }
    let instance = match linker.instantiate(&mut store, &module) {
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