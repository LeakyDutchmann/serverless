use crate::workers::main_loop::wasm_imports::http::outgoing;
use crate::workers::main_loop::wasm_imports::http::incoming;
use crate::workers::main_loop::wasm_imports::http::io;
use super::super::model::CallerTable;

use wasmtime::Linker;

pub fn provide_http(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    outgoing::register(linker)?;
    incoming::register(linker)?;
    io::register(linker)?;
    
    Ok(())
}