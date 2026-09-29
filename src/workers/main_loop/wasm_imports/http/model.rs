use crate::workers::main_loop::wasm_imports::http::outgoing;
use crate::workers::main_loop::wasm_imports::http::incoming;
use crate::workers::main_loop::wasm_imports::http::io;
use super::super::model::CallerTable;

use wasmtime::Linker;

pub fn provide_http(mut linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    outgoing::register(&mut linker)?;
    incoming::register(&mut linker)?;
    io::register(&mut linker)?;
    
    Ok(())
}