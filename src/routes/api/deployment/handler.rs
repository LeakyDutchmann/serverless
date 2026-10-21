use crate::http::response::{Response, StatusCode};
use crate::http::utils::get_function_name;
use super::validation::core::validate_wasm_module;

use sqlx::MySqlPool;
use wasmtime::{Engine, Module};


pub async fn deploy(buffer: &[u8], path: &str, db_pool: &MySqlPool, engine: &Engine, keep_alive: bool) -> Response {
    let function_name = get_function_name(path);
    let wasm = buffer.to_vec();
    let path = path.to_string();
    // println!("{:02x?}", &wasm[..16]);
    let module = match Module::new(engine, &wasm) {
        Ok(module) => {module},
        Err(e) => {
            let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to deploy function at path {}, {}", path, e)), keep_alive);
            return response;
        }
    };
    let memory_usage = match module.serialize() {
        Ok(v) => {
            v.len()
        }
        Err(e) => {
            let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to deploy function at path {}, {}", path, e)), keep_alive);
            return response;
        }
    };
    match validate_wasm_module(engine, &module).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{}", e);
            let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to deploy function at path {}: {}", path, e)), keep_alive);
            return response;
        }
    }
    let result = sqlx::query("INSERT INTO functions(path, wasm, memory_usage) value(?, ?, ?)")
        .bind(&function_name)
        .bind(&wasm)
        .bind(memory_usage as i32)
        .execute(db_pool)
        .await;
    match result {
        Ok(_) => {
            let status = "function was succesfully deployed at ".to_string();
            let status = status + &path;
            let result = status.as_bytes().to_vec();
            
            Response::new(StatusCode::Ok, result, None, keep_alive)
        }
        Err(e) => {
            eprintln!("Error while deploying: {}", e);
            
            Response::new(StatusCode::IntServerError, vec![], Some(format!("Failed to deploy function at path {}: {}",path, e)), keep_alive)
        }
    }
}

