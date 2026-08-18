use crate::http::response::{Response, StatusCode, send};
use super::chunk_parser::parser::{get_wasm_chunked, get_wasm_code};
use super::validation::core::validate_wasm_module;

use tokio::net::TcpStream;
use sqlx::MySqlPool;
use wasmtime::{Engine, Module};

pub fn get_function_name(path: &str) -> String {
    let new = path.split('/').last().unwrap_or("").to_string();
    println!("path: {path}");
    new
}

pub async fn deploy(mut stream: TcpStream, buffer: &[u8], path: &str, db_pool: MySqlPool, wasm_engine: Engine) {
    let function_name = get_function_name(path);
    let string_buffer = String::from_utf8_lossy(buffer).to_string();
    let wasm = if string_buffer.contains("Transfer-Encoding: chunked") {
        get_wasm_chunked(&buffer, &string_buffer, &mut stream).await
    } else {
        get_wasm_code(&buffer, &string_buffer)
    };
    match wasm {
        Ok(wasm) => {
            let path = path.to_string();
            tokio::spawn(async move {
                let engine = wasm_engine;
                let module = Module::new(&engine, &wasm).expect("Failed to create wasm module from wasm code");
                match validate_wasm_module(&engine, &module).await {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("Failed to deploy function at path {}: {}", &path, e)));
                        send(&mut stream, &response).await
                    }
                }
                let result = sqlx::query("INSERT INTO functions(name, wasm) value(?, ?)")
                    .bind(&function_name)
                    .bind(&wasm)
                    .execute(&db_pool)
                    .await;
                match result {
                    Ok(_) => {
                        let response = Response::json(StatusCode::Ok, vec![], Some(format!("Function was succesfully deployed at {}", &path)));
                        send(&mut stream, &response).await
                    }
                    Err(e) => {
                        eprintln!("Error while deploying: {}", e);
                        let response = Response::json(StatusCode::IntServerError, vec![], Some(format!("Failed to deploy function at path {}: {}",&path, e)));
                        send(&mut stream, &response).await
                    }
                }
            });
        }
        Err(e) => {
            eprintln!("Error occured after attempt to read wasm file: {}", e);
            let response = Response::json(StatusCode::IntServerError, vec![], Some(format!("Failed to read wasm file: {}", e)));
            send(&mut stream, &response).await
        }
    }
}

