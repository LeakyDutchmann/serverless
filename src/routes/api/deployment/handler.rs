use crate::http::response::{Response, StatusCode, send};
use crate::http::utils::get_function_name;
use super::validation::core::validate_wasm_module;

use tokio::net::TcpStream;
use sqlx::MySqlPool;
use wasmtime::{Engine, Module};


pub async fn deploy(mut stream: TcpStream, buffer: &[u8], path: &str, db_pool: MySqlPool, wasm_engine: Engine) {
    let function_name = get_function_name(path);
    let wasm = buffer.to_vec();
    let path = path.to_string();
    tokio::spawn(async move {
        let engine = wasm_engine;
        // println!("{:02x?}", &wasm[..16]);
        let module = Module::new(&engine, &wasm).expect("Failed to create wasm module from wasm code");
        match validate_wasm_module(&engine, &module).await {
            Ok(_) => {}
            Err(e) => {
                eprintln!("{}", e);
                let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("Failed to deploy function at path {}: {}", &path, e)));
                send(&mut stream, &response).await;
                return;
            }
        }
        let result = sqlx::query("INSERT INTO functions(path, wasm) value(?, ?)")
            .bind(&function_name)
            .bind(&wasm)
            .execute(&db_pool)
            .await;
        match result {
            Ok(_) => {
                let status = "function was succesfully deployed at ".to_string();
                let status = status + &path;
                let result = status.as_bytes().to_vec();
                let response = Response::json(StatusCode::Ok, result, None);
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

