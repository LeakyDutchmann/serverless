use super::model::Route;
use super::api::deployment::handler::deploy;
use super::api::execute::execute;
use super::api::delete::delete;
use crate::scheduler::model::Job;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlx::MySqlPool;
use wasmtime::Engine;

pub async fn handle_connection(mut stream: TcpStream, db_pool: MySqlPool, tx: Sender<Job>, wasm_engine: Engine) {
    let mut buffer = [0u8; 4096];
    let n = stream.read(&mut buffer).await.unwrap();    
    let request_string = String::from_utf8_lossy(&buffer[0..n]).into_owned();
    if let Some((method, path)) = parse_request_line(&request_string) {
        match method.as_str() {
            "POST" => {
                if path.starts_with("/functions") {
                    deploy(stream, &buffer, &path, db_pool, wasm_engine).await;
                } else if path.starts_with("/invoke") {
                    execute(stream, &buffer, &path, tx).await;
                }
            },
            "DELETE" => {
                if path.starts_with("/functions") {
                    delete(stream, &buffer).await;
                }
            }
            _ => {
                println!("Unexpected request: {}", request_string);
            }
        }
    } else {
        println!("Invalid request: {}", request_string);
    }
    
}

pub fn parse_request_line(request: &str) -> Option<(String, String)> {
    let line = request.lines().next().unwrap_or("");
    if line.is_empty() {
        return None;
    }
    let parts: Vec<&str> = line.split_whitespace().collect();
    let method = parts[0];
    if parts.len() < 2 {
        return None;
    }
    Some((method.to_string(), parts[1].to_string()))
}