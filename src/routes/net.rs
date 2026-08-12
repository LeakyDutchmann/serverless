use super::model::Route;
use super::api::deployment::handler::deploy;
use super::api::execute::execute;
use super::api::delete::delete;
use crate::workers::model::Message;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlx::MySqlPool;

pub async fn handle_connection(mut stream: TcpStream, db_pool: MySqlPool, tx: Sender<String>) {
    let mut buffer = [0u8; 4096];
    let n = stream.read(&mut buffer).await.unwrap();    
    let request_string = String::from_utf8_lossy(&buffer[0..n]).into_owned();
    let (method, path) = parse_request_line(&request_string);
    match method.as_str() {
        "POST" => {
            if path.starts_with("/functions") {
                deploy(stream, &buffer, &path, db_pool).await;
            } else if path.starts_with("/invoke") {
                execute(stream, &path, tx).await;
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
}

pub fn parse_request_line(request: &str) -> (String, String) {
    let line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = line.split_whitespace().collect();
    let method = parts[0];
    if parts.len() < 2 {
        return ("INCORRECT".to_string(), "".to_string());
    }
    (method.to_string(), parts[1].to_string())
}