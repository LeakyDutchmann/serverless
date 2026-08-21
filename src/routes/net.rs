use super::model::Route;
use super::api::deployment::handler::deploy;
use super::api::deployment::chunk_parser::{parser::read_chunks_from_buffer, models::{ChunkParserResult, ChunkReadingError}};
use super::api::execute::execute;
use super::api::delete::delete;
use crate::http::utils::get_body_len;
use crate::http::response::{Response, send, StatusCode};
use crate::scheduler::model::Job;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sqlx::MySqlPool;
use tokio_tungstenite::tungstenite::http::header;
use wasmtime::Engine;

pub async fn handle_connection(mut stream: TcpStream, db_pool: MySqlPool, tx: Sender<Job>, wasm_engine: Engine) {
    let (headers_buf, leftover, headers_end) = match read_headers(&mut stream).await {
        Ok((headers, leftover, hn)) => (headers, leftover, hn),
        Err(e) => {
            println!("Error reading headers: {}", e);
            let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("{}", e)));
            send(&mut stream, &response).await;
            return;
        }
    };
    
    let headers_str = String::from_utf8_lossy(&headers_buf).to_string();
    let body = if headers_str.contains("Transfer-Encoding: chunked") {
        let result = read_body_chunked(&mut stream, &leftover).await;
        match result {
            Ok(body) => body,
            Err(e) => {
                println!("Failed to read chunked body: {}", e);
                let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("Failed to read chunked body: {}", e)));
                send(&mut stream, &response).await;
                return;
            }
        }
    } else {
        let len = get_body_len(&headers_str);
        if len.is_none() {
            let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("Mailformed request: no body length defined")));
            send(&mut stream, &response).await;
            return;
        }
        let len = len.unwrap();
        if len != 0 {
            let mut buffer: Vec<u8> = vec![0u8; len];
            buffer[..leftover.len()].copy_from_slice(&leftover);
            if leftover.len() < len {
                let r = stream.read(&mut buffer[leftover.len()..]).await;
                match r {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error reading body: {:?}", e);
                        return;
                    }
                }
            }
            buffer
        } else {
            return;
        }
    };
    let header_string = String::from_utf8_lossy(&headers_buf).into_owned();
    let mut buffer: Vec<u8> = headers_buf.clone();
    buffer.extend_from_slice(&body);
    if let Some((method, path)) = parse_request_line(&header_string) {
        match method.as_str() {
            "POST" => {
                if path.starts_with("/functions") {
                    deploy(stream, &body, &path, db_pool, wasm_engine).await;
                } else if path.starts_with("/invoke") {
                    println!("Executing function...");
                    execute(stream, &buffer, &path, tx).await;
                }
            },
            "DELETE" => {
                if path.starts_with("/functions") {
                    delete(stream, &buffer).await;
                }
            }
            _ => {
                println!("Unexpected request: {}", header_string);
            }
        }
    } else {
        println!("Invalid request: {}", header_string);
    }
}

pub async fn read_body_chunked(stream: &mut TcpStream, leftover: &[u8]) -> Result<Vec<u8>, ChunkReadingError> {
    let mut body: Vec<u8> = Vec::new();
    let mut master_buffer: Vec<u8> = Vec::new();
    master_buffer.extend_from_slice(leftover); 
    match read_chunks_from_buffer(&mut master_buffer, &mut body).await {
        ChunkParserResult::Done => {return Ok(body)}
        ChunkParserResult::NeedMoreData => {}
        ChunkParserResult::Error(e) => {
            eprintln!("error ocured while parsing chunk of data: {}", e);
        }
    }
    let mut new_buffer = [0u8; 1024];
    loop {
        let n = match stream.read(&mut new_buffer).await {
            Ok(n) => n,
            Err(e) => {
                return Err(ChunkReadingError::IoError(e))
            },
        };
        if n == 0 {
            return Err(ChunkReadingError::UnexpectedEOF);
        }
        master_buffer.extend_from_slice(&new_buffer[..n]);
        match read_chunks_from_buffer(&mut master_buffer, &mut body).await {
            ChunkParserResult::Done => {return Ok(body)}
            ChunkParserResult::NeedMoreData => {continue}
            ChunkParserResult::Error(e) => {
                return Err(ChunkReadingError::ParserError(e))
            }
        }
    }
}

pub async fn read_headers(stream: &mut TcpStream) -> anyhow::Result<(Vec<u8>, Vec<u8>, usize)> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut temp = [0u8; 1024];
    loop {
        let n = stream.read(&mut temp).await;
        match n {
            Ok(0) => return Err(anyhow::anyhow!("Unexpected close of connection")),
            Ok(n) => buf.extend_from_slice(&temp[..n]),
            Err(e) => return Err(e.into()),
        }
        if let Some(pos) = find_headers_end(&buf) {
            return Ok((buf[..pos].to_vec(), buf[pos..].to_vec(), pos));
        }
        if buf.len() > 64 * 1024 {
            return Err(anyhow::anyhow!("Request headers section is too large"));
        }
    }
}

pub fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|w| w + 4)
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