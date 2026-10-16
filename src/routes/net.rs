use super::api::deployment::handler::deploy;
use super::api::execute::execute;
use super::api::delete::delete;
use crate::http::utils::{get_body_len, parse_request_line, read_body_chunked, read_headers};
use crate::http::response::{Response, send, StatusCode};
use crate::scheduler::types::Job;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::io::AsyncReadExt;
use sqlx::MySqlPool;
use wasmtime::Engine;

pub async fn handle_connection(mut stream: TcpStream, db_pool: MySqlPool, tx: Sender<Job>, wasm_engine: Engine) {
    let mut keep_alive;
    let break_reason;
    loop {
            let (headers_buf, leftover, _headers_end) = match read_headers(&mut stream).await {
            Ok((headers, leftover, hn)) => (headers, leftover, hn),
            Err(e) => {
                // println!("Error reading headers: {}", e);
                let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("{}", e)), false);
                send(&mut stream, &response).await;
                return;
            }
        };
        
        let headers_str = String::from_utf8_lossy(&headers_buf).to_string();
        if !headers_str.contains("connection: close") && !headers_str.contains("Connection: close") {
            keep_alive = true;
        } else {
            keep_alive = false;
        }
        let body = if headers_str.contains("Transfer-Encoding: chunked") {
            let result = read_body_chunked(&mut stream, &leftover).await;
            match result {
                Ok(body) => body,
                Err(e) => {
                    println!("Failed to read chunked body: {}", e);
                    let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to read chunked body: {}", e)), keep_alive);
                    send(&mut stream, &response).await;
                    return;
                }
            }
        } else {
            let len = get_body_len(&headers_str);
            if len.is_none() {
                println!("No body len provided");
                let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Mailformed request: no body length defined")), keep_alive);
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
                println!("Body len is 0");
                return;
            }
        };
        let header_string = String::from_utf8_lossy(&headers_buf).into_owned();
        let mut buffer: Vec<u8> = headers_buf.clone();
        buffer.extend_from_slice(&body);
        if let Some((method, path)) = parse_request_line(&header_string) {
            let response = match method.as_str() {
                "POST" => {
                    if path.starts_with("/functions") {
                        deploy(&body, &path, &db_pool, &wasm_engine, keep_alive).await
                    } else if path.starts_with("/invoke") {
                        execute(&buffer, &path, tx.clone(), keep_alive).await
                    } else {
                        break_reason = format!("Invalid Request: {}, {}", method, path);
                        break;
                    }
                },
                "DELETE" => {
                    if path.starts_with("/functions") {
                        delete(&buffer).await
                    } else {
                        break_reason = format!("Invalid Request: {}, {}", method, path);
                        break;
                    }
                }
                _ => {
                    println!("Unexpected request: {}", header_string);
                    break_reason = format!("Invalid Request: {}, {}", method, path);
                    break;
                }
            };
            match send(&mut stream, &response).await {
                Ok(_) => {
                    if keep_alive {
                        continue;
                    } else {
                        return;
                    }
                }, 
                Err(e) => {
                    return;
                    //something went wrong with stream, no keep alive possible.
                }
            }
        } else {
            break_reason = format!("Invalid Request: {}", header_string);
            break;
        }
    }
    let response = Response::new(StatusCode::BadRequest, vec!{}, Some(break_reason), false);
    send(&mut stream, &response).await;
}






