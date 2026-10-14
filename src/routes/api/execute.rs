use crate::scheduler::types::{Job, JobResult};
use crate::http::response::{Response, StatusCode, send};
use crate::http::utils::get_input;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

pub async fn execute(buffer: &[u8], path: &str, j_tx: Sender<Job>, keep_alive: bool) -> Response {
    let input: Vec<u8> =  match get_input(&buffer) {
        Ok(input) => input,
        Err(e) => {
            println!("Failed to get input: {:?}", e);
            let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to get input: {}", e)), keep_alive);
            return response;
        }
    };   
    let result: Result<serde_json::Value, serde_json::Error> = if input.is_empty() {
        Ok(serde_json::json!({}))
    } else {
        serde_json::from_slice(&input)
    };
    match result {
        Ok(wrapper) => {
            let (tx, rx) = tokio::sync::oneshot::channel::<JobResult>();
            let inner = wrapper.get("input").unwrap_or_default();
            let raw_input = serde_json::to_vec(inner).unwrap();
            let job = Job {
                path: path.to_string(),
                input: raw_input,
                respond_to: tx,
            };
            match j_tx.send(job).await {
                Ok(_) => {},
                Err(e) => {
                    let response = Response::new(StatusCode::IntServerError, vec![], Some(e.to_string()), keep_alive);
                    return response;
                }
            }
            let response = match rx.await {
                Ok(result) => {
                    match result {
                        JobResult::Ok{result} => {
                            Response::new(StatusCode::Ok, result, None, keep_alive)
                        },
                        JobResult::BadRequest{reason} => {
                            Response::new(StatusCode::BadRequest, vec![], Some(reason), keep_alive)
                        },
                        JobResult::ServerErr{reason} => {
                            Response::new(StatusCode::IntServerError, vec![], Some(reason), keep_alive)
                        }
                    }
                },
                Err(e) => {
                    Response::new(StatusCode::IntServerError, vec![], Some(e.to_string()), keep_alive)
                }
            };
            return response;
        }
        Err(e) => {
            println!("Failed to wrap input value {:?}", e);
            let response = Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to wrap input value as a json {}", e)), keep_alive);
            return response;
        }
    }
}

