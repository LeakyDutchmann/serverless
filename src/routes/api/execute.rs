use crate::scheduler::model::Job;
use crate::http::response::{Response, StatusCode, send};
use crate::http::utils::{get_body_idx, get_body_len, get_input};

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

pub async fn execute(mut stream: TcpStream, buffer: &[u8], path: &str, tx: Sender<Job>) {
    let input: Vec<u8> =  match get_input(&buffer) {
        Ok(input) => input,
        Err(e) => {
            println!("Failed to get input: {:?}", e);
            let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("Failed to get input: {}", e)));
            send(&mut stream, &response).await;
            return;
        }
    };
    let result: Result<serde_json::Value, serde_json::Error> = if input.is_empty() {
        Ok(serde_json::json!({}))
    } else {
        serde_json::from_slice(&input)
    };
    match result {
        Ok(wrapper) => {
            let inner = wrapper.get("input").unwrap_or_default();
            let raw_input = serde_json::to_vec(inner).unwrap();
            let job = Job {
                path: path.to_string(),
                input: raw_input,
                stream,
            };
            let _ = tx.send(job).await;
        }
        Err(e) => {
            println!("Failed to wrap input value {:?}", e);
            let response = Response::json(StatusCode::BadRequest, vec![], Some(format!("Failed to wrap input value as a json {}", e)));
            send(&mut stream, &response).await;
        }
    }
    
}

