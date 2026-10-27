use std::default;

use crate::scheduler::types::{Job, JobResult};
use crate::http::response::{Response, StatusCode};
use crate::http::utils::get_input;
use crate::scheduler::loops::metrics::model::CTCMetrics;

use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

pub async fn execute(buffer: &[u8], path: &str, j_tx: Sender<Job>, keep_alive: bool, started: Instant) -> Response {
    let input: Vec<u8> =  match get_input(buffer) {
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
            let (tx, rx) = tokio::sync::oneshot::channel::<(JobResult, CTCMetrics)>();
            let inner = wrapper.get("input").unwrap_or_default();
            let raw_input = serde_json::to_vec(inner).unwrap();
            let job = Job {
                path: path.to_string(),
                input: raw_input,
                respond_to: tx,
                ctc_metrics: CTCMetrics {
                    request_processing_time: Some(Instant::now().duration_since(started)),
                    handler_to_scheduler: None,
                    last_action: Instant::now(),
                    sheduler_to_worker: None,
                    execution_time: None,
                    worker_to_scheduler: None,
                    sheduler_to_handler: None,
                    scheduler_to_handler_send: None,
                }
            };
            let waited_to_send;
            let before_send = Instant::now();
            match j_tx.send(job).await {
                Ok(_) => {
                    waited_to_send = Instant::now().duration_since(before_send);
                },
                Err(e) => {
                    let response = Response::new(StatusCode::IntServerError, vec![], Some(e.to_string()), keep_alive);
                    return response;
                }
            }
            
            match rx.await {
                Ok((result, mut ctc_m)) => {
                    ctc_m.sheduler_to_handler = Some(Instant::now().duration_since(ctc_m.last_action));
                    ctc_m.scheduler_to_handler_send = Some(waited_to_send);
                    // println!("ctc {:?}", ctc_m);
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
            }
        }
        Err(e) => {
            println!("Failed to wrap input value {:?}", e);
            
            Response::new(StatusCode::BadRequest, vec![], Some(format!("Failed to wrap input value as a json {}", e)), keep_alive)
        }
    }
}

