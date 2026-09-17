use crate::workers::model::WorkerSignal;
use crate::http::response::{Response, StatusCode, send};

use tokio::time::Instant;
use tokio::net::TcpStream;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn handle_feedback(
    job_map: Arc<RwLock<HashMap<usize, TcpStream>>>,
    load_map: Arc<RwLock<HashMap<usize, usize>>>, 
    heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>>,
    signal: WorkerSignal
)  {
    let mut load_map = load_map.write().await;
    let mut job_map = job_map.write().await;
    match signal {
        WorkerSignal::HeartBeat { w_id } => {
            let mut map = heartbeat_map.write().await;
            map.insert(w_id, Instant::now());
        }
        WorkerSignal::Working {w_id, j_id} => {   
            if let Some(load) = load_map.get_mut(&w_id) {
                *load += 1;
                println!("Worker {} started task {}", w_id, j_id);
            } else {
                load_map.insert(w_id, 1);
            }
        }
        WorkerSignal::Finished {w_id, j_id, result} => {
            if let Some(stream) = job_map.get_mut(&j_id) {
                if let Some(load) = load_map.get_mut(&w_id) {
                    if *load != 0 {
                        *load -= 1;
                        println!("Worker {} finished task {}", w_id, j_id);
                        let response = Response::json(StatusCode::Ok, result, None);
                        send(stream, &response).await;
                    } else {
                        let response = Response::json(StatusCode::Ok, result, None);
                        send(stream, &response).await;
                        println!("Worker {} finished untracked task", w_id);
                    }      
                } else {
                    let response = Response::json(StatusCode::Ok, result, None);
                    send(stream, &response).await;
                    println!("Worker {} finished untracked task", w_id);
                }  
                job_map.remove(&j_id);
            } else {
                println!("Worker {} finished task that belongs to no client. Task id: {}", w_id, j_id);
            }     
        }
        WorkerSignal::Failed {w_id, j_id, reason} => {
            if let Some(stream) = job_map.get_mut(&j_id) {
                if let Some(load) = load_map.get_mut(&w_id) {
                    if *load != 0 {
                        *load -= 1;
                        let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                        send(stream, &response).await;
                        println!("Worker {} failed task {}", w_id, j_id);
                    } else {
                        let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                        send(stream, &response).await;
                        println!("Worker {} failed untracked task", w_id);
                    }      
                } else {
                    let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                    send(stream, &response).await;
                    println!("Worker {} failed untracked task", w_id);
                }
                job_map.remove(&j_id);
            } else {
                println!("Worker {} failed task that belongs to no client. Task id: {}, reason {}", w_id, j_id, reason);
            }   
        }
    }
}