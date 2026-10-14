use crate::workers::model::{{WorkerSignal, WorkerId, WorkerLoad}};
use crate::http::response::{Response, StatusCode, send};
use crate::scheduler::types::JobResult;

use tokio::time::Instant;
use tokio::sync::oneshot::Sender as OneShotSender;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn handle_feedback(
    job_map: Arc<RwLock<HashMap<usize, OneShotSender<JobResult>>>>,
    load_map: Arc<RwLock<HashMap<WorkerId, WorkerLoad>>>, 
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
                load.task_count += 1;
                load.last_task_time = Instant::now();
                // println!("Worker {} started task {}", w_id, j_id);
            } else {
                load_map.insert(w_id, WorkerLoad { task_count: 1, last_task_time: Instant::now() });
            }
        }
        WorkerSignal::Finished {w_id, j_id, result} => {
            if let Some(tx) = job_map.remove(&j_id) {
                if let Some(load) = load_map.get_mut(&w_id) {
                    if load.task_count != 0 {
                        load.task_count -= 1;
                        // println!("Worker {} finished task {}", w_id, j_id);
                        tx.send(JobResult::from_ok(result));
                    } else {
                        tx.send(JobResult::from_ok(result));
                        // println!("Worker {} finished untracked task", w_id);
                    }      
                } else {
                    tx.send(JobResult::from_ok(result));
                    // println!("Worker {} finished untracked task", w_id);
                }  
                job_map.remove(&j_id);
            } else {
                println!("Worker {} finished task that belongs to no client. Task id: {}", w_id, j_id);
            }     
        }
        WorkerSignal::Failed {w_id, j_id, reason} => {
            if let Some(tx) = job_map.remove(&j_id) {
                if let Some(load) = load_map.get_mut(&w_id) {
                    if load.task_count != 0 {
                        load.task_count -= 1;
                        tx.send(JobResult::server_err(reason.clone()));
                        println!("Worker {} failed task {} : Reason: {}", w_id, j_id, reason);
                    } else {
                        tx.send(JobResult::server_err(reason.clone()));
                        println!("Worker {} failed untracked task, reason: {}", w_id, reason);
                    }      
                } else {
                    tx.send(JobResult::server_err(reason.clone()));
                    println!("Worker {} failed untracked task, reason: {}", w_id, reason);
                }
            } else {
                println!("Worker {} failed task that belongs to no client. Task id: {}, reason {}", w_id, j_id, reason);
            }   
        }
    }
}