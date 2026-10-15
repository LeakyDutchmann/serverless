use crate::workers::model::{Worker, Message};
use crate::http::response::{Response, StatusCode, send};
use crate::scheduler::types::{Job, JobResult};

use crate::scheduler::utils::generate_job_id;

use tokio::sync::oneshot::Sender as OneShotSender;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn handle_job(
    workers: Arc<RwLock<Vec<Worker>>>,
    next_worker: Arc<AtomicUsize>,
    job_map: Arc<RwLock<HashMap<usize, OneShotSender<JobResult>>>>,
    task: Job
) {
    tokio::spawn(async move {
        let workers = workers.read().await;
        let id = next_worker.fetch_add(1, Ordering::SeqCst);
        let id = id % workers.len();

        let worker = &workers[id];
        let w_tx = worker.sender.clone();
        drop(workers);
    
        let j_id = generate_job_id().await;
        
        let mut map = job_map.write().await;
        map.insert(j_id, task.respond_to);
        drop(map);

        let result = w_tx.send(Message::Job{path: task.path, input: task.input, j_id}).await;
        match result {
            Ok(_) => {}
            Err(e) => {
                let mut map = job_map.write().await;
                let sender = map.remove(&j_id).unwrap();
                drop(map);
                let line = format!("Failed to send job to worker {}", e);
                let _ = sender.send(JobResult::server_err(line));
                println!("Failed to send job to worker: {}", e)
            },
        }
    });
}
