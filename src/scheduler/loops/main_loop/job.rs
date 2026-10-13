use crate::workers::model::{Worker, Message};
use crate::http::response::{Response, StatusCode, send};
use crate::scheduler::types::{Job, JobResult};

use crate::scheduler::utils::generate_job_id;

use tokio::sync::oneshot::Sender as OneShotSender;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn handle_job(
    workers: Arc<RwLock<Vec<Worker>>>,
    next_worker: Arc<AtomicUsize>,
    job_map: Arc<RwLock<HashMap<usize, OneShotSender<JobResult>>>>,
    mut task: Job
) {
    tokio::spawn(async move {
        let workers = workers.read().await;
        let id = next_worker.fetch_add(1, Ordering::SeqCst) + 1;
        let id = if id == 0{
            next_worker.store(1, Ordering::SeqCst);
            1
        } else if id > workers.len() {
            next_worker.store(1, Ordering::SeqCst);
            1
        } else {
            id
        };
        if let Some(worker) = workers.iter().find(|w| w.id == id) {
            let j_id = generate_job_id().await;
            let result = worker.sender.send(Message::Job{path: task.path, input: task.input, j_id}).await;
            match result {
                Ok(_) => {
                    let mut map = job_map.write().await;
                    map.insert(j_id, task.respond_to);
                }
                Err(e) => {
                    let line = format!("Failed to send job to worker {}", e);
                    task.respond_to.send(JobResult::server_err(line));
                    println!("Failed to send job to worker: {}", e)
                },
            }
        } else {
            let vec = workers.iter().map(|w| w.id.clone()).collect::<Vec<_>>();
            let s = vec.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ");
            let line = format!("Failed to attach task to a worker because of inconsistent worker id's");
            task.respond_to.send(JobResult::server_err(line));
            println!("Tried to attach job to worker with id {}, but there is no such worker in worker pool. Workers available: {}", id, s);
        }
    });
}
