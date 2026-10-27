use crate::scheduler::loops::metrics::model::CTCMetrics;
use crate::workers::model::{Worker, Message};
use crate::scheduler::types::{Job, JobResult};

use crate::scheduler::utils::generate_job_id;
use crate::sharded::ShardedMap;

use tokio::sync::oneshot::Sender as OneShotSender;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::Instant;

pub async fn handle_job(
    workers: Arc<RwLock<Vec<Worker>>>,
    next_worker: Arc<AtomicUsize>,
    job_map: Arc<ShardedMap<usize, OneShotSender<(JobResult, CTCMetrics)>>>,
    mut task: Job
) {
    task.ctc_metrics.handler_to_scheduler = Some(Instant::now().duration_since(task.ctc_metrics.last_action));
    task.ctc_metrics.last_action = Instant::now();
    let workers = workers.read().await;
    let id = next_worker.fetch_add(1, Ordering::SeqCst);
    let id = id % workers.len();

    let worker = &workers[id];
    let w_tx = worker.sender.clone();
    drop(workers);

    let j_id = generate_job_id().await;
    
    job_map.insert(j_id, task.respond_to).await;

    let result = w_tx.send(Message::Job{path: task.path, input: task.input, j_id, ctc_metrics: task.ctc_metrics.clone()}).await;
    match result {
        Ok(_) => {}
        Err(e) => {
            let sender = job_map.remove(&j_id).await.unwrap();
            let line = format!("Failed to send job to worker {}", e);
            let _ = sender.send((JobResult::server_err(line), task.ctc_metrics));
            println!("Failed to send job to worker: {}", e)
        },
    }
}
