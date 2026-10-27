use crate::{scheduler::loops::metrics::model::CTCMetrics, workers::model::{WorkerId, WorkerLoad, WorkerSignal}};
use crate::scheduler::types::JobResult;
use crate::sharded::ShardedMap;

use tokio::time::Instant;
use tokio::sync::oneshot::Sender as OneShotSender;
use std::sync::Arc;

pub async fn handle_feedback(
    job_map: Arc<ShardedMap<usize, OneShotSender<(JobResult, CTCMetrics)>>>,
    load_map: Arc<ShardedMap<WorkerId, WorkerLoad>>, 
    heartbeat_map: Arc<ShardedMap<WorkerId, Instant>>,
    signal: WorkerSignal
)  {
    let tracked_closure = |l: &mut WorkerLoad| -> bool {
        if l.task_count > 0 {
            l.task_count -= 1;
            true
        } else {
            false
        }
    };
    
    match signal {
        WorkerSignal::HeartBeat { w_id } => {
            heartbeat_map.insert(w_id, Instant::now()).await;
        }
        WorkerSignal::Working {w_id, j_id: _} => {   
            if let Some(_) = load_map.modify(&w_id, |l| {l.task_count += 1; l.last_task_time = Instant::now()}).await {
                // println!("Worker {} started task {}", w_id, j_id);
            } else {
                load_map.insert(w_id, WorkerLoad { task_count: 1, last_task_time: Instant::now() }).await;
            }
        }
        WorkerSignal::Finished {w_id, j_id, result, mut ctc_m} => {
            ctc_m.worker_to_scheduler = Some(Instant::now().duration_since(ctc_m.last_action));
            ctc_m.last_action = Instant::now();
            let tx = job_map.remove(&j_id).await;
            
            if let Some(tx) = tx {
                let tracked = load_map.modify(&w_id, tracked_closure).await.unwrap_or(false);
                
                if !tracked {
                    println!("Worker {} finished untracked task", w_id);
                }
                
                tx.send((JobResult::from_ok(result), ctc_m));
            } else {
                println!("Worker {} finished task that belongs to no client. Task id: {}", w_id, j_id);
            }     
        }
        WorkerSignal::Failed {w_id, j_id, reason, mut ctc_m} => {
            ctc_m.worker_to_scheduler = Some(Instant::now().duration_since(ctc_m.last_action));
            ctc_m.last_action = Instant::now();
            let tx = job_map.remove(&j_id).await;
            
            if let Some(tx) = tx {
                let tracked = load_map.modify(&w_id, tracked_closure).await.unwrap_or(false);

                if !tracked {
                    println!("Worker {} failed untracked task, reason: {}", w_id, reason);
                }
                
                tx.send((JobResult::server_err(reason.clone()),ctc_m));
                
            } else {
                println!("Worker {} failed task that belongs to no client. Task id: {}, reason {}", w_id, j_id, reason);
            }   
        }
    }
}