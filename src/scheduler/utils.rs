use crate::workers::model::{Worker, WorkerSignal, Message, CacherTelemetry, WorkerId, WorkerLoad};
use super::model::NEXT_JOB_ID;
use super::loops::gcc_loop::model::{GCCSignal, BcastSender};
use crate::scheduler::loops::metrics::model::MetricsPacket;
use crate::sharded::ShardedMap;

use tokio::sync::mpsc::Sender;
use sqlx::MySqlPool;
use std::collections::HashMap;
use std::sync::{Arc, atomic::Ordering};
use tokio::sync::RwLock;

pub async fn upgrade(workers: Arc<RwLock<Vec<Worker>>>, amount: usize, db_pool: MySqlPool, tx: Sender<WorkerSignal>, tl_tx: Sender<CacherTelemetry>, gcc_tx: BcastSender<GCCSignal>, l_map: Arc<ShardedMap<WorkerId, WorkerLoad>>, m_tx: Sender<(String, MetricsPacket)>) {
    let mut workers = workers.write().await;
    let last_id = workers.len();
    for id in last_id + 1..=last_id + amount {
        let worker = Worker::spawn(WorkerId(id), db_pool.clone(), tx.clone(), tl_tx.clone(), gcc_tx.clone(), m_tx.clone()).await;
        workers.push(worker);
        l_map.insert(WorkerId(id), WorkerLoad::default()).await;
    }
    println!("Upgraded {} workers", amount);
}

pub async fn downgrade(workers: Arc<RwLock<Vec<Worker>>>, to_remove: WorkerId, l_map: Arc<ShardedMap<WorkerId, WorkerLoad>>) {
    let mut workers = workers.write().await;
    for worker in workers.iter() {
        if to_remove == worker.id {
            let _ = worker.sender.send(Message::Stop("Worker {} was downgraded".into())).await;
            l_map.remove(&worker.id).await;
        }
    }
    workers.retain(|w| w.id != to_remove);
    println!("Downgraded {} worker", to_remove);
}

pub async fn drop_dead_worker(workers: Arc<RwLock<Vec<Worker>>>, to_remove: WorkerId, l_map: Arc<ShardedMap<WorkerId, WorkerLoad>>) {
    let mut workers = workers.write().await;
    if let Some(pos) = workers.iter().position(|w| w.id == to_remove) {
        workers[pos].main_loop.abort();
        workers[pos].jobs.read().await.iter().for_each(|j| j.abort());
        workers.remove(pos);
        l_map.remove(&to_remove).await;
        println!("Dead worker {} dropped", to_remove);
    }
}

pub async fn generate_job_id() -> usize {
    
    NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed)
}


#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn id_gen() {
        let id = generate_job_id().await;
        assert!(id > 0);
    }
}