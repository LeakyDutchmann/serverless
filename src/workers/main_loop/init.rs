use tokio::sync::mpsc::{Sender, Receiver};
use sqlx::{MySqlPool};
use wasmtime::{Engine, Module};
use tokio::task::JoinHandle;
use tokio::select;
use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{interval, Duration};

use crate::workers::model::{CacherTelemetry, WorkerSignal, Message};
use super::handler::handle_job;

pub async fn start_main_loop(
    engine: Engine,
    db_pool: MySqlPool,
    tl_tx: Sender<CacherTelemetry>,
    fb_tx: Sender<WorkerSignal>,
    cache: Arc<RwLock<HashMap<String, Module>>>,
    jobs: Arc<RwLock<Vec<JoinHandle<()>>>>,
    id: usize,
    mut rx: Receiver<Message>,
) -> JoinHandle<()> {
    let mut heartbeat = interval(Duration::from_secs(3));
    tokio::spawn(async move {
        let engine = engine.clone();
        loop {
            let engine = engine.clone();
            let db = db_pool.clone();
            let fb = fb_tx.clone();
            let tl = tl_tx.clone();
            select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        Message::Stop(reason) => {
                            println!("Worker {} received stop signal because of this reason: {}", id, reason);
                            let mut jobs = jobs.write().await;
                            println!("Aborting {} jobs", jobs.len());
                            for job in jobs.iter() {
                                job.abort();
                            }
                            jobs.clear();
                            println!("Worker {} stopped", id);
                            break;
                        },
                        Message::Job{path, input, j_id} => {
                            let cache_map = Arc::clone(&cache);
                            let job = tokio::spawn(async move {
                                let _ = fb.send(WorkerSignal::Working{w_id: id, j_id}).await;
                                match handle_job(path.clone(), engine, cache_map, &input, db).await {
                                    Ok(result) => {
                                        let _ = fb.send(WorkerSignal::Finished{w_id: id, j_id, result}).await;
                                    },
                                    Err(e) => {
                                        let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                    }
                                }
                                let _ = tl.send(CacherTelemetry::ModuleUsed{path}).await;
                            });
                            let mut jobs = jobs.write().await;
                            jobs.push(job);       
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    let result = fb_tx.send(WorkerSignal::HeartBeat{w_id: id}).await;
                    match result {
                        Ok(_) => {continue}
                        Err(e) => {
                            let mut jobs = jobs.write().await;
                            println!("Failed to send heartbeat: {:?}. Aborting all jobs from current worker", e);
                            for job in jobs.iter() {
                                job.abort();
                            }
                            jobs.clear();
                            break;
                        }
                    }
                }
            }
        }
    })
}

