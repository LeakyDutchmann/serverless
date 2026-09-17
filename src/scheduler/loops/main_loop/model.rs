use crate::workers::model::Worker;
use crate::scheduler::types::{Job, ModuleStats, InternalChannels};

use crate::scheduler::types::SchedulerCommand;
use crate::scheduler::loops::gcc_loop::model::{GCCSignal, BcastSender};
use super::feedback::handle_feedback;
use super::job::handle_job;
use super::load::handle_load;
use super::telemetry::handle_telemetry;


use tokio::{sync::mpsc::Receiver, time::Instant};
use sqlx::MySqlPool;
use tokio::task::JoinHandle;
use tokio::net::TcpStream;
use tokio::select;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::AtomicUsize};
use tokio::sync::RwLock;


pub async fn start_main_loop(
    load_map: Arc<RwLock<HashMap<usize, usize>>>,
    job_map: Arc<RwLock<HashMap<usize, TcpStream>>>,
    heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>>,
    mut job_rx: Receiver<Job>,
    workers: Arc<RwLock<Vec<Worker>>>,
    mut load_rx: Receiver<SchedulerCommand>,
    db_pool: MySqlPool,
    mut internal_channels: InternalChannels,
    gcc_tx: BcastSender<GCCSignal>,
    forbidden_paths: Arc<RwLock<HashSet<String>>>,
    cache_memory_usage: Arc<AtomicUsize>,
    stats_map: Arc<RwLock<HashMap<String, ModuleStats>>>,
) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        loop {
            select! {
                Some(worker_signal) = internal_channels.feedback.rx.recv() => {
                    handle_feedback(Arc::clone(&job_map), Arc::clone(&load_map), Arc::clone(&heartbeat_map), worker_signal).await;     
                }
                Some(task) = job_rx.recv() => {
                    handle_job(Arc::clone(&workers), Arc::clone(&load_map), Arc::clone(&job_map), task);
                }
                Some(cmd) = load_rx.recv() => {
                    handle_load(db_pool.clone(), &internal_channels, Arc::clone(&workers), Arc::clone(&load_map), gcc_tx.clone(), cmd).await;
                    
                }
                Some(tl_signal) = internal_channels.telemetry.rx.recv() => {
                    handle_telemetry(Arc::clone(&stats_map), Arc::clone(&forbidden_paths), Arc::clone(&cache_memory_usage), db_pool.clone(), tl_signal).await;
                }
            }
        }
    });
    handle
}