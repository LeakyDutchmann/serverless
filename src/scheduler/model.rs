use crate::workers::model::{Worker, WorkerSignal, Message, CacherTelemetry, CacheErr};
use crate::http::response::{Response, StatusCode, send};

use super::loops::load_loop::model::start_load_loop;
use super::loops::hb_loop::model::start_hb_loop;
use super::loops::gcc_loop::model::{start_gcc_loop, GCCSignal, BcastSender};
use super::loops::main_loop::model::start_main_loop;

use tokio::{sync::mpsc::{Receiver, Sender, channel}, time::{Instant, interval}};
use sqlx::{MySqlPool, Row};
use tokio::task::JoinHandle;
use tokio::net::TcpStream;
use tokio::select;
use wasmparser::TableType;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::RwLock;
use tokio::time::Duration;
use std::collections::BinaryHeap;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use ordered_float::OrderedFloat;


pub struct Job {
    pub path: String,
    pub input: Vec<u8>,
    pub stream: TcpStream,
}

pub enum SchedulerCommand {
    Upgrade(usize),
    Downgrade(Vec<usize>),
    DropDeadWorker(usize)
}


pub struct Scheduler {
    pub max_workers: usize,
    pub workers: Arc<RwLock<Vec<Worker>>>,
    pub rx: Option<Receiver<Job>>,
    pub load_rx: Option<Receiver<usize>>,
    pub gcc_tx: Option<BcastSender<GCCSignal>>,
    pub telemetry_tx: Option<Sender<CacherTelemetry>>,
    pub telemetry_rx: Option<Receiver<CacherTelemetry>>,
    pub feedback_tx: Option<Sender<WorkerSignal>>,
    pub feedback_rx: Option<Receiver<WorkerSignal>>,
    pub scheduler_task: Option<JoinHandle<()>>,
    pub heartbeat_task: Option<JoinHandle<()>>,
    pub load_task: Option<JoinHandle<()>>,
    pub db_pool: MySqlPool,
    pub load_map: Arc<RwLock<HashMap<usize, usize>>>,
}

static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(1);

pub struct ModuleStats {
    pub memory_usage: usize,
    pub invokations: u64,
    pub first_invocation: Instant,
    pub last_invocation: Instant,
    pub pre_last_invocation: Option<Instant>,
    pub last_eviction: Option<Instant>,
    pub cached_instances: u64,
}

const MAX_STATS_ENTRIES: usize = 50_000;

impl Scheduler {
    pub async fn initialize(worker_amount: usize, max_workers: usize, rx: Receiver<Job>, db_pool: MySqlPool) -> Self {
        let mut workers = Vec::new();
        let mut load_map = HashMap::new();
        let (fb_tx, fb_rx) = channel::<WorkerSignal>(1024);
        let (tl_tx, tl_rx) = channel::<CacherTelemetry>(1024);
        let (gcc_tx, _) = tokio::sync::broadcast::channel::<GCCSignal>(1024);
        for i in 1..=worker_amount {
            let pool = db_pool.clone();
            let worker = Worker::spawn(i, pool, fb_tx.clone(), tl_tx.clone(), gcc_tx.clone()).await;
            load_map.insert(i, 0);
            workers.push(worker);
        }
        let scheduler = Scheduler {
            max_workers: max_workers,
            workers: Arc::new(RwLock::new(workers)),
            rx: Some(rx),
            gcc_tx: Some(gcc_tx),
            telemetry_tx: Some(tl_tx),
            telemetry_rx: Some(tl_rx),
            feedback_tx: Some(fb_tx),
            feedback_rx: Some(fb_rx),
            load_rx: None,
            load_task: None,
            scheduler_task: None,
            heartbeat_task: None,
            db_pool,
            load_map: Arc::new(RwLock::new(load_map)),
        }; 
        scheduler
    }
    pub async fn run(&mut self) {
        if self.rx.is_none() || self.feedback_rx.is_none() {
            panic!("Main feedback and tasks receivers not found for scheduler, panicking!");
        }
        let mut job_rx = self.rx.take().expect("Scheduler job receiver not found. FATAL: panicking");
        let mut tl_rx = self.telemetry_rx.take().expect("Scheduler telemetry sender not found. FATAL: panicking");
        let mut feedback_rx = self.feedback_rx.take().expect("Schedulet feedback receiver not found");

        let (load_tx, mut load_rx) = channel::<SchedulerCommand>(1024);

        let heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>> = Arc::new(RwLock::new(HashMap::new()));
        let load_map = Arc::clone(&self.load_map);
        let job_map: Arc<RwLock<HashMap<usize, TcpStream>>> = Arc::new(RwLock::new(HashMap::new()));
        let stats_map: Arc<RwLock<HashMap<String, ModuleStats>>> = Arc::new(RwLock::new(HashMap::new()));
        let s_map = Arc::clone(&stats_map);
        let l_map = Arc::clone(&load_map);
        let l_map_2 = Arc::clone(&l_map);
        let h_map = Arc::clone(&heartbeat_map);
        let forbidden_paths: Arc<RwLock<HashSet<String>>> = Arc::new(RwLock::new(HashSet::new()));
        let f_paths = Arc::clone(&forbidden_paths);
        let db_pool = self.db_pool.clone();
        let feedback_tx = self.feedback_tx.clone().expect("Scheduler feedback sender not found. FATAL: panicking");
        let tl_tx = self.telemetry_tx.clone().expect("Scheduler telemetry sender not found. FATAL: panicking");
        let gcc_tx = self.gcc_tx.clone().expect("Scheduler gcc sender not found. FATAL: panicking");
        let cache_tx = gcc_tx.clone();
        let workers = Arc::clone(&self.workers);
        let workers_clone = Arc::clone(&self.workers);
        let workers_cloned = Arc::clone(&self.workers);

        let cache_memory_usage: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let cache_memory_usage_clone = Arc::clone(&cache_memory_usage);
    
        
        let main_loop = start_main_loop(
            feedback_rx,
            load_map,
            job_map,
            heartbeat_map,
            job_rx,
            workers_clone,
            l_map_2,
            load_rx,
            db_pool,
            feedback_tx,
            tl_tx,
            tl_rx,
            gcc_tx,
            forbidden_paths,
            cache_memory_usage,
            stats_map,
        ).await;
        let gcc_loop = start_gcc_loop(workers_cloned, s_map, f_paths, cache_memory_usage_clone, cache_tx).await;
        
        let hb_tx = load_tx.clone();
        let heartbeat_loop = start_hb_loop(hb_tx, h_map).await;
        let max_workers = self.max_workers;
        let load_tx = load_tx.clone();
        let load_loop = start_load_loop(max_workers, load_tx, l_map).await;
        self.load_task = Some(load_loop);
        self.heartbeat_task = Some(heartbeat_loop);
        self.scheduler_task = Some(main_loop);
    }
}

pub async fn upgrade(workers: Arc<RwLock<Vec<Worker>>>, amount: usize, db_pool: MySqlPool, tx: Sender<WorkerSignal>, tl_tx: Sender<CacherTelemetry>, gcc_tx: BcastSender<GCCSignal>, l_map: Arc<RwLock<HashMap<usize, usize>>>) {
    let mut workers = workers.write().await;
    let mut l_map = l_map.write().await;
    let last_id = workers.len();
    for id in last_id + 1..=last_id + amount {
        let worker = Worker::spawn(id, db_pool.clone(), tx.clone(), tl_tx.clone(), gcc_tx.clone()).await;
        workers.push(worker);
        l_map.insert(id, 0);
    }
    println!("Upgraded {} workers", amount);
}

pub async fn downgrade(workers: Arc<RwLock<Vec<Worker>>>, to_remove: Vec<usize>, l_map: Arc<RwLock<HashMap<usize, usize>>>) {
    let mut workers = workers.write().await;
    let mut l_map = l_map.write().await;
    for worker in workers.iter() {
        if to_remove.contains(&worker.id) {
            let _ = worker.sender.send(Message::Stop("Worker {} was downgraded".into())).await;
            l_map.remove(&worker.id);
        }
    }
    workers.retain(|w| !to_remove.contains(&w.id));
    println!("Downgraded {} workers", to_remove.len());
}

pub async fn drop_dead_worker(workers: Arc<RwLock<Vec<Worker>>>, to_remove: usize, l_map: Arc<RwLock<HashMap<usize, usize>>>) {
    let mut workers = workers.write().await;
    let mut l_map = l_map.write().await;
    if let Some(pos) = workers.iter().position(|w| w.id == to_remove) {
        workers[pos].main_loop.abort();
        workers[pos].jobs.read().await.iter().for_each(|j| j.abort());
        workers.remove(pos);
        l_map.remove(&to_remove);
        println!("Dead worker {} dropped", to_remove);
    }
}

pub async fn generate_job_id() -> usize {
    let id = NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed);
    id
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