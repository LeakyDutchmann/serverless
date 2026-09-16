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


struct Scheduler {
    max_workers: usize,
    workers: Arc<RwLock<Vec<Worker>>>,
    ext_channels: ExternalChannels,
    int_channels: Option<InternalChannels>,
    tasks: RuntimeTasks,
    db_pool: MySqlPool,
    load_map: Arc<RwLock<HashMap<usize, usize>>>,
}

struct RuntimeTasks {
    pub scheduler_task: Option<JoinHandle<()>>,
    pub heartbeat_task: Option<JoinHandle<()>>,
    pub load_task: Option<JoinHandle<()>>,
    pub gcc_task: Option<JoinHandle<()>>,
}

impl RuntimeTasks {
    fn empty() -> RuntimeTasks {
        RuntimeTasks {
            scheduler_task: None,
            heartbeat_task: None,
            load_task: None,
            gcc_task: None,
        }
    }
}

struct ExternalChannels {
    gcc_tx: Option<BcastSender<GCCSignal>>,
    load_rx: Option<Receiver<usize>>,
    job_rx: Option<Receiver<Job>>,
}
impl ExternalChannels {
    fn init(job_rx: Receiver<Job>) -> ExternalChannels {
        let (gcc_tx, _) = tokio::sync::broadcast::channel::<GCCSignal>(1024);
        ExternalChannels {
            gcc_tx: Some(gcc_tx),
            load_rx: None,
            job_rx: Some(job_rx),
        }
    }
}

pub struct InternalChannels {
    pub feedback: Channel<WorkerSignal>,
    pub telemetry: Channel<CacherTelemetry>,
}

impl InternalChannels {
    fn init() -> InternalChannels {
        let feeedback_channel: Channel<WorkerSignal> = Channel::new(1024);
        let telemetry_channel: Channel<CacherTelemetry> = Channel::new(1024);
        InternalChannels {
            feedback: feeedback_channel,
            telemetry: telemetry_channel,
        }
    }
}

pub struct Channel<T> {
    pub tx: Sender<T>,
    pub rx: Option<Receiver<T>>,
}

impl<T> Channel<T> {
    fn new(buffer_size: usize) -> Channel<T> {
        let (tx, rx) = tokio::sync::mpsc::channel::<T>(buffer_size);
        Channel { tx, rx: Some(rx) }
    }
}

struct RuntimeState {
    heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>>,
    job_map: Arc<RwLock<HashMap<usize, TcpStream>>>,
    stats_map: Arc<RwLock<HashMap<String, ModuleStats>>>,
    forbidden_paths: Arc<RwLock<HashSet<String>>>
}

impl RuntimeState {
    fn new() -> RuntimeState {
        RuntimeState {
            heartbeat_map: Arc::new(RwLock::new(HashMap::new())),
            job_map: Arc::new(RwLock::new(HashMap::new())),
            stats_map: Arc::new(RwLock::new(HashMap::new())),
            forbidden_paths: Arc::new(RwLock::new(HashSet::new())),
        }
    }
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
    pub async fn initialize(worker_amount: usize, max_workers: usize, job_rx: Receiver<Job>, db_pool: MySqlPool) -> Self {
        let mut workers = Vec::new();
        let mut load_map = HashMap::new();
        let int_channels = InternalChannels::init();
        let ext_channels = ExternalChannels::init(job_rx);
        
        for i in 1..=worker_amount {
            let pool = db_pool.clone();
            let worker = Worker::spawn(i, pool, int_channels.feedback.tx.clone(), int_channels.telemetry.tx.clone(), ext_channels.gcc_tx.clone().unwrap()).await;
            load_map.insert(i, 0);
            workers.push(worker);
        }
        
        let scheduler = Scheduler {
            max_workers: max_workers,
            workers: Arc::new(RwLock::new(workers)),
            int_channels: Some(int_channels),
            ext_channels: ext_channels,
            tasks: RuntimeTasks::empty(),
            db_pool,
            load_map: Arc::new(RwLock::new(load_map)),
        }; 
        scheduler
    }
    pub async fn run(&mut self) {
        if self.int_channels.is_none() {
            panic!("Scheduler internal channels not found, panicking!");
        }
        if self.ext_channels.job_rx.is_none() || self.int_channels.as_ref().unwrap().feedback.rx.is_none() {
            panic!("Main feedback or tasks receivers not found for scheduler, panicking!");
        }
        let (load_tx, load_rx) = channel::<SchedulerCommand>(1024);
        
        let load_map = Arc::clone(&self.load_map);
        let db_pool = self.db_pool.clone();
        
        let gcc_tx = self.ext_channels.gcc_tx.clone().expect("Scheduler gcc sender not found. FATAL: panicking");
        let workers = Arc::clone(&self.workers);
        let cache_memory_usage: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        let state = RuntimeState::new();
        let main_loop = start_main_loop(
            Arc::clone(&load_map),
            state.job_map,
            Arc::clone(&state.heartbeat_map),
            self.ext_channels.job_rx.take().unwrap(),
            Arc::clone(&workers),
            load_rx,
            db_pool,
            self.int_channels.take().unwrap(),
            gcc_tx.clone(),
            Arc::clone(&state.forbidden_paths),
            Arc::clone(&cache_memory_usage),
            Arc::clone(&state.stats_map),
        ).await;
        let gcc_loop = start_gcc_loop(Arc::clone(&workers), state.stats_map, state.forbidden_paths, cache_memory_usage, gcc_tx).await;
        let heartbeat_loop = start_hb_loop(load_tx.clone(), state.heartbeat_map).await;
        let max_workers = self.max_workers;
        let load_loop = start_load_loop(max_workers, load_tx, load_map).await;

        let tasks = RuntimeTasks {
            scheduler_task: Some(main_loop),
            heartbeat_task: Some(heartbeat_loop),
            load_task: Some(load_loop),
            gcc_task: Some(gcc_loop),
        };
        self.tasks = tasks;
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