use crate::workers::model::Worker;

use super::loops::load_loop::model::start_load_loop;
use super::loops::hb_loop::model::start_hb_loop;
use super::loops::gcc_loop::model::start_gcc_loop;
use super::loops::main_loop::model::start_main_loop;
use super::types::{Job, ModuleStats, InternalChannels, ExternalChannels, RuntimeTasks, SchedulerCommand};

use tokio::{sync::mpsc::{Receiver, channel}, time::Instant};
use sqlx::MySqlPool;
use tokio::net::TcpStream;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicUsize}};
use tokio::sync::RwLock;

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

pub static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(1);

const MAX_STATS_ENTRIES: usize = 50_000;

struct Scheduler {
    max_workers: usize,
    workers: Arc<RwLock<Vec<Worker>>>,
    ext_channels: ExternalChannels,
    int_channels: Option<InternalChannels>,
    tasks: RuntimeTasks,
    db_pool: MySqlPool,
    load_map: Arc<RwLock<HashMap<usize, usize>>>,
}

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

