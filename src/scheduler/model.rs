use crate::workers::model::{Worker, WorkerId, WorkerLoad};
use crate::sharded::{ShardedMap, Sharded};
use super::shutdown::Shutdown;
use super::loops::load_loop::model::start_load_loop;
use super::loops::hb_loop::model::start_hb_loop;
use super::loops::gcc_loop::model::start_gcc_loop;
use super::loops::main_loop::model::start_main_loop;
use super::loops::metrics::metrics_loop::start_metrics_loop;
use super::types::{Job, ModuleStats, InternalChannels, ExternalChannels, RuntimeTasks, SchedulerCommand, JobResult};

use tokio::time::interval;
use tokio::{sync::mpsc::{Receiver, channel, Sender}, time::Instant};
use sqlx::MySqlPool;
use tokio::sync::oneshot::Sender as OneShotSender;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicUsize}};
use std::time::Duration;
use tokio::sync::RwLock;

struct RuntimeState {
    heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>>,
    job_map: Arc<RwLock<HashMap<usize, OneShotSender<JobResult>>>>,
    stats_map: Arc<ShardedMap<String, ModuleStats>>,
    forbidden_paths: Arc<RwLock<HashSet<String>>>
}

impl RuntimeState {
    fn new() -> RuntimeState {
        RuntimeState {
            heartbeat_map: Arc::new(RwLock::new(HashMap::new())),
            job_map: Arc::new(RwLock::new(HashMap::new())),
            stats_map: Arc::new(ShardedMap::new(64)),
            forbidden_paths: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}

pub static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(1);

pub struct Scheduler {
    max_workers: usize,
    workers: Arc<RwLock<Vec<Worker>>>,
    ext_channels: ExternalChannels,
    int_channels: Option<InternalChannels>,
    tasks: Option<RuntimeTasks>,
    db_pool: MySqlPool,
    load_map: Arc<ShardedMap<WorkerId, WorkerLoad>>,
}

impl Scheduler {
    pub async fn initialize(worker_amount: usize, max_workers: usize, job_rx: Receiver<Job>, db_pool: MySqlPool, shutdown_tx: Sender<Shutdown>) -> Self {
        let mut workers = Vec::new();
        let mut load_map: ShardedMap<WorkerId, WorkerLoad> = ShardedMap::new(12);
        let int_channels = InternalChannels::init();
        let ext_channels = ExternalChannels::init(job_rx, shutdown_tx);
        
        for i in 1..=worker_amount {
            let pool = db_pool.clone();
            let worker = Worker::spawn(i, pool, int_channels.feedback.tx.clone(), int_channels.telemetry.tx.clone(), ext_channels.gcc_tx.clone().unwrap(), ext_channels.metrics_tx.clone()).await;
            load_map.insert(i, WorkerLoad::default());
            workers.push(worker);
        }
        
         
        Scheduler {
            max_workers,
            workers: Arc::new(RwLock::new(workers)),
            int_channels: Some(int_channels),
            ext_channels,
            tasks: None,
            db_pool,
            load_map: Arc::new(load_map),
        }
    }
    pub async fn run(&mut self) {
        if self.int_channels.is_none() {
            panic!("Scheduler internal channels not found, panicking!");
        }
        if self.ext_channels.job_rx.is_none() {
            panic!("Main tasks receiver not found for scheduler, panicking!");
        }
        let (load_tx, load_rx) = channel::<SchedulerCommand>(1024);
        
        let load_map = Arc::clone(&self.load_map);
        let db_pool = self.db_pool.clone();
        
        let gcc_tx = self.ext_channels.gcc_tx.clone().expect("Scheduler gcc sender not found. FATAL: panicking");
        let workers = Arc::clone(&self.workers);
        let cache_memory_usage: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let next_worker: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(1));

        let state = RuntimeState::new();
        let main_loop = start_main_loop(
            Arc::clone(&load_map),
            state.job_map,
            Arc::clone(&state.heartbeat_map),
            self.ext_channels.job_rx.take().unwrap(),
            Arc::clone(&workers),
            load_rx,
            db_pool.clone(),
            self.int_channels.take().unwrap(),
            gcc_tx.clone(),
            Arc::clone(&state.forbidden_paths),
            Arc::clone(&cache_memory_usage),
            Arc::clone(&next_worker),
            Arc::clone(&state.stats_map),
            self.ext_channels.shutdown_tx.take().unwrap(),
            self.ext_channels.metrics_tx.clone()
        ).await;
        let gcc_loop = start_gcc_loop(Arc::clone(&workers), state.stats_map, state.forbidden_paths, cache_memory_usage, gcc_tx).await;
        let heartbeat_loop = start_hb_loop(load_tx.clone(), state.heartbeat_map).await;
        let max_workers = self.max_workers;
        let load_loop = start_load_loop(max_workers, load_tx, load_map).await;

        let m_rx = self.ext_channels.metrics_rx.take().unwrap();
        let metrics_loop = start_metrics_loop(
            m_rx,
            db_pool.clone()
        );
        //probably dev tool
        let _pool_monitor = tokio::spawn(async move {
            let pool = db_pool;
            let mut int = interval(Duration::from_secs(4));
            loop {
                int.tick().await;
                println!("DB Connections {}, from them idle {}", pool.size(), pool.num_idle());
            }
        });
        let tasks = RuntimeTasks {
            scheduler_task: main_loop,
            heartbeat_task: heartbeat_loop,
            load_task: load_loop,
            gcc_task: gcc_loop,
            metrics_task: metrics_loop,
        };
        self.tasks = Some(tasks);
    }
    pub async fn shutdown(&mut self) {
        let mut workers = self.workers.write().await;
        for worker in workers.iter_mut() {
            println!("Stopping worker {}", worker.id);
            worker.stop().await; 
        }
        workers.clear();
        self.tasks.as_mut().unwrap().abort();
        self.load_map.clear().await;
    }
}

