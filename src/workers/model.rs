use crate::scheduler::loops::gcc_loop::model::{BcastSender, GCCSignal};
use crate::scheduler::loops::metrics::model::{CTCMetrics, MetricsPacket};
use super::cache_manager::start_cache_loop;
use super::main_loop::init::start_main_loop;
use crate::sharded::ShardedMap;


use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::channel;
use sqlx::MySqlPool;
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::sync::Arc;
use tokio::time::Instant;
use wasmtime::{Engine, Module};
use std::fmt::{Display, Formatter};
use std::fmt::Error;

#[derive(Clone, Eq, PartialEq, Hash, Copy)]
pub struct WorkerId(pub usize);

impl Display for WorkerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}


#[derive(Clone)]
pub struct WorkerLoad {
    pub task_count: usize,
    pub last_task_time: Instant,
}

impl Default for WorkerLoad {
    fn default() -> Self {
        Self {
            task_count: 0,
            last_task_time: Instant::now(),
        }
    }
}

pub enum Message {
    Stop(String),
    Job{path: String, input: Vec<u8>, j_id: usize, ctc_metrics: CTCMetrics}
}

pub enum WorkerSignal {
    HeartBeat{ w_id: WorkerId},
    Working{w_id: WorkerId, j_id: usize},
    Finished{w_id: WorkerId, j_id: usize, result: Vec<u8>, ctc_m: CTCMetrics},
    Failed{w_id: WorkerId, j_id: usize, reason: String, ctc_m: CTCMetrics},
}

pub enum CacherTelemetry {
    ModuleCached{path: String},
    ModuleEvicted{path: String},
    ModuleUsed{path: String},
    FailedToCache{path: String, error: CacheErr}
}

#[derive(Debug)]
pub enum CacheErr {
    SerializationError{reason: String},
    ModuleCreationError{reason: String},
    IoError{reason: String},
    NotFound,
}

pub struct Worker {
    pub main_loop: JoinHandle<()>,
    pub cache_loop: JoinHandle<()>,
    pub id: WorkerId,
    pub sender: Sender<Message>,
    pub load: usize, 
    pub jobs: Arc<RwLock<Vec<JoinHandle<()>>>>
}

impl Worker {
    pub async fn spawn(id: WorkerId, db_pool: MySqlPool, fb_tx: Sender<WorkerSignal>, tl_tx: Sender<CacherTelemetry>, gcc_tx: BcastSender<GCCSignal>, m_tx: Sender<(String, MetricsPacket)>) -> Self {
        let (tx, rx) = channel::<Message>(4096);
        let cache: Arc<ShardedMap<String, Arc<Module>>> = Arc::new(ShardedMap::new(64));
        let jobs: Arc<RwLock<Vec<JoinHandle<()>>>> = Arc::new(RwLock::new(Vec::new()));
        let jobs_clone = Arc::clone(&jobs);

        let mut config = wasmtime::Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let engine = match engine {
            Ok(engine) => engine,
            Err(e) => {
                panic!("Failed to create engine: {}. FATAL: panicking!", e);
            }
        };
        let cache_loop = start_cache_loop(
            engine.clone(),
            db_pool.clone(),
            gcc_tx,
            tl_tx.clone(),
            Arc::clone(&cache)
        ).await;
        let task = start_main_loop(
            engine,
            db_pool,
            tl_tx,
            fb_tx,
            Arc::clone(&cache),
            Arc::clone(&jobs),
            id,
            rx,
            m_tx,
        ).await;
        Worker {
            main_loop: task,
            cache_loop,
            id,
            sender: tx,
            load: 0,
            jobs: jobs_clone,
        }     
    }
    pub async fn stop(&mut self) {
        for job in self.jobs.write().await.iter_mut() {
            job.abort();
        }
        self.cache_loop.abort();
        self.main_loop.abort();
        println!("Worker {} is stopped", self.id);
    }
}

