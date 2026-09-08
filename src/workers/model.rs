use crate::http::utils::get_function_name;
use crate::scheduler::model::{BcastSender, GCCSignal};
use super::cache_manager::start_cache_loop;
use super::main_loop::init::start_main_loop;

use tokio::task::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::channel;
use sqlx::{MySqlPool, Row};
use std::collections::HashMap;
use tokio::select;
use tokio::time::{interval, Duration};
use tokio::sync::RwLock;
use std::sync::Arc;
use wasmtime::{Engine, Module, Store, Instance};



pub enum Message {
    Stop(String),
    Job{path: String, input: Vec<u8>, j_id: usize}
}

pub enum WorkerSignal {
    HeartBeat{ w_id: usize},
    Working{w_id: usize, j_id: usize},
    Finished{w_id: usize, j_id: usize, result: Vec<u8>},
    Failed{w_id: usize, j_id: usize, reason: String},
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
    pub id: usize,
    pub sender: Sender<Message>,
    pub load: usize, 
    pub jobs: Arc<RwLock<Vec<JoinHandle<()>>>>
}

impl Worker {
    pub async fn spawn(id: usize, db_pool: MySqlPool, fb_tx: Sender<WorkerSignal>, tl_tx: Sender<CacherTelemetry>, gcc_tx: BcastSender<GCCSignal>) -> Self {
        let (tx, mut rx) = channel::<Message>(1024);
        let cache: Arc<RwLock<HashMap<String, Module>>> = Arc::new(RwLock::new(HashMap::new()));
        let cache_copy = Arc::clone(&cache);
        let tl_tx_copy = tl_tx.clone();
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

        let cache_db = db_pool.clone();
        let cache_engine = engine.clone();
        let cache_loop = start_cache_loop(
            cache_engine,
            cache_db,
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
        ).await;
        Worker {
            main_loop: task,
            id,
            sender: tx,
            load: 0,
            jobs: jobs_clone,
        }     
    }
}

