use crate::http::utils::get_function_name;

use tokio::task::JoinHandle;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc::channel;
use sqlx::{MySqlPool, Row};
use tokio::select;
use tokio::time::{Interval, interval, Duration};
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

pub struct Worker {
    pub main_loop: JoinHandle<()>,
    pub id: usize,
    pub sender: Sender<Message>,
    pub load: usize, 
    pub jobs: Arc<RwLock<Vec<JoinHandle<()>>>>
}

impl Worker {
    pub async fn spawn(id: usize, db_pool: MySqlPool, fb_tx: Sender<WorkerSignal>) -> Self {
        let (tx, mut rx) = channel::<Message>(1024);
        let mut heartbeat = interval(Duration::from_secs(3));
        let jobs: Arc<RwLock<Vec<JoinHandle<()>>>> = Arc::new(RwLock::new(Vec::new()));
        let jobs_clone = Arc::clone(&jobs);
        let task = tokio::spawn(async move {
            let engine = Engine::default();
            loop {
                let engine = engine.clone();
                let db = db_pool.clone();
                let fb= fb_tx.clone();
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
                                let func_name = get_function_name(&path);
                                let job = tokio::spawn(async move {
                                    let _ = fb.send(WorkerSignal::Working{w_id: id, j_id}).await;
                                    println!("fetching func on path: {}", path);
                                    let result = sqlx::query("SELECT wasm FROM functions WHERE path = ?")
                                        .bind(&func_name)
                                        .fetch_optional(&db)
                                        .await;
                                    match result {
                                        Ok(Some(row)) => {
                                            let wasm: Vec<u8> = match row.try_get("wasm") {
                                                Ok(wasm) => wasm,
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            };
                                            let module = match Module::new(&engine, wasm) {
                                                Ok(module) => module,
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            };
                                            let mut store = Store::new(&engine, ());
                                            let instance = match Instance::new(&mut store, &module, &[]) {
                                                Ok(instance) => instance,
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            };
                                            let alloc = match instance.get_typed_func::<u32, u32>(&mut store, "alloc") {
                                                Ok(alloc) => alloc,
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            };
                                            let ptr = match alloc.call(&mut store, input.len() as u32) {
                                                Ok(ptr) => ptr,
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            };
                                            println!("Alloc returned pointer: {}", ptr);
                                            let memory = match instance.get_memory(&mut store, "memory") {
                                                Some(memory) => memory,
                                                None => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: "No memory found in wasm module".to_string()}).await;
                                                    return;
                                                }
                                            };
                                            match memory.write(&mut store, ptr as usize, &input) {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            }
                                            println!("Memory written {:?}", &input);
                                            let main = match instance.get_typed_func::<(u32, u32), (u32, u32)>(&mut store, "main") {
                                                Ok(main) => main,
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                    return;
                                                }
                                            };
                                            let result = main.call(&mut store, (ptr, input.len() as u32));
                                            if result.is_err() {
                                                let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: result.err().unwrap().to_string()}).await;
                                                return;
                                            }
                                            let (new_ptr, len) = result.unwrap();
                                            println!("Result of main: (ptr: {}, len: {})", new_ptr, len);
                                            let mut buffer = vec![0u8; len as usize];
                                            let func_result = memory.read(&mut store, new_ptr as usize, &mut buffer);
                                            match func_result {
                                                Ok(_) => {
                                                    println!("Memory read successfully. Output: {:?}", buffer);
                                                    let _ = fb.send(WorkerSignal::Finished{w_id: id, j_id, result: buffer}).await;
                                                }
                                                Err(e) => {
                                                    let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                                }
                                            }
                                        },
                                        Ok(None) => {
                                            let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: "MySql returned Ok(None)".to_string()}).await;
                                        }
                                        Err(e) => {
                                            let _ = fb.send(WorkerSignal::Failed{w_id: id, j_id, reason: e.to_string()}).await;
                                        }
                                    };
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
        });
        Worker {
            main_loop: task,
            id,
            sender: tx,
            load: 0,
            jobs: jobs_clone,
        }
        
    }
}