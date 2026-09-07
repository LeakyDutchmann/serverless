use tokio::task::JoinHandle;
use wasmtime::{Engine, Module};
use sqlx::{MySqlPool, Row};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::scheduler::model::{GCCSignal, BcastSender};
use crate::http::utils::get_function_name;
use crate::workers::model::WorkerTelemetry;

pub async fn start_cache_loop(engine: Engine, db_pool: MySqlPool, gcc_tx: BcastSender<GCCSignal>, tl_tx: Sender<WorkerTelemetry>, cache: Arc<RwLock<HashMap<String, Module>>>) ->  JoinHandle<()>{
    tokio::spawn(async move {
        let mut rx = gcc_tx.subscribe();
        while let Ok(signal) = rx.recv().await {
            let mut map = cache.write().await;
            match signal {
                GCCSignal::CacheModule { path } => {
                    if map.contains_key(&path) {
                        continue;
                    }
                    let func_name = get_function_name(&path);
                    let result = sqlx::query("SELECT wasm FROM functions WHERE path = ?")
                        .bind(&func_name)
                        .fetch_optional(&db_pool.clone())
                        .await;
                    match result {
                        Ok(Some(row)) => {
                            let wasm: Vec<u8> = match row.try_get("wasm") {
                                Ok(wasm) => wasm,
                                Err(e) => {
                                    println!("Error getting wasm: {:?}", e);
                                    return;
                                }
                            };
                            
                            let module = match Module::new(&engine, wasm) {
                                Ok(module) => module,
                                Err(e) => {
                                    println!("Error creating wasm module: {:?}", e);
                                    return;
                                }
                            };
                            let _ = map.insert(path.clone(), module);
                            let _ = tl_tx.send(WorkerTelemetry::ModuleCached { path: path.clone() });
                            println!("Cached module: {}", path);
                        },
                        Ok(None) => {
                            println!("Module not found: {}", path);
                        }
                        Err(e) => {
                            println!("Failed to fetch module {}: {}",path, e);
                        }
                    };
                }
                GCCSignal::EvictModule { path } => {
                    map.remove(&path);
                    let _ = tl_tx.send(WorkerTelemetry::ModuleEvicted { path: path });
                }
            }
        }
    })
}

