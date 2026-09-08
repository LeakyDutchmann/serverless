use tokio::task::JoinHandle;
use wasmtime::{Engine, Module};
use sqlx::{MySqlPool, Row};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::select;
use tokio::time::{interval, Duration};

use crate::scheduler::model::{GCCSignal, BcastSender};
use crate::http::utils::get_function_name;
use crate::workers::model::{CacherTelemetry, CacheErr};



pub async fn start_cache_loop(engine: Engine, db_pool: MySqlPool, gcc_tx: BcastSender<GCCSignal>, tl_tx: Sender<CacherTelemetry>, cache: Arc<RwLock<HashMap<String, Module>>>) ->  JoinHandle<()>{
    tokio::spawn(async move {
        let mut rx = gcc_tx.subscribe();
        let mut interval = interval(Duration::from_secs(10));
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
                                    let _ = tl_tx.send(CacherTelemetry::FailedToCache{path: path.clone(), error: CacheErr::IoError{reason: e.to_string()}});
                                    continue;
                                }
                            };
                            let module = match Module::new(&engine, wasm) {
                                Ok(module) => module,
                                Err(e) => {
                                    let _ = tl_tx.send(CacherTelemetry::FailedToCache{path: path.clone(), error: CacheErr::ModuleCreationError{reason: e.to_string()}});
                                    continue;
                                }
                            };
                            let _ = map.insert(path.clone(), module);
                            let _ = tl_tx.send(CacherTelemetry::ModuleCached { path: path.clone()});
                        },
                        Ok(None) => {
                            let _ = tl_tx.send(CacherTelemetry::FailedToCache{path: path.clone(), error: CacheErr::NotFound});
                        }
                        Err(e) => {
                            let _ = tl_tx.send(CacherTelemetry::FailedToCache{path: path.clone(), error: CacheErr::IoError{reason: e.to_string()}});
                        }
                    };
                }
                GCCSignal::EvictModule { path } => {
                    map.remove(&path);
                    let _ = tl_tx.send(CacherTelemetry::ModuleEvicted { path: path });
                }
            }
        }
    })
}

