use crate::workers::model::CacherTelemetry;
use crate::scheduler::types::ModuleStats;
use crate::scheduler::shutdown::Shutdown;
use crate::http::utils::get_function_name;
use crate::sharded::{ShardedMap, Sharded};

use tokio::time::Instant;
use sqlx::{MySqlPool, Row};
use tokio::sync::mpsc::Sender;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::AtomicUsize};
use tokio::sync::RwLock;

//This threshold is more of experimental thing. I have not built any extra stats eviction logic, because 
// I hope that cache eviction signals will do the job. Anyway, I will test all of that.
const MAX_STATS_ENTRIES: usize = 50_000;

//If you ever get to implementaton of f_paths_map, then remeber to not aquire lock for too long because it blocks your gcc loop. 
pub async fn handle_telemetry(
    stats_map: Arc<ShardedMap<String, ModuleStats>>,
    _forbidden_paths: Arc<RwLock<HashSet<String>>>,
    cache_memory_usage: Arc<AtomicUsize>,
    db_pool: MySqlPool,
    tl_signal: CacherTelemetry,
    shutdown_tx: Sender<Shutdown>,
)  {
    tokio::spawn(async move {
        let instant = Instant::now();
        match tl_signal {
            CacherTelemetry::ModuleCached{path} => {
                let map_len = stats_map.len().await; 
                
                if let Some(stats) = stats_map.modify(&path, |s| s.cached_instances += 1).await {
                    cache_memory_usage.fetch_add(stats.memory_usage, std::sync::atomic::Ordering::Relaxed);
                    println!("Module cached: {}, memory_usage: {}", path, stats.memory_usage);
                    return;
                }  

                let result = sqlx::query("SELECT memory_usage FROM functions WHERE path = ?")
                    .bind(path.clone())
                    .fetch_optional(&db_pool)
                    .await;
                match result {
                    Ok(Some(row)) => {
                        let memory_usage: i32 = match row.try_get("memory_usage") {
                            Ok(memory_usage) => memory_usage,
                            Err(e) => {
                                println!("CACHING: Failed to fetch memory usage for module: {}, error: {}", path, e);
                                return;
                            },
                        }; 
                        if memory_usage < 0 {
                            println!("CACHING: Module on path {} requires negative amount of memory: {}", path, memory_usage);
                            return;
                        }
                        
                        if map_len < MAX_STATS_ENTRIES {
                            stats_map.insert(path.clone(), ModuleStats {
                                first_invocation: instant,
                                invokations: 1,
                                last_invocation: instant,
                                pre_last_invocation: None,
                                last_eviction: None,
                                cached_instances: 1,
                                memory_usage: memory_usage as usize,
                            }).await;
                            cache_memory_usage.fetch_add(memory_usage as usize, std::sync::atomic::Ordering::Relaxed);
                            println!("CACHING: Added module on path: {}, memory_usage: {}", path, memory_usage);
                        } else {
                            //Probably want to implement some queue
                        }
        
                    }
                    Ok(None) => {
                        println!("CACHING: Failed to look up module at path: {}, MODULE NOT FOUND", path);
                    }
                    Err(e) => {
                        println!("CACHING: Failed to look up module at path: {}, {}", path, e);
                    }
                }  
                
            },
            CacherTelemetry::ModuleEvicted{path} => {
                if let Some(stats) = stats_map.modify(&path, |s| {s.last_eviction = Some(instant); s.cached_instances -= 1}).await {
                    let current = cache_memory_usage.load(std::sync::atomic::Ordering::Relaxed);
                    if stats.memory_usage <= current {
                        cache_memory_usage.fetch_sub(stats.memory_usage, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        //Shutdown because when program thinks that it uses less memory than it does - it's bad
                        let _ = shutdown_tx.send(Shutdown{reason: format!("Inconsistent memory usage counter. Using less memory than expected. Using: {}, expected min: {}", current, stats.memory_usage), instant: tokio::time::Instant::now()}).await;
                    }
                    if stats.cached_instances == 0 {
                        stats_map.remove(&path).await;
                    }
                }
            },
            CacherTelemetry::ModuleUsed{path} => {
                let map_len = stats_map.len().await;
                
                if let Some(_) = stats_map.modify(&path, |s| {s.invokations += 1; s.pre_last_invocation = Some(s.last_invocation); s.last_invocation = instant}).await {
                    return;
                }
                
                let func_name = get_function_name(&path);
                let result = sqlx::query("SELECT memory_usage FROM functions WHERE path = ?")
                    .bind(func_name)
                    .fetch_optional(&db_pool)
                    .await;
                match result {
                    Ok(Some(row)) => {
                        let memory_usage: i32 = match row.try_get("memory_usage") {
                            Ok(memory_usage) => memory_usage,
                            Err(e) => {
                                println!("CACHINGSTATS: Failed to fetch memory usage for module: {}, error: {}", path, e);
                                return;
                            },
                        }; 
                        if memory_usage < 0 {
                            println!("CACHINGSTATS: Module on path {} requires negative amount of memory: {}", path, memory_usage);
                            return;
                        }
                        
                        if map_len < MAX_STATS_ENTRIES {
                            stats_map.insert(path.clone(), ModuleStats {
                                invokations: 1,
                                first_invocation: instant,
                                last_invocation: instant,
                                pre_last_invocation: None,
                                last_eviction: None,
                                cached_instances: 0,
                                memory_usage: memory_usage as usize,
                            }).await;
                        }
                    }                    
                    Ok(None) => {
                        println!("CACHINGSTATS: Failed to look up module at path: {}, MODULE NOT FOUND", path);
                    }
                    Err(e) => {
                        println!("CACHINGSTATS: Failed to look up module at path: {}, {}", path, e);
                    }
                }
            },
            CacherTelemetry::FailedToCache{path, error} => {    
                println!("Failed to cache module {}, error: {:?}", path, error);
                //probably need to build some logic aroung it.
            },
        }
    });
}