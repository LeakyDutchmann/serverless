use crate::workers::model::CacherTelemetry;
use crate::scheduler::types::ModuleStats;

use tokio::time::Instant;
use sqlx::{MySqlPool, Row};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::AtomicUsize};
use tokio::sync::RwLock;

pub async fn handle_telemetry(
    stats_map: Arc<RwLock<HashMap<String, ModuleStats>>>,
    forbidden_paths: Arc<RwLock<HashSet<String>>>,
    cache_memory_usage: Arc<AtomicUsize>,
    db_pool: MySqlPool,
    tl_signal: CacherTelemetry,
)  {
    tokio::spawn(async move {
        let mut map = stats_map.write().await;
        let _f_map = forbidden_paths.write().await;
        let instant = Instant::now();
        match tl_signal {
            CacherTelemetry::ModuleCached{path} => {
                if let Some(stats) = map.get_mut(&path) {
                    stats.cached_instances += 1;
                } else {
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
                            map.insert(path.clone(), ModuleStats {
                                first_invocation: instant,
                                invokations: 1,
                                last_invocation: instant,
                                pre_last_invocation: None,
                                last_eviction: None,
                                cached_instances: 1,
                                memory_usage: memory_usage as usize,
                            });
                            cache_memory_usage.fetch_add(memory_usage as usize, std::sync::atomic::Ordering::SeqCst);
                        }
                        Ok(None) => {
                            println!("CACHING: Failed to look up module at path: {}, MODULE NOT FOUND", path);
                        }
                        Err(e) => {
                            println!("CACHING: Failed to look up module at path: {}, {}", path, e);
                        }
                    }      
                }
            },
            CacherTelemetry::ModuleEvicted{path} => {
                if let Some(stats) = map.get_mut(&path) {
                    stats.last_eviction = Some(instant);
                    stats.cached_instances -= 1;
                    let current = cache_memory_usage.load(std::sync::atomic::Ordering::SeqCst);
                    if stats.memory_usage < current {
                        cache_memory_usage.fetch_sub(stats.memory_usage, std::sync::atomic::Ordering::SeqCst);
                    } else {
                        panic!("Memory usage counter went inconsistent");
                        //Little note on this one: I don't think that part should stay this way, but I will handle it
                        // more gracefully later. One thing to remember is that this is FATAL error, no fall back - you have to shutdown
                        // server immediately!
                    }
                }
            },
            CacherTelemetry::ModuleUsed{path} => {
                if let Some(stats) = map.get_mut(&path) {
                    stats.invokations += 1;
                    stats.pre_last_invocation = Some(stats.last_invocation);
                    stats.last_invocation = instant;
                } else {
                    let result = sqlx::query("SELECT memory_usage FROM functions WHERE path = ?")
                        .bind(path.clone())
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
                            map.insert(path.clone(), ModuleStats {
                                invokations: 1,
                                first_invocation: instant,
                                last_invocation: instant,
                                pre_last_invocation: None,
                                last_eviction: None,
                                cached_instances: 0,
                                memory_usage: memory_usage as usize,
                            });
                            cache_memory_usage.fetch_add(memory_usage as usize, std::sync::atomic::Ordering::SeqCst);
                        }
                        Ok(None) => {
                            println!("CACHINGSTATS: Failed to look up module at path: {}, MODULE NOT FOUND", path);
                        }
                        Err(e) => {
                            println!("CACHINGSTATS: Failed to look up module at path: {}, {}", path, e);
                        }
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