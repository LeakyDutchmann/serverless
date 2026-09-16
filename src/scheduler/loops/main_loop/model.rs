use crate::workers::model::{Worker, WorkerSignal, Message, CacherTelemetry, CacheErr};
use crate::http::response::{Response, StatusCode, send};
use crate::scheduler::types::{Job, ModuleStats, InternalChannels, Channel};

use crate::scheduler::{types::SchedulerCommand, utils::{upgrade, downgrade, drop_dead_worker, generate_job_id}};
use crate::scheduler::loops::gcc_loop::model::{GCCSignal, BcastSender};
use super::feedback::handle_feedback;
use super::job::handle_job;
use super::load::handle_load;


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


pub async fn start_main_loop(
    load_map: Arc<RwLock<HashMap<usize, usize>>>,
    job_map: Arc<RwLock<HashMap<usize, TcpStream>>>,
    heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>>,
    mut job_rx: Receiver<Job>,
    workers: Arc<RwLock<Vec<Worker>>>,
    mut load_rx: Receiver<SchedulerCommand>,
    db_pool: MySqlPool,
    mut internal_channels: InternalChannels,
    gcc_tx: BcastSender<GCCSignal>,
    forbidden_paths: Arc<RwLock<HashSet<String>>>,
    cache_memory_usage: Arc<AtomicUsize>,
    stats_map: Arc<RwLock<HashMap<String, ModuleStats>>>,
) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        loop {
            select! {
                Some(worker_signal) = internal_channels.feedback.rx.recv() => {
                    handle_feedback(Arc::clone(&job_map), Arc::clone(&load_map), Arc::clone(&heartbeat_map), worker_signal).await;     
                }
                Some(task) = job_rx.recv() => {
                    handle_job(Arc::clone(&workers), Arc::clone(&load_map), Arc::clone(&job_map), task);
                }
                Some(cmd) = load_rx.recv() => {
                    handle_load(db_pool.clone(), &internal_channels, Arc::clone(&workers), Arc::clone(&load_map), gcc_tx.clone(), cmd).await;
                    
                }
                Some(tl_signal) = internal_channels.telemetry.rx.recv() => {
                    let s_map = Arc::clone(&stats_map);
                    //provide here boundaries checking, huh? [[[[[[[[[[[[[[just me being extremely funny]]]]]]]]]]]]]]
                    let f_paths = Arc::clone(&forbidden_paths);
                    let db_pool = db_pool.clone();
                    let m_counter = Arc::clone(&cache_memory_usage);
                    tokio::spawn(async move {
                        let mut map = s_map.write().await;
                        let mut f_map = f_paths.write().await;
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
                                            m_counter.fetch_add(memory_usage as usize, std::sync::atomic::Ordering::SeqCst);
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
                                    let current = m_counter.load(std::sync::atomic::Ordering::SeqCst);
                                    if stats.memory_usage < current {
                                        m_counter.fetch_sub(stats.memory_usage, std::sync::atomic::Ordering::SeqCst);
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
                                            m_counter.fetch_add(memory_usage as usize, std::sync::atomic::Ordering::SeqCst);
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
            }
        }
    });
    handle
}