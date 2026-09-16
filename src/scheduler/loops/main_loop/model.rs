use crate::workers::model::{Worker, WorkerSignal, Message, CacherTelemetry, CacheErr};
use crate::http::response::{Response, StatusCode, send};
use crate::scheduler::model::{Job, ModuleStats, InternalChannels, Channel};

use crate::scheduler::model::{SchedulerCommand, upgrade, downgrade, drop_dead_worker, generate_job_id};
use crate::scheduler::loops::gcc_loop::model::{GCCSignal, BcastSender};

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


pub async fn start_main_loop
(
    load_map: Arc<RwLock<HashMap<usize, usize>>>,
    job_map: Arc<RwLock<HashMap<usize, TcpStream>>>,
    heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>>,
    mut job_rx: Receiver<Job>,
    workers: Arc<RwLock<Vec<Worker>>>,
    mut load_rx: Receiver<SchedulerCommand>,
    db_pool: MySqlPool,
    internal_channels: InternalChannels,
    gcc_tx: BcastSender<GCCSignal>,
    forbidden_paths: Arc<RwLock<HashSet<String>>>,
    cache_memory_usage: Arc<AtomicUsize>,
    stats_map: Arc<RwLock<HashMap<String, ModuleStats>>>,
) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        loop {
            select! {
                Some(worker_signal) = feedback_rx.recv() => {
                    let mut load_map = load_map.write().await;
                    let mut job_map = job_map.write().await;
                    match worker_signal {
                        WorkerSignal::HeartBeat { w_id } => {
                            let mut map = heartbeat_map.write().await;
                            map.insert(w_id, Instant::now());
                        }
                        WorkerSignal::Working {w_id, j_id} => {   
                            if let Some(load) = load_map.get_mut(&w_id) {
                                *load += 1;
                                println!("Worker {} started task {}", w_id, j_id);
                            } else {
                                load_map.insert(w_id, 1);
                            }
                        }
                        WorkerSignal::Finished {w_id, j_id, result} => {
                            if let Some(stream) = job_map.get_mut(&j_id) {
                                if let Some(load) = load_map.get_mut(&w_id) {
                                    if *load != 0 {
                                        *load -= 1;
                                        println!("Worker {} finished task {}", w_id, j_id);
                                        let response = Response::json(StatusCode::Ok, result, None);
                                        send(stream, &response).await;
                                    } else {
                                        let response = Response::json(StatusCode::Ok, result, None);
                                        send(stream, &response).await;
                                        println!("Worker {} finished untracked task", w_id);
                                    }      
                                } else {
                                    let response = Response::json(StatusCode::Ok, result, None);
                                    send(stream, &response).await;
                                    println!("Worker {} finished untracked task", w_id);
                                }  
                                job_map.remove(&j_id);
                            } else {
                                println!("Worker {} finished task that belongs to no client. Task id: {}", w_id, j_id);
                            }     
                        }
                        WorkerSignal::Failed {w_id, j_id, reason} => {
                            if let Some(stream) = job_map.get_mut(&j_id) {
                                if let Some(load) = load_map.get_mut(&w_id) {
                                    if *load != 0 {
                                        *load -= 1;
                                        let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                                        send(stream, &response).await;
                                        println!("Worker {} failed task {}", w_id, j_id);
                                    } else {
                                        let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                                        send(stream, &response).await;
                                        println!("Worker {} failed untracked task", w_id);
                                    }      
                                } else {
                                    let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                                    send(stream, &response).await;
                                    println!("Worker {} failed untracked task", w_id);
                                }
                                job_map.remove(&j_id);
                            } else {
                                println!("Worker {} failed task that belongs to no client. Task id: {}, reason {}", w_id, j_id, reason);
                            }   
                        }
                    }
                }
                Some(mut task) = job_rx.recv() => {
                    println!("Got request to run this function: {:?}", task.path);
                    let workers = workers.clone();
                    let l_map = Arc::clone(&load_map);
                    let j_map = job_map.clone();
                    tokio::spawn(async move {
                        let workers = workers.read().await;
                        let map = l_map.read().await;
                        if let Some((id, _)) = map.iter().min_by_key(|(_, v)| *v) {
                            if let Some(worker) = workers.iter().find(|w| w.id == *id) {
                                let j_id = generate_job_id().await;
                                let result = worker.sender.send(Message::Job{path: task.path, input: task.input, j_id}).await;
                                match result {
                                    Ok(_) => {
                                        let mut map = j_map.write().await;
                                        map.insert(j_id, task.stream);
                                    }
                                    Err(e) => {
                                        let line = format!("Failed to send job to worker {}", e);
                                        let response = Response::json(StatusCode::IntServerError, vec![], Some(line));
                                        send(&mut task.stream, &response).await;
                                        println!("Failed to send job to worker: {}", e)
                                    },
                                }
                            } else {
                                let vec = workers.iter().map(|w| w.id.clone()).collect::<Vec<_>>();
                                let s = vec.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ");
                                let line = format!("Failed to attach task to a worker because of inconsistent worker id's");
                                let response = Response::json(StatusCode::IntServerError, vec![], Some(line));
                                send(&mut task.stream, &response).await;
                                println!("Tried to attach job to worker with id {}, but there is no such worker in worker pool. Workers available: {}", id, s);
                            }
                        } else {
                            let line = format!("Failed to attach task to a worker because there are no workers");
                            let response = Response::json(StatusCode::IntServerError, vec![], Some(line));
                            send(&mut task.stream, &response).await;
                            println!("Couldn't pick a worker, because load map contains exactly 0 elements");
                        }
                    });
                }
                Some(cmd) = load_rx.recv() => {
                    let db_pool = db_pool.clone();
                    let feedback_tx = feedback_tx.clone();
                    let tl_tx = tl_tx.clone();
                    let workers = workers.clone();
                    let gcc_tx = gcc_tx.clone();
                    let l_map = Arc::clone(&load_map);
                    match cmd {
                        SchedulerCommand::Upgrade(n) => {
                            tokio::spawn(async move {
                                upgrade(workers.clone(), n, db_pool.clone(), feedback_tx.clone(), tl_tx.clone(), gcc_tx.clone(), l_map.clone()).await;
                            });
                        }
                        SchedulerCommand::Downgrade(n) => {
                            downgrade(workers.clone(), n, l_map.clone()).await;
                        }
                        SchedulerCommand::DropDeadWorker(n) => {
                            drop_dead_worker(workers.clone(), n, l_map.clone()).await;
                        }
                    }
                }
                Some(tl_signal) = tl_rx.recv() => {
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