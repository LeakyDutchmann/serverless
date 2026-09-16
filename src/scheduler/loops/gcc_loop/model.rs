use crate::workers::model::Worker;
use crate::scheduler::types::ModuleStats;

use tokio::time::{Instant, interval};
use tokio::task::JoinHandle;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::RwLock;
use tokio::time::Duration;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use ordered_float::OrderedFloat;

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ModuleEvictionStats {
    pub path: String,
    pub total_memory_usage: usize,
}

#[derive(Clone)]
pub enum GCCSignal {
    CacheModule{path: String},
    EvictModule{path: String}
}

pub type BcastSender<T> = tokio::sync::broadcast::Sender<T>;

const MAX_MEMORY_USAGE: usize = 512_000_000;

pub async fn start_gcc_loop
(
    workers: Arc<RwLock<Vec<Worker>>>,
    s_map: Arc<RwLock<HashMap<String, ModuleStats>>>,
    f_paths: Arc<RwLock<HashSet<String>>>,
    cache_memory_usage: Arc<AtomicUsize>,
    cache_tx: BcastSender<GCCSignal>,
) -> JoinHandle<()>{
    let handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(10));
        let mut evict_candidates: PriorityQueue<ModuleEvictionStats, Reverse<OrderedFloat<f64>>> = PriorityQueue::new();
        loop { 
            let m_counter = cache_memory_usage.load(Ordering::SeqCst);
            let w_counter = workers.read().await.len();
            interval.tick().await;
            let map = s_map.read().await; 
            let f_map = f_paths.read().await;
            for (path, stats) in map.iter() {
                if f_map.contains(path) {
                    continue;
                }
                if let Some(pre_last) = stats.pre_last_invocation {
                    if stats.last_invocation.duration_since(pre_last) < Duration::from_secs(60) {
                        if Instant::now().duration_since(stats.last_invocation) < Duration::from_secs(60) {
                            let memory_needed = stats.memory_usage * w_counter + m_counter;
                            if  memory_needed < MAX_MEMORY_USAGE {
                                println!("Sending signal to cache module on path: {}", path);
                                let _ = cache_tx.send(GCCSignal::CacheModule{path: path.clone()});
                            } else {
                                println!("Not enough memory to cache module on path: {}", path);
                                let mut memory_freed = 0;
                                let use_time = Instant::now() - stats.first_invocation;
                                let use_dynamic = stats.invokations as f64 / use_time.as_secs() as f64;
                                loop {
                                    if memory_freed >= memory_needed {
                                        println!("Memory freed: {} >= memory needed: {}", memory_freed, memory_needed);
                                        println!("Sending signal to cache module on path: {}", path);
                                        let _ = cache_tx.send(GCCSignal::CacheModule{path: path.clone()});
                                        break;
                                    }
                                    if let Some((e_stats, Reverse(OrderedFloat(e_use_dynamic)))) = evict_candidates.peek() {
                                        if memory_needed <= e_stats.total_memory_usage {
                                            if e_use_dynamic < &use_dynamic {
                                                let _ = cache_tx.send(GCCSignal::EvictModule{path: e_stats.path.clone()});
                                                memory_freed += e_stats.total_memory_usage;
                                                let _ = evict_candidates.pop();
                                            }
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                
                            }
                        
                        }
                    }
                }
                if Instant::now().duration_since(stats.last_invocation) > Duration::from_secs(120) {
                    println!("Sending signal to evict module on path: {}", path);
                    let _ = cache_tx.send(GCCSignal::EvictModule{path: path.clone()});
                }
                if stats.cached_instances > 0 {
                    let use_time = Instant::now() - stats.first_invocation;
                    let use_dynamic = stats.invokations as f64 / use_time.as_secs() as f64;
                    let total_memory_usage = stats.memory_usage * stats.cached_instances as usize;
                    if use_dynamic < 0.05 {
                        //Length threshold 10000 might be customized
                        if evict_candidates.len() < 10000 {
                            evict_candidates.push(ModuleEvictionStats{path: path.clone(), total_memory_usage }, Reverse(OrderedFloat::from(use_dynamic)));
                        } 
                        
                    }
                }
            }
            evict_candidates.clear();
        }
    });
    handle
}