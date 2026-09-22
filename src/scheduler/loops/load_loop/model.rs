use crate::scheduler::types::SchedulerCommand;
use crate::workers::model::{WorkerId, WorkerLoad};

use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Instant;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};

pub async fn start_load_loop(
    max_workers: usize,
    load_tx: Sender<SchedulerCommand>,
    l_map: Arc<RwLock<HashMap<WorkerId, WorkerLoad>>>,
) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3));
        let max_workers = max_workers;
        loop {
            interval.tick().await;
            let map = l_map.read().await;
            let mut busy_workes: Vec<(usize, usize)> = Vec::with_capacity(map.len());
            for (id, load) in map.iter() {
                if load.task_count >= 4 {
                    busy_workes.push((*id, load.task_count));
                }
                if load.task_count == 0 {
                    if Instant::now().duration_since(load.last_task_time) > Duration::from_secs(30) {
                        let _ = load_tx.send(SchedulerCommand::Downgrade(*id)).await;
                        println!("Sent downgrade command");  
                    }
                    
                }
            }
            if busy_workes.len() as f64 >= map.len() as f64 * 0.75 {
                if map.len() + 4 <= max_workers {
                    let _ = load_tx.send(SchedulerCommand::Upgrade(4)).await;
                    println!("Sent upgrade command");
                    continue; 
                }
                
            }
            
        }
    });
    handle
}