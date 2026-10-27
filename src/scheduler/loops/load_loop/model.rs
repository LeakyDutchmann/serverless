use crate::scheduler::types::SchedulerCommand;
use crate::sharded::ShardedMap;
use crate::workers::model::{WorkerId, WorkerLoad};

use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use std::sync::Arc;
use tokio::time::Instant;
use tokio::time::{Duration, interval};

pub async fn start_load_loop(
    max_workers: usize,
    load_tx: Sender<SchedulerCommand>,
    l_map: Arc<ShardedMap<WorkerId, WorkerLoad>>,
) -> JoinHandle<()> {
    
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(2));
        let max_workers = max_workers;
        loop {
            interval.tick().await;
            let l_snap = l_map.snapshot().await;
            
            let workers = l_snap.len();
            let mut busy_workes: Vec<(WorkerId, usize)> = Vec::with_capacity(workers);
            let mut future_workers = workers;
            for (id, load) in l_snap.iter() {
                // println!("load of {} is {}", id, load.task_count);
                if load.task_count >= 4 {
                    busy_workes.push((id.clone(), load.task_count));
                }
                
                if future_workers > 4
                    && load.task_count == 0
                        && Instant::now().duration_since(load.last_task_time) > Duration::from_secs(30) {
                            let _ = load_tx.send(SchedulerCommand::Downgrade(id.clone())).await;
                            future_workers -= 1;
                            println!("Sent downgrade command");  
                }   
            }
            println!("busy workers: {}", busy_workes.len());
            if busy_workes.len() as f64 >= workers as f64 * 0.75
                && workers + 4 <= max_workers {
                    let _ = load_tx.send(SchedulerCommand::Upgrade(4)).await;
                    println!("Sent upgrade command");
                    continue; 
                }
            
        }
    })
}