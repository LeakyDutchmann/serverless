use crate::scheduler::types::SchedulerCommand;
use crate::workers::model::WorkerId;
use crate::sharded::ShardedMap;

use tokio::{sync::mpsc::Sender, time::{Instant, interval}};
use tokio::task::JoinHandle;
use std::sync::Arc;
use tokio::time::Duration;

pub async fn start_hb_loop(hb_tx: Sender<SchedulerCommand>, h_map: Arc<ShardedMap<WorkerId, Instant>>,) -> JoinHandle<()> {
    
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        let sender = hb_tx.clone();
        loop {
            interval.tick().await;
            let heartbeat_snap = h_map.snapshot().await;
            for (id, last_heartbeat) in heartbeat_snap.iter() {
                if Instant::now().duration_since(*last_heartbeat) > Duration::from_secs(6) {
                    let _ = sender.send(SchedulerCommand::DropDeadWorker(id.clone())).await;
                }
            }
        }
    })
}