use crate::scheduler::types::SchedulerCommand;
use tokio::{sync::mpsc::Sender, time::{Instant, interval}};
use tokio::task::JoinHandle;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

pub async fn start_hb_loop(hb_tx: Sender<SchedulerCommand>, h_map: Arc<RwLock<HashMap<usize, Instant>>>,) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3));
        let sender = hb_tx.clone();
        loop {
            interval.tick().await;
            let heartbeat_map = h_map.read().await;
            for (id, last_heartbeat) in heartbeat_map.iter() {
                if Instant::now().duration_since(*last_heartbeat) > Duration::from_secs(6) {
                    let _ = sender.send(SchedulerCommand::DropDeadWorker(*id)).await;
                }
            }
        }
    });
    handle
}