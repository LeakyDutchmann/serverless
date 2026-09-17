use crate::workers::model::Worker;
use crate::scheduler::types::InternalChannels;

use crate::scheduler::{types::SchedulerCommand, utils::{upgrade, downgrade, drop_dead_worker}};
use crate::scheduler::loops::gcc_loop::model::{GCCSignal, BcastSender};

use sqlx::MySqlPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn handle_load(
    db_pool: MySqlPool,
    internal_channels: &InternalChannels,
    workers: Arc<RwLock<Vec<Worker>>>,
    load_map: Arc<RwLock<HashMap<usize, usize>>>,
    gcc_tx: BcastSender<GCCSignal>,
    cmd: SchedulerCommand,
) {
    let feedback_tx = internal_channels.feedback.tx.clone();
    let tl_tx = internal_channels.telemetry.tx.clone();
    match cmd {
        SchedulerCommand::Upgrade(n) => {
            tokio::spawn(async move {
                upgrade(workers.clone(), n, db_pool.clone(), feedback_tx, tl_tx, gcc_tx, load_map).await;
            });
        }
        SchedulerCommand::Downgrade(n) => {
            downgrade(workers.clone(), n, load_map).await;
        }
        SchedulerCommand::DropDeadWorker(n) => {
            drop_dead_worker(workers.clone(), n, load_map).await;
        }
    }
}