use crate::workers::model::{CacherTelemetry, Worker, WorkerId, WorkerLoad, WorkerSignal};
use crate::scheduler::types::InternalChannels;
use crate::sharded::ShardedMap;

use crate::scheduler::{types::SchedulerCommand, utils::{upgrade, downgrade, drop_dead_worker}};
use crate::scheduler::loops::gcc_loop::model::{GCCSignal, BcastSender};
use crate::scheduler::loops::metrics::model::MetricsPacket;

use sqlx::MySqlPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;    

pub async fn handle_load(
    db_pool: MySqlPool,
    feedback_tx: Sender<WorkerSignal>,
    tl_tx: Sender<CacherTelemetry>,
    workers: Arc<RwLock<Vec<Worker>>>,
    load_map: Arc<ShardedMap<WorkerId, WorkerLoad>>,
    gcc_tx: BcastSender<GCCSignal>,
    cmd: SchedulerCommand,
    m_tx: Sender<(String, MetricsPacket)>,
) {
    
    match cmd {
        SchedulerCommand::Upgrade(n) => {
            upgrade(workers.clone(), n, db_pool.clone(), feedback_tx, tl_tx, gcc_tx, load_map, m_tx).await;
        }
        SchedulerCommand::Downgrade(n) => {
            downgrade(workers.clone(), n, load_map).await;
        }
        SchedulerCommand::DropDeadWorker(n) => {
            drop_dead_worker(workers.clone(), n, load_map).await;
        }
    }
}