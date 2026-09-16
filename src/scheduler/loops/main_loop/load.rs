use crate::workers::model::{Worker, WorkerSignal, Message, CacherTelemetry, CacheErr};
use crate::http::response::{Response, StatusCode, send};
use crate::scheduler::types::{Job, ModuleStats, InternalChannels, Channel};

use crate::scheduler::{types::SchedulerCommand, utils::{upgrade, downgrade, drop_dead_worker, generate_job_id}};
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