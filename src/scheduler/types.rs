use crate::workers::model::{WorkerSignal, CacherTelemetry};

use super::loops::{gcc_loop::model::{GCCSignal, BcastSender}, metrics::model::MetricsPacket};
use super::shutdown::Shutdown;


use tokio::{sync::mpsc::{Receiver, Sender}, time::Instant};
use tokio::task::JoinHandle;
use tokio::net::TcpStream;

pub struct Job {
    pub path: String,
    pub input: Vec<u8>,
    pub stream: TcpStream,
}

pub enum SchedulerCommand {
    Upgrade(usize),
    Downgrade(usize),
    DropDeadWorker(usize)
}

pub struct RuntimeTasks {
    pub scheduler_task: JoinHandle<()>,
    pub heartbeat_task: JoinHandle<()>,
    pub load_task: JoinHandle<()>,
    pub gcc_task: JoinHandle<()>,
    pub metrics_task: JoinHandle<()>,
}

impl RuntimeTasks {
    pub fn abort(&mut self) {
        self.gcc_task.abort();
        self.load_task.abort();
        self.heartbeat_task.abort();
        self.scheduler_task.abort();
        self.metrics_task.abort();
    }
}

pub struct ExternalChannels {
    pub gcc_tx: Option<BcastSender<GCCSignal>>,
    pub load_rx: Option<Receiver<usize>>,
    pub job_rx: Option<Receiver<Job>>,
    pub shutdown_tx: Option<Sender<Shutdown>>,
    pub metrics_rx: Option<Receiver<(String, MetricsPacket)>>,
    pub metrics_tx: Sender<(String, MetricsPacket)>,
}
impl ExternalChannels {
    pub fn init(job_rx: Receiver<Job>, shutdown_tx: Sender<Shutdown>) -> ExternalChannels {
        let (gcc_tx, _) = tokio::sync::broadcast::channel::<GCCSignal>(1024);
        let (m_tx, m_rx) = tokio::sync::mpsc::channel::<(String, MetricsPacket)>(1024);
        ExternalChannels {
            gcc_tx: Some(gcc_tx),
            load_rx: None,
            job_rx: Some(job_rx),
            shutdown_tx: Some(shutdown_tx),
            metrics_rx: Some(m_rx),
            metrics_tx: m_tx,
        }
    }
}

pub struct InternalChannels {
    pub feedback: Channel<WorkerSignal>,
    pub telemetry: Channel<CacherTelemetry>,
}

impl InternalChannels {
    pub fn init() -> InternalChannels {
        let feeedback_channel: Channel<WorkerSignal> = Channel::new(1024);
        let telemetry_channel: Channel<CacherTelemetry> = Channel::new(1024);
        InternalChannels {
            feedback: feeedback_channel,
            telemetry: telemetry_channel,
        }
    }
}

pub struct Channel<T> {
    pub tx: Sender<T>,
    pub rx: Receiver<T>,
}

impl<T> Channel<T> {
    fn new(buffer_size: usize) -> Channel<T> {
        let (tx, rx) = tokio::sync::mpsc::channel::<T>(buffer_size);
        Channel { tx, rx}
    }
}

pub struct ModuleStats {
    pub memory_usage: usize,
    pub invokations: u64,
    pub first_invocation: Instant,
    pub last_invocation: Instant,
    pub pre_last_invocation: Option<Instant>,
    pub last_eviction: Option<Instant>,
    pub cached_instances: u64,
}
