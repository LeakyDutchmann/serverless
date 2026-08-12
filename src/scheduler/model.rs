use crate::workers::model::{Worker, WorkerSignal};

use tokio::{sync::mpsc::{Receiver, Sender, channel}, time::{Instant, interval}};
use sqlx::MySqlPool;
use tokio_stream::wrappers::ReceiverStream;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio::select;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;


pub enum SchedulerCommand {
    Upgrade(usize),
    Downgrade(Vec<usize>),
}

pub struct Scheduler {
    pub workers: Arc<RwLock<Vec<Worker>>>,
    pub rx: Option<Receiver<String>>,
    pub load_rx: Option<Receiver<usize>>,
    pub feedback_rx: Option<Receiver<WorkerSignal>>,
    pub feedback_task: Option<JoinHandle<()>>,
    pub scheduler_task: Option<JoinHandle<()>>,
    pub heartbeat_task: Option<JoinHandle<()>>,
    pub load_task: Option<JoinHandle<()>>,
}

impl Scheduler {
    pub async fn intialize(worker_amount: usize, rx: Receiver<String>, db_pool: MySqlPool) -> Self {
        let mut workers = Vec::new();
        for i in 1..=worker_amount {
            let pool = db_pool.clone();
            let worker = Worker::spawn(i, pool).await;
            workers.push(worker);
        }
        let (tx, fb_rx) = channel::<WorkerSignal>(1024);
        let mut scheduler = Scheduler {
            workers: Arc::new(RwLock::new(workers)),
            rx: Some(rx),
            feedback_rx: Some(fb_rx),
            feedback_task: None,
            load_rx: None,
            load_task: None,
            scheduler_task: None,
            heartbeat_task: None,
        };
        scheduler.start_feedback_listener(tx).await;
        
        scheduler
    }
    pub async fn start_feedback_listener(&mut self, tx: Sender<WorkerSignal>) {
        let mut receivers: Vec<ReceiverStream<WorkerSignal>> = Vec::new();
        
        let lock_workers = Arc::clone(&self.workers);
        let mut workers = lock_workers.write().await;
        
        for worker in workers.iter_mut() {
            let stream = ReceiverStream::new(worker.feedback_recv.take().unwrap());
            receivers.push(stream);
        }
        let task = tokio::spawn(async move {
            let mut stream = futures::stream::select_all(receivers);
            while let Some(signal) = stream.next().await {
                let _ = tx.send(signal).await;
            }
        });
        self.feedback_task = Some(task);
    }
    pub fn run(&mut self) {
        if self.rx.is_none() || self.feedback_rx.is_none() {
            panic!("Main feedback and tasks receivers not found for scheduler, panicking!");
        }
        let mut rx = self.rx.take().unwrap();
        let mut feedback_rx = self.feedback_rx.take().unwrap();

        let (load_tx, mut load_rx) = channel::<SchedulerCommand>(1024);

        let heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>> = Arc::new(RwLock::new(HashMap::new()));
        let load_map: Arc<RwLock<HashMap<usize, usize>>> = Arc::new(RwLock::new(HashMap::new()));
        let l_map = Arc::clone(&load_map);
        let h_map = Arc::clone(&heartbeat_map);
        let main_loop = tokio::spawn(async move {
            loop {
                select! {
                    Some(worker_signal) = feedback_rx.recv() => {
                        let mut load_map = load_map.write().await;
                        match worker_signal {
                            WorkerSignal::HeartBeat { id } => {
                                let mut map = heartbeat_map.write().await;
                                map.insert(id, Instant::now());
                            }
                            WorkerSignal::Working {id} => {   
                                if let Some(load) = load_map.get_mut(&id) {
                                    *load += 1;
                                    println!("Worker {} started task", id);
                                } else {
                                    load_map.insert(id, 1);
                                }
                            }
                            WorkerSignal::Finished {id} => {
                                if let Some(load) = load_map.get_mut(&id) {
                                    if *load != 0 {
                                        *load -= 1;
                                        println!("Worker {} finished task", id);
                                    } else {
                                        println!("Worker {} finished untracked task", id);
                                    }      
                                } else {
                                    println!("Worker {} finished untracked task", id);
                                }
                            }
                            WorkerSignal::Failed {id} => {
                                if let Some(load) = load_map.get_mut(&id) {
                                    if *load != 0 {
                                        *load -= 1;
                                        println!("Worker {} failed task", id);
                                    } else {
                                        println!("Worker {} failed untracked task", id);
                                    }      
                                } else {
                                    println!("Worker {} failed untracked task", id);
                                }
                            }
                        }
                    }
                    Some(task) = rx.recv() => {
                        //handle!
                    }
                    Some(cmd) = load_rx.recv() => {
                        match cmd {
                            SchedulerCommand::Upgrade(n) => {
                                
                            }
                            SchedulerCommand::Downgrade(n) => {
                                
                            }
                        }
                    }
                }
            }
        });
        let heartbeat_loop = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                let heartbeat_map = h_map.read().await;
                for (id, last_heartbeat) in heartbeat_map.iter() {
                    if Instant::now().duration_since(*last_heartbeat) > Duration::from_secs(3) {
                        //implement dead workers dropping
                    }
                }
            }
        });
        let load_loop = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3));
            
            loop {
                interval.tick().await;
                let map = l_map.read().await;
                let mut busy_workes: Vec<(usize, usize)> = Vec::with_capacity(map.len());
                let mut idle_workers: Vec<usize> = Vec::with_capacity(map.len());
                for (id, load) in map.iter() {
                    if *load >= 4 {
                        busy_workes.push((*id, *load));
                    }
                    if *load == 0 {
                        idle_workers.push(*id);
                    }
                }
                if idle_workers.len() >= 4 {
                    let ids = idle_workers.iter().take(4).cloned().collect::<Vec<usize>>();
                    let _ = load_tx.send(SchedulerCommand::Downgrade(ids)).await;
                    println!("Sent downgrade command");
                }
                if busy_workes.len() as f64 >= map.len() as f64 * 0.75 {
                    let _ = load_tx.send(SchedulerCommand::Upgrade(4)).await;
                    println!("Sent upgrade command");
                }
            }
        });
        self.load_task = Some(load_loop);
        self.heartbeat_task = Some(heartbeat_loop);
        self.scheduler_task = Some(main_loop);
    }
}

pub async fn upgrade(workers: Arc<RwLock<Vec<Worker>>>, amount: usize, db_pool: MySqlPool) {
    let mut workers = workers.write().await;
    let last_id = workers.len();
    for id in last_id..amount {
        let worker = Worker::spawn(id, db_pool.clone()).await;
        workers.push(worker);
    }
}

pub async fn downgrade(workers: Arc<RwLock<Vec<Worker>>>, to_remove: Vec<usize>) {
    let mut workers = workers.write().await;
    workers.retain(|w| !to_remove.contains(&w.id))
}

//now you have to connect new workers and disconnect old ones! Shit!