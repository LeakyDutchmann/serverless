use crate::workers::model::{Worker, WorkerSignal, Message};

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
    DropDeadWorker(usize)
}

pub struct Scheduler {
    pub max_workers: usize,
    pub workers: Arc<RwLock<Vec<Worker>>>,
    pub rx: Option<Receiver<String>>,
    pub load_rx: Option<Receiver<usize>>,
    pub feedback_tx: Option<Sender<WorkerSignal>>,
    pub feedback_rx: Option<Receiver<WorkerSignal>>,
    pub scheduler_task: Option<JoinHandle<()>>,
    pub heartbeat_task: Option<JoinHandle<()>>,
    pub load_task: Option<JoinHandle<()>>,
    pub db_pool: MySqlPool,
}

impl Scheduler {
    pub async fn intialize(worker_amount: usize, max_workers: usize, rx: Receiver<String>, db_pool: MySqlPool) -> Self {
        let mut workers = Vec::new();
        let (fb_tx, fb_rx) = channel::<WorkerSignal>(1024);
        for i in 1..=worker_amount {
            let pool = db_pool.clone();
            let worker = Worker::spawn(i, pool, fb_tx.clone()).await;
            workers.push(worker);
        }
        let scheduler = Scheduler {
            max_workers: max_workers,
            workers: Arc::new(RwLock::new(workers)),
            rx: Some(rx),
            feedback_tx: Some(fb_tx),
            feedback_rx: Some(fb_rx),
            load_rx: None,
            load_task: None,
            scheduler_task: None,
            heartbeat_task: None,
            db_pool,
        }; 
        scheduler
    }
    pub async fn run(&mut self) {
        if self.rx.is_none() || self.feedback_rx.is_none() {
            panic!("Main feedback and tasks receivers not found for scheduler, panicking!");
        }
        let mut rx = self.rx.take().unwrap();
        let mut feedback_rx = self.feedback_rx.take().unwrap();

        let (load_tx, mut load_rx) = channel::<SchedulerCommand>(1024);

        let heartbeat_map: Arc<RwLock<HashMap<usize, Instant>>> = Arc::new(RwLock::new(HashMap::new()));
        let load_map: Arc<RwLock<HashMap<usize, usize>>> = Arc::new(RwLock::new(HashMap::new()));
        let l_map = Arc::clone(&load_map);
        let l_map_2 = Arc::clone(&l_map);
        let h_map = Arc::clone(&heartbeat_map);

        let db_pool = self.db_pool.clone();
        let feedback_tx = self.feedback_tx.clone().unwrap();
        let workers = Arc::clone(&self.workers);
        let workers_clone = Arc::clone(&self.workers);
        
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
                        println!("Got request to run this function: {:?}", task);
                        let workers = workers_clone.clone();
                        let l_map = l_map_2.clone();
                        tokio::spawn(async move {
                            let workers = workers.read().await;
                            let map = l_map.read().await;
                            if let Some((id, _)) = map.iter().min_by_key(|(_, v)| *v) {
                                if let Some(worker) = workers.get(*id) {
                                    let result = worker.sender.send(Message::Job(task)).await;
                                    match result {
                                        Ok(_) => {}
                                        Err(e) => println!("Failed to send job to worker: {}", e),
                                    }
                                } else {
                                    println!("Couldn't pick a worker, because load map contains exactly 0 elements");
                                }
                            } else {
                                println!("Couldn't pick a worker, because load map contains exactly 0 elements");
                            }
                        });
                    }
                    Some(cmd) = load_rx.recv() => {
                        match cmd {
                            SchedulerCommand::Upgrade(n) => {
                                let db_pool = db_pool.clone();
                                let feedback_tx = feedback_tx.clone();
                                let workers = workers.clone();
                                tokio::spawn(async move {
                                    upgrade(workers.clone(), n, db_pool.clone(), feedback_tx.clone()).await;
                                });
                            }
                            SchedulerCommand::Downgrade(n) => {
                                downgrade(workers.clone(), n).await;
                            }
                            SchedulerCommand::DropDeadWorker(n) => {
                                drop_dead_worker(workers.clone(), n).await;
                            }
                        }
                    }
                }
            }
        });
        let hb_tx = load_tx.clone();
        let heartbeat_loop = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3));
            let sender = hb_tx.clone();
            loop {
                interval.tick().await;
                let heartbeat_map = h_map.read().await;
                for (id, last_heartbeat) in heartbeat_map.iter() {
                    if Instant::now().duration_since(*last_heartbeat) > Duration::from_secs(3) {
                        let _ = sender.send(SchedulerCommand::DropDeadWorker(*id)).await;
                    }
                }
            }
        });
        let max_workers = self.max_workers;
        let load_tx = load_tx.clone();
        let load_loop = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3));
            let max_workers = max_workers;
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
                    if map.len() >= 8 {
                        let ids = idle_workers.iter().take(4).cloned().collect::<Vec<usize>>();
                        let _ = load_tx.send(SchedulerCommand::Downgrade(ids)).await;
                        println!("Sent downgrade command");  
                        continue
                    }
                   ;
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
        self.load_task = Some(load_loop);
        self.heartbeat_task = Some(heartbeat_loop);
        self.scheduler_task = Some(main_loop);
    }
}

pub async fn upgrade(workers: Arc<RwLock<Vec<Worker>>>, amount: usize, db_pool: MySqlPool, tx: Sender<WorkerSignal>) {
    let mut workers = workers.write().await;
    let last_id = workers.len();
    for id in last_id..last_id + amount {
        let worker = Worker::spawn(id, db_pool.clone(), tx.clone()).await;
        workers.push(worker);
    }
}

pub async fn downgrade(workers: Arc<RwLock<Vec<Worker>>>, to_remove: Vec<usize>) {
    let mut workers = workers.write().await;
    for worker in workers.iter() {
        if to_remove.contains(&worker.id) {
            let _ = worker.sender.send(Message::Stop("Worker {} was downgraded".into())).await;
        }
    }
    workers.retain(|w| !to_remove.contains(&w.id))
}

pub async fn drop_dead_worker(workers: Arc<RwLock<Vec<Worker>>>, to_remove: usize) {
    let mut workers = workers.write().await;
    if let Some(pos) = workers.iter().position(|w| w.id == to_remove) {
        workers[pos].main_loop.abort();
        workers.remove(pos);
    }
}

