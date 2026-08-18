use crate::workers::model::{Worker, WorkerSignal, Message};
use crate::http::response::{Response, StatusCode, send};

use tokio::{sync::mpsc::{Receiver, Sender, channel}, time::{Instant, interval}};
use sqlx::MySqlPool;
use tokio_stream::wrappers::ReceiverStream;
use tokio::task::JoinHandle;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio::select;
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::RwLock;
use tokio::time::Duration;

pub struct Job {
    pub path: String,
    pub input: Vec<u8>,
    pub stream: TcpStream,
}

pub enum SchedulerCommand {
    Upgrade(usize),
    Downgrade(Vec<usize>),
    DropDeadWorker(usize)
}

pub struct Scheduler {
    pub max_workers: usize,
    pub workers: Arc<RwLock<Vec<Worker>>>,
    pub rx: Option<Receiver<Job>>,
    pub load_rx: Option<Receiver<usize>>,
    pub feedback_tx: Option<Sender<WorkerSignal>>,
    pub feedback_rx: Option<Receiver<WorkerSignal>>,
    pub scheduler_task: Option<JoinHandle<()>>,
    pub heartbeat_task: Option<JoinHandle<()>>,
    pub load_task: Option<JoinHandle<()>>,
    pub db_pool: MySqlPool,
}

static NEXT_JOB_ID: AtomicUsize = AtomicUsize::new(1);

impl Scheduler {
    pub async fn intialize(worker_amount: usize, max_workers: usize, rx: Receiver<Job>, db_pool: MySqlPool) -> Self {
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
        let job_map: Arc<RwLock<HashMap<usize, TcpStream>>> = Arc::new(RwLock::new(HashMap::new()));
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
                        let mut job_map = job_map.write().await;
                        match worker_signal {
                            WorkerSignal::HeartBeat { w_id } => {
                                let mut map = heartbeat_map.write().await;
                                map.insert(w_id, Instant::now());
                            }
                            WorkerSignal::Working {w_id, j_id} => {   
                                if let Some(load) = load_map.get_mut(&w_id) {
                                    *load += 1;
                                    println!("Worker {} started task {}", w_id, j_id);
                                } else {
                                    load_map.insert(w_id, 1);
                                }
                            }
                            WorkerSignal::Finished {w_id, j_id, result} => {
                                if let Some(stream) = job_map.get_mut(&j_id) {
                                    if let Some(load) = load_map.get_mut(&w_id) {
                                        if *load != 0 {
                                            *load -= 1;
                                            println!("Worker {} finished task {}", w_id, j_id);
                                            let response = Response::json(StatusCode::Ok, result, None);
                                            send(stream, &response).await;
                                        } else {
                                            let response = Response::json(StatusCode::Ok, result, None);
                                            send(stream, &response).await;
                                            println!("Worker {} finished untracked task", w_id);
                                        }      
                                    } else {
                                        let response = Response::json(StatusCode::Ok, result, None);
                                        send(stream, &response).await;
                                        println!("Worker {} finished untracked task", w_id);
                                    }  
                                    job_map.remove(&j_id);
                                } else {
                                    println!("Worker {} finished task that belongs to no client. Task id: {}", w_id, j_id);
                                }     
                            }
                            WorkerSignal::Failed {w_id, j_id, reason} => {
                                if let Some(stream) = job_map.get_mut(&j_id) {
                                    if let Some(load) = load_map.get_mut(&w_id) {
                                        if *load != 0 {
                                            *load -= 1;
                                            let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                                            send(stream, &response).await;
                                            println!("Worker {} failed task {}", w_id, j_id);
                                        } else {
                                            let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                                            send(stream, &response).await;
                                            println!("Worker {} failed untracked task", w_id);
                                        }      
                                    } else {
                                        let response = Response::json(StatusCode::IntServerError, Vec::new(), Some(reason));
                                        send(stream, &response).await;
                                        println!("Worker {} failed untracked task", w_id);
                                    }
                                    job_map.remove(&j_id);
                                } else {
                                    println!("Worker {} failed task that belongs to no client. Task id: {}, reason {}", w_id, j_id, reason);
                                }   
                            }
                        }
                    }
                    Some(mut task) = rx.recv() => {
                        println!("Got request to run this function: {:?}", task.path);
                        let workers = workers_clone.clone();
                        let l_map = l_map_2.clone();
                        let j_map = job_map.clone();
                        tokio::spawn(async move {
                            let workers = workers.read().await;
                            let map = l_map.read().await;
                            if let Some((id, _)) = map.iter().min_by_key(|(_, v)| *v) {
                                if let Some(worker) = workers.get(*id) {
                                    let j_id = generate_job_id().await;
                                    let result = worker.sender.send(Message::Job{path: task.path, input: task.input, j_id}).await;
                                    match result {
                                        Ok(_) => {
                                            let mut map = j_map.write().await;
                                            map.insert(j_id, task.stream);
                                        }
                                        Err(e) => {
                                            let line = format!("Failed to send job to worker {}", e);
                                            let response = Response::json(StatusCode::IntServerError, vec![], Some(line));
                                            send(&mut task.stream, &response).await;
                                            println!("Failed to send job to worker: {}", e)
                                        },
                                    }
                                } else {
                                    let line = format!("Failed to attach task to a worker because of inconsistent worker id's");
                                    let response = Response::json(StatusCode::IntServerError, vec![], Some(line));
                                    send(&mut task.stream, &response).await;
                                    println!("Tried to attach job to worker with id {}, but there is no such worker in worker pool", id);
                                }
                            } else {
                                let line = format!("Failed to attach task to a worker because there are no workers");
                                let response = Response::json(StatusCode::IntServerError, vec![], Some(line));
                                send(&mut task.stream, &response).await;
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
        workers[pos].jobs.read().await.iter().for_each(|j| j.abort());
        workers.remove(pos);
    }
}

pub async fn generate_job_id() -> usize {
    let id = NEXT_JOB_ID.fetch_add(1, Ordering::Relaxed);
    id
}
