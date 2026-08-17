use tokio::task::JoinHandle;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc::channel;
use sqlx::MySqlPool;
use tokio::select;
use tokio::time::{Interval, interval, Duration};
use tokio::sync::RwLock;
use std::sync::Arc;

pub enum Message {
    Stop(String),
    Job(String)
}

pub enum WorkerSignal {
    HeartBeat{ id: usize},
    Working{id: usize},
    Finished{id: usize},
    Failed{id: usize},
}

pub struct Worker {
    pub main_loop: JoinHandle<()>,
    pub id: usize,
    pub sender: Sender<Message>,
    pub load: usize, 
    pub jobs: Arc<RwLock<Vec<JoinHandle<()>>>>
}

impl Worker {
    pub async fn spawn(id: usize, db_pool: MySqlPool, fb_tx: Sender<WorkerSignal>) -> Self {
        let (tx, mut rx) = channel::<Message>(1024);
        let mut heartbeat = interval(Duration::from_secs(3));
        let jobs: Arc<RwLock<Vec<JoinHandle<()>>>> = Arc::new(RwLock::new(Vec::new()));
        let jobs_clone = Arc::clone(&jobs);
        let task = tokio::spawn(async move {
            loop {
                let db = db_pool.clone();
                let fb= fb_tx.clone();
                select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            Message::Stop(reason) => {
                                println!("Worker {} received stop signal because of this reason: {}", id, reason);
                                let mut jobs = jobs.write().await;
                                println!("Aborting {} jobs", jobs.len());
                                for job in jobs.iter() {
                                    job.abort();
                                }
                                jobs.clear();
                                println!("Worker {} stopped", id);
                                break;
                            },
                            Message::Job(path) => {
                                let job = tokio::spawn(async move {
                                    let _ = fb.send(WorkerSignal::Working{id}).await;
                                    let result = sqlx::query("SELECT wasm FROM functions WHERE name = ?")
                                        .bind(&path)
                                        .fetch_optional(&db)
                                        .await;
                                    match result {
                                        Ok(Some(_wasm)) => {
                                            println!("Whoops! Forgot to handle succes");
                                        },
                                        Ok(None) => {
                                            println!("Whoops! Forgot to handle Ok(None) case");
                                            let _ = fb.send(WorkerSignal::Failed{id}).await;
                                        }
                                        Err(e) => {
                                            println!("Whoops! Forgot to handle Err case: {:?}", e);
                                            let _ = fb.send(WorkerSignal::Failed{id}).await;
                                        }
                                    };
                                });
                                let mut jobs = jobs.write().await;
                                jobs.push(job);       
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        let result = fb_tx.send(WorkerSignal::HeartBeat{id}).await;
                        match result {
                            Ok(_) => {continue}
                            Err(e) => {
                                let mut jobs = jobs.write().await;
                                println!("Failed to send heartbeat: {:?}. Aborting all jobs from current worker", e);
                                for job in jobs.iter() {
                                    job.abort();
                                }
                                jobs.clear();
                                break;
                            }
                        }
                    }
                }
            }
        });
        Worker {
            main_loop: task,
            id,
            sender: tx,
            load: 0,
            jobs: jobs_clone,
        }
        
    }
}