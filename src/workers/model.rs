use tokio::task::JoinHandle;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc::channel;
use sqlx::MySqlPool;
use tokio::select;
use tokio::time::{Interval, interval, Duration};

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
    pub task: JoinHandle<()>,
    pub id: usize,
    pub sender: Sender<Message>,
    pub load: usize,
}

impl Worker {
    pub async fn spawn(id: usize, db_pool: MySqlPool, fb_tx: Sender<WorkerSignal>) -> Self {
        let (tx, mut rx) = channel::<Message>(1024);
        let mut heartbeat = interval(Duration::from_secs(3));
        let task = tokio::spawn(async move {
            loop {
                let db = db_pool.clone();
                let fb= fb_tx.clone();
                select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            Message::Stop(_) => {
                                //handle stop
                            },
                            Message::Job(path) => {
                                tokio::spawn(async move {
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
                                
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        let result = fb_tx.send(WorkerSignal::HeartBeat{id}).await;
                        match result {
                            Ok(_) => {continue}
                            Err(e) => {
                                println!("Failed to send heartbeat: {:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
        });
        Worker {
            task,
            id,
            sender: tx,
            load: 0,
        }
        
    }
}