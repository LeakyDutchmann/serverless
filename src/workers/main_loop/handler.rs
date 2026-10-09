use sqlx::{MySqlPool, Row};
use wasmtime::{Engine, Instance, Module, Store};
use tokio::sync::RwLock;
use tokio::time::Instant;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

use crate::http::utils::get_function_name;
use super::wasm_utils::run_wasm;
use crate::scheduler::loops::metrics::model::MetricsPacket;
use super::wasm_imports::model::CallerTable;
use super::wasm_utils::create_wasm_instance;

pub struct JobHandlingError {
    pub message: String,
    pub metrics: Option<MetricsPacket>,
}

impl JobHandlingError {
    pub fn full(message: String, handling_started: Instant, store: &Store<CallerTable>, fuel_init: u64, cold: bool, success: bool) -> Self {
        let m = MetricsPacket::new(handling_started, &store, fuel_init, cold, success);
        Self { message, metrics: Some(m) }
    }
    pub fn partial(message: String) -> Self {
        Self { message, metrics: None }
    }
}

pub async fn handle_job(
    path: String,
    engine: Engine,
    cache_map: Arc<RwLock<HashMap<String, Module>>>,
    input: &[u8],
    db_pool: MySqlPool,
) -> Result<(Vec<u8>, MetricsPacket), JobHandlingError>{
    //metrics part...
    let fuel_init = 10_000;
    let handling_started = Instant::now();
    let c_map = cache_map.read().await;
    let func_name = get_function_name(&path);
    let mut store = Store::new(&engine, CallerTable::new());
    match store.set_fuel(fuel_init) {
        Ok(_) => {}
        Err(e) => {
            let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, true, false);
            return Err(result);
        }
    };
    if let Some(module) = c_map.get(&func_name) {
        let instance = match create_wasm_instance(&engine, &[], &mut store, Some(module.clone())) {
            Ok(instance) => instance,
            Err(e) => {
                println!("Failed to create instance wasm module: {}", e);
                let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, false, false);
                return Err(result);
            }
        };
        println!("Warm start");
        match run_wasm(instance, &mut store, &input).await {
            Ok(result) => {
                let packet = MetricsPacket::new(handling_started, &store, fuel_init, false, true);
                return Ok((result, packet));
            }
            Err(e) => {
                let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, false, false);
                return Err(result);
            }
        }
    } else {
        let result = sqlx::query("SELECT wasm FROM functions WHERE path = ?")
            .bind(&func_name)
            .fetch_optional(&db_pool)
            .await;
        match result {
            Ok(Some(row)) => {
                let wasm: Vec<u8> = match row.try_get("wasm") {
                    Ok(wasm) => wasm,
                    Err(e) => {
                        let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, true, false);
                        return Err(result);
                    }
                };
                let instance = match create_wasm_instance(&engine, &wasm, &mut store, None) {
                    Ok(instance) => instance,
                    Err(e) => {
                        let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, true, false);
                        return Err(result);
                    }
                };
                println!("Cold start");
                match run_wasm(instance, &mut store, &input).await {
                    Ok(result) => {
                        let packet = MetricsPacket::new(handling_started, &store, fuel_init, true, true);
                        return Ok((result, packet));
                    }
                    Err(e) => {
                        let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, true, false);
                        return Err(result);
                    }
                }
            },
            Ok(None) => {
                let result = JobHandlingError::partial("MySql returned Ok(None)".to_string());
                return Err(result);
            }
            Err(e) => {
                let result = JobHandlingError::partial(e.to_string());
                return Err(result)
            }
        };
    }
}