use sqlx::{MySqlPool, Row};
use wasmtime::{Engine, Instance, Module, Store};
use tokio::sync::RwLock;
use tokio::time::Instant;
use std::sync::Arc;
use std::collections::HashMap;

use crate::http::utils::get_function_name;
use super::wasm_utils::run_wasm;
use super::metrics::model::{MetricsPacket, update_metrics};
use super::wasm_imports::model::CallerTable;
use super::wasm_utils::create_wasm_instance;


pub async fn handle_job(
    path: String,
    engine: Engine,
    cache_map: Arc<RwLock<HashMap<String, Module>>>,
    input: &[u8],
    db_pool: MySqlPool,
    metrics_map: Arc<RwLock<HashMap<String, MetricsPacket>>>,
) -> Result<Vec<u8>, String>{
    //metrics part...
    let fuel_init = 10_000;
    let handling_started = Instant::now();
    
    let c_map = cache_map.read().await;
    let func_name = get_function_name(&path);
    let mut store = Store::new(&engine, CallerTable::new());
    match store.set_fuel(fuel_init) {
        Ok(_) => {}
        Err(e) => {
            return Err(e.to_string());
        }
    };
    if let Some(module) = c_map.get(&func_name) {
        let instance = match Instance::new(&mut store, &module, &[]) {
            Ok(instance) => instance,
            Err(e) => {
                println!("Failed to create instance wasm module: {}", e);
                return Err(e.to_string());
            }
        };
        match run_wasm(instance, &mut store, &input).await {
            Ok(result) => {
                update_metrics(metrics_map, handling_started, &store, func_name.clone(), fuel_init, false).await;
                return Ok(result);
            }
            Err(e) => {
                return Err(e.to_string());
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
                        return Err(e.to_string());
                    }
                };
                let instance = match create_wasm_instance(&engine, &wasm, &mut store) {
                    Ok(instance) => instance,
                    Err(e) => {
                        return Err(e.to_string());
                    }
                };
                match run_wasm(instance, &mut store, &input).await {
                    Ok(result) => {
                        update_metrics(metrics_map, handling_started, &store, func_name.clone(), fuel_init, true).await;
                        return Ok(result);
                    }
                    Err(e) => {
                        return Err(e.to_string());
                    }
                }
            },
            Ok(None) => {
                return Err("MySql returned Ok(None)".to_string());
            }
            Err(e) => {
                return Err(e.to_string())
            }
        };
    }
}