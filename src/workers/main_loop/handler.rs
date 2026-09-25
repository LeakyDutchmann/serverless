use sqlx::{MySqlPool, Row};
use wasmtime::{Engine, Instance, Module, Store};
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use std::mem;
use std::sync::Arc;
use std::collections::HashMap;

use crate::http::utils::get_function_name;
use super::wasm_utils::run_wasm;
use super::init::{MetricsPacket, MemoryUsage};
use super::wasm_utils::create_wasm_instance;

pub async fn update_metrics(metrics_map: Arc<RwLock<HashMap<String, MetricsPacket>>>, handling_started: Instant, store: &Store<MemoryUsage>, path: String, fuel_init: u64, cold: bool) {
    let handling_ended = Instant::now();
    let duration = handling_ended - handling_started;
    let fuel_used = match store.get_fuel() {
        Ok(fuel) => {
            if fuel < fuel_init {
                fuel_init - fuel
            } else {
                fuel_init
            }
        },
        Err(e) => {
            println!("MetricsErr: failed to fetch fuel from store: {:?}", e);
            0
        },
    };
    if let Some(m) = metrics_map.write().await.get_mut(&path) {
        m.invocations += 1;
        m.successes += 1;
        m.duration_min = m.duration_min.min(duration);
        m.duration_max = m.duration_max.max(duration);
        m.duration_mean = Duration::from_millis(123);
        m.window_end = handling_ended;
        m.memory_peak = m.memory_peak.max(store.data().peak);
        m.memory_min = m.memory_min.min(store.data().min);
        m.fuel_used_total += fuel_used;
        if cold {
            m.cold_starts += 1;
        }
        println!("metrics: {:?}", m);
    } else {
        let mut measurements = MetricsPacket {
            invocations: 1,
            successes: 1,
            failures: 0,
            cold_starts: 0,
            fuel_used_total: fuel_used,
            duration_min: duration,
            duration_max: duration,
            duration_mean: duration,
            window_start: handling_started,
            window_end: handling_ended,
            memory_peak: store.data().peak,
            memory_min: store.data().min,
        };
        if cold {
            measurements.cold_starts += 1;
        }
        println!("metrics: {:?}", measurements);
        metrics_map.write().await.insert(path.clone(), measurements);
    }
}

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
    let mut store = Store::new(&engine, MemoryUsage::zero());
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