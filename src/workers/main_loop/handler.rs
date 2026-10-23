use sqlx::{MySqlPool, Row};
use wasmtime::{Engine, Module, Store};
use tokio::sync::RwLock;
use tokio::time::Instant;
use std::sync::Arc;
use std::collections::HashMap;

use crate::http::utils::get_function_name;
use super::wasm_utils::run_wasm;
use crate::scheduler::loops::metrics::model::MetricsPacket;
use super::wasm_imports::model::CallerTable;
use crate::sharded::ShardedMap;
use super::wasm_utils::create_wasm_instance;

pub struct JobHandlingError {
    pub message: String,
    pub metrics: Option<MetricsPacket>,
}

struct JobContext<'a> {
    engine: &'a Engine,
    store: &'a mut Store<CallerTable>,
    handling_started: Instant,
    fuel_init: u64,
    cold_start: bool,
    input: &'a [u8],
}

impl JobHandlingError {
    pub fn full(message: String, handling_started: Instant, store: &Store<CallerTable>, fuel_init: u64, cold: bool, success: bool) -> Self {
        let m = MetricsPacket::new(handling_started, store, fuel_init, cold, success);
        Self { message, metrics: Some(m) }
    }
    pub fn partial(message: String) -> Self {
        Self { message, metrics: None }
    }
}

pub async fn handle_job(
    path: String,
    engine: Engine,
    cache_map: Arc<ShardedMap<String, Arc<Module>>>,
    input: &[u8],
    db_pool: MySqlPool,
) -> Result<(Vec<u8>, MetricsPacket), JobHandlingError>{
    //metrics part...
    let fuel_init = 10_000;
    let handling_started = Instant::now();
    let func_name = get_function_name(&path);
    
    let mut store = Store::new(&engine, CallerTable::new());
    match store.set_fuel(fuel_init) {
        Ok(_) => {}
        Err(e) => {
            let result = JobHandlingError::full(e.to_string(), handling_started, &store, fuel_init, true, false);
            return Err(result);
        }
    };
    
    let module = cache_map.get(&func_name, |m| Arc::clone(&m)).await;

    let mut ctx = JobContext {
        engine: &engine,
        store: &mut store, 
        handling_started,
        fuel_init,
        cold_start: module.is_none(),
        input
    };
    
    if let Some(module) = module {
        return instantiate_and_run(&mut ctx, Some(module), None).await;
    }

    let row = sqlx::query("SELECT wasm FROM functions WHERE path = ?")
        .bind(&func_name)
        .fetch_optional(&db_pool)
        .await
        .map_err(|e| JobHandlingError::partial(e.to_string()))?;

    let Some(row) = row else { return Err(JobHandlingError::partial("Not found in database".to_string()))};
    let wasm: Vec<u8> = row.try_get("wasm")
        .map_err(|e| JobHandlingError::full(
            e.to_string(), handling_started, ctx.store, fuel_init, ctx.cold_start, false
    ))?;
    
    
    instantiate_and_run(&mut ctx, None, Some(&wasm)).await 
}

async fn instantiate_and_run(ctx: &mut JobContext<'_>, module: Option<Arc<Module>>, wasm: Option<&[u8]> ) -> Result<(Vec<u8>, MetricsPacket), JobHandlingError>{
    let instance = match module {
        Some(m) => {create_wasm_instance(ctx.engine, None, ctx.store, Some(m))},
        None => {create_wasm_instance(ctx.engine, wasm, ctx.store, None)}
    }.map_err(|e| JobHandlingError::full(
        e.to_string(), ctx.handling_started, ctx.store, ctx.fuel_init, ctx.cold_start, false
    ))?;
    
    match run_wasm(instance, ctx.store, ctx.input).await {
        Ok(result) => {
            let packet = MetricsPacket::new(ctx.handling_started, ctx.store, ctx.fuel_init, ctx.cold_start, true);
            Ok((result, packet))
        }
        Err(e) => {
            let result = JobHandlingError::full(e.to_string(), ctx.handling_started, ctx.store, ctx.fuel_init, ctx.cold_start, false);
            Err(result)
        }
    }
}