use super::model::CallerTable;
use wasmtime::{Linker, Caller};
use rand::RngExt;

pub fn provide_random(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    match linker.func_wrap("host", "random_u64", |mut caller: Caller<'_, CallerTable>| -> i64{
        let mut rng = rand::rng();
        let random = rng.random::<u64>() as i64;
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.peak = m_usage.max(data.memory_usage.peak);
        data.memory_usage.min = m_usage.min(data.memory_usage.min);
        random
    }) {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow::anyhow!("Failed to provide random: {}", e)),
    }
}