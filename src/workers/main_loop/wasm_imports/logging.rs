use super::model::CallerTable;
use wasmtime::{Linker, Caller};

pub fn provide_logging(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    match linker.func_wrap("host", "log", |mut caller: Caller<'_, CallerTable>, ptr: i32, len: i32 |  {
        if len <= 0 {
            return;
        }
        let mut buffer = vec![0u8; len as usize];
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let metrics = caller.data_mut();
        metrics.memory_usage.peak = m_usage.max(metrics.memory_usage.peak);
        metrics.memory_usage.min = m_usage.min(metrics.memory_usage.min);
        let _ = memory.read(&mut caller, ptr as usize, &mut buffer).unwrap();
        println!("WASMLOG: {}", String::from_utf8_lossy(&buffer[..len as usize]));
    }) {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow::anyhow!("Failed to provide logging: {}", e)),
    }
}