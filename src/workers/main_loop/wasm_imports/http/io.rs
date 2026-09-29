use super::super::model::CallerTable;
use wasmtime::{Linker, Caller};

pub fn register(mut linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    register_read(&mut linker)?;
    register_write(&mut linker)?;
    register_drop(&mut linker)?;
    Ok(())
}

fn register_read(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:io/streams", "read", |mut caller: Caller<'_, CallerTable>, handle: i32, offset: i32, len_to_read: i32| {
        let handle = handle as u32;
        let offset = offset as usize;
        let len_to_read = len_to_read as u64;
        let mut buffer = Vec::new();
        let data = caller.data_mut();
        if let Some((stream, _)) = data.input_streams.get(&handle) {
            match stream.read(len_to_read) {
                Ok(bytes) => {
                    buffer.extend_from_slice(&bytes);
                }
                Err(e) => {
                    println!("error reading input stream: {}", e);
                    return 0;
                }
            }
        }
        if buffer.len() > 0 {
            let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
            memory.write(caller, offset, &buffer).unwrap();
        }
        1
    })?;
    Ok(())
}

fn register_write(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:io/streams", "write", |mut caller: Caller<'_, CallerTable>, handle: i32, ptr: i32, len: i32 | -> i32 {
        let handle = handle as u32;
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let mut buffer = vec![0u8; len as usize];
        memory.read(&caller, ptr as usize, &mut buffer);
        let mut data = caller.data_mut();
        if let Some((stream, _)) = data.output_streams.get_mut(&handle) {
            match stream.write(&buffer) {
                Ok(_) => 1,
                Err(e) => {
                    println!("Failed to write to output stream: {:?}", e);
                    0
                }
            }
        } else {
            0
        }
    })?;
    Ok(())
}

fn register_drop(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "output-stream.drop", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some((_, p_handle)) = data.output_streams.remove(&handle) {
            if let Some((_, stream_handle)) = data.outgoing_body.get_mut(&p_handle.handle) {
                stream_handle.handle = None;
                stream_handle.released = true;
            } else {
                return 0;
            }
            1
        } else {
            0
        }
    })?;
    linker.func_wrap("wasi:http/types", "input-stream.drop", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some((_, p_handle)) = data.input_streams.remove(&handle) {
            if let Some((_, stream_handle)) = data.incoming_body.get_mut(&p_handle.handle) {
                stream_handle.handle = None;
                stream_handle.released = true;
            } else {
                return 0;
            }
            1
        } else {
            0
        }
    })?;
    Ok(())
}
