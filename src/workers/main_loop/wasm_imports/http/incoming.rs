use crate::workers::main_loop::wasm_imports::model::{StreamHandle, ParentHandle};

use super::super::model::CallerTable;

use wasmtime::Caller;
use wasmtime::Linker;
use wasi::http::types::IncomingBody;

pub fn register(mut linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    register_status(&mut linker)?;
    register_body(&mut linker)?;
    register_headers(&mut linker)?;
    register_stream(&mut linker)?;
    register_drop_response(&mut linker)?;
    register_drop_body(&mut linker)?;
    register_finish_body(&mut linker)?;
    register_future_response_poll(&mut linker)?;
    Ok(())
}

fn register_future_response_poll(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "future-incoming-response.poll", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some(future) = data.future_incoming_responses.get_mut(&handle) {
            let status = future.subscribe();
            if status.ready() {
                let result = future.get().unwrap();
                let result = match result {
                    Ok(res) => {res},
                    Err(e) => {
                        println!("failed to receive response: {:?}", e);
                        return 0;
                    }
                };
                match result {
                    Ok(incoming_response) => {
                        let new_handle = data.next_handle;
                        data.next_handle += 1;
                        data.incoming_responses.insert(new_handle, incoming_response);
                        return new_handle as i32
                    },
                    Err(e) => {
                        println!("failed to receive response: {:?}", e);
                        return 0;
                    }
                }
            }
        }
        0
    })?;
    Ok(())
}

fn register_finish_body(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "incoming-body.finish", |mut caller: Caller<'_, CallerTable>, handle: i32| -> (i32, i32) {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some((body, mut stream_handle)) = data.incoming_body.remove(&handle) {
            if stream_handle.handle.is_some() {
                if let Some(_) = data.input_streams.remove(&stream_handle.handle.unwrap()) {
                    stream_handle.released = true;
                } else {
                    return (1, 0)
                }
            }
            //handle trailers!!!
            let trailer =  IncomingBody::finish(body); 
            (0, 0)
        } else {
            return (1, 0)
        }
    })?;
    Ok(())
}

fn register_drop_body(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "drop-incoming-body", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some((_, stream_handle)) = data.incoming_body.remove(&handle) {
            if let Some(s_handle) = stream_handle.handle {
                data.input_streams.remove(&s_handle);
            }
            1
        } else {
            0
        }
    })?;
    Ok(())
}

fn register_drop_response(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "drop-incoming-response", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some(_) = data.incoming_responses.remove(&handle) {
            1
        } else {
            0
        }
    })?;
    Ok(())
}

fn register_status(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "incoming-response.status", |mut caller: Caller<'_, CallerTable>, handle: i32 | -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some(response) = data.incoming_responses.get(&handle) {
            return response.status() as i32
        } else {
            return 0
        }
    })?;
    Ok(())
}

fn register_headers(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "incoming-response.headers", |mut caller: Caller<'_, CallerTable>, handle: i32, offset: i32 | -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        let mut buffer = Vec::new();
        let mut len = 0;
        let offset = offset as usize;
        if let Some(response) = data.incoming_responses.get(&handle) {
            let headers = response.headers().entries();
            for (name, value) in headers {
                len += name.as_bytes().len() + value.len();
                buffer.extend_from_slice(name.as_bytes());
                buffer.extend(value);
            }
        } else {
            return 0
        }
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        match memory.write(caller, offset, &buffer) {
            Ok(_) => {if len != 0 { len as i32 } else { 1 }},
            Err(_) => 0,
        }
    })?;
    Ok(())
}

fn register_body(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "incoming-response.body", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some(resp) = data.incoming_responses.get(&handle) {
            match resp.consume() {
                Ok(body) => {
                    let new_handle = data.next_handle;
                    data.next_handle += 1;
                    data.incoming_body.insert(new_handle, (body, StreamHandle {handle: None, released: false}));
                    return new_handle as i32
                }
                Err(e) => {
                    println!("failed to consume incoming response: {:?}", e);
                    return 0;
                }
            }
        }
        0
    })?;
    Ok(())
}

fn register_stream(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "incoming-body.stream", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        let data = caller.data_mut();
        if let Some((body, stream_handle)) = data.incoming_body.get_mut(&handle) {
            if stream_handle.handle.is_some() || stream_handle.released {
                return 0
            }
            let new_handle = data.next_handle;
            data.next_handle += 1;
            match body.stream() {
                Ok(stream) => {
                    stream_handle.handle.replace(new_handle);
                    data.input_streams.insert(new_handle, (stream, ParentHandle { handle }));
                    return new_handle as i32
                }
                Err(e) => {
                    println!("failed to get input stream: {:?}", e);
                    return 0;
                }
            }
        }
        0
    })?;
    Ok(())
}
