use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use crate::routes::api::deployment::chunk_parser::{parser::read_chunks_from_buffer, models::{ChunkReadingError, ChunkParserResult}};

use super::find_headers_end;

pub async fn read_headers(stream: &mut TcpStream) -> anyhow::Result<(Vec<u8>, Vec<u8>, usize)> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut temp = [0u8; 1024];
    loop {
        let n = stream.read(&mut temp).await;
        match n {
            Ok(0) => return Err(anyhow::anyhow!("Unexpected close of connection")),
            Ok(n) => buf.extend_from_slice(&temp[..n]),
            Err(e) => return Err(e.into()),
        }
        if let Some(pos) = find_headers_end(&buf) {
            return Ok((buf[..pos].to_vec(), buf[pos..].to_vec(), pos));
        }
        if buf.len() > 64 * 1024 {
            return Err(anyhow::anyhow!("Request headers section is too large"));
        }
    }
}

pub async fn read_body_chunked(stream: &mut TcpStream, leftover: &[u8]) -> Result<Vec<u8>, ChunkReadingError> {
    let mut body: Vec<u8> = Vec::new();
    let mut master_buffer: Vec<u8> = Vec::new();
    master_buffer.extend_from_slice(leftover); 
    match read_chunks_from_buffer(&mut master_buffer, &mut body).await {
        ChunkParserResult::Done => {return Ok(body)}
        ChunkParserResult::NeedMoreData => {}
        ChunkParserResult::Error(e) => {
            eprintln!("error ocured while parsing chunk of data: {}", e);
        }
    }
    let mut new_buffer = [0u8; 1024];
    loop {
        let n = match stream.read(&mut new_buffer).await {
            Ok(n) => n,
            Err(e) => {
                return Err(ChunkReadingError::IoError(e))
            },
        };
        if n == 0 {
            return Err(ChunkReadingError::UnexpectedEOF);
        }
        master_buffer.extend_from_slice(&new_buffer[..n]);
        match read_chunks_from_buffer(&mut master_buffer, &mut body).await {
            ChunkParserResult::Done => {return Ok(body)}
            ChunkParserResult::NeedMoreData => {continue}
            ChunkParserResult::Error(e) => {
                return Err(ChunkReadingError::ParserError(e))
            }
        }
    }
}