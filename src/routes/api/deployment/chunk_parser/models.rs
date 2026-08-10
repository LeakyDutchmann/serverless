use std::fmt::Display;
use std::fmt::Formatter;

pub enum ChunkReadingError {
    UnexpectedEOF,
    ParserError(ChunkParserError),
    IoError(std::io::Error),
    ParseError(HttpParseError),
}


pub enum HttpParseError {
    MissingBodySeparator,
}

impl Display for HttpParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpParseError::MissingBodySeparator => {
                write!(f, "Missing body separator")
            }
        }
    }
}


impl Display for ChunkReadingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkReadingError::UnexpectedEOF => {
                write!(f, "Connection was closer earlier before reader could finish reading chunk, EOF error")
            }
            ChunkReadingError::ParserError(e) => {
                write!(f, "Parser error: {}", e)
            }
            ChunkReadingError::IoError(e) => {
                write!(f, "IO error: {}", e)
            }
            ChunkReadingError::ParseError(e) => {
                write!(f, "Parse error: {}", e)
            }
        }
    }
}


pub enum ChunkParserResult {
    Done,
    NeedMoreData,
    Error(ChunkParserError)
}


#[derive(Debug)]
pub enum ChunkParserError {
    InvalidUtf8{line: Vec<u8>},
    InvalidChunkSize{line: String},
    MissingCRLF{after_chunk: Vec<u8>},
    TrailingGarbage { bytes: Vec<u8> },
}

impl Display for ChunkParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkParserError::InvalidUtf8 { line } => {
                write!(f, "Invalid Utf8 characters: {}", String::from_utf8_lossy(&line))
            }
            ChunkParserError::InvalidChunkSize{line} => {
                write!(f, "Size: {}", line)
            }
            ChunkParserError::MissingCRLF{after_chunk} => {
                write!(f, "The line: {} is placed instead of proper CRLF", String::from_utf8_lossy(&after_chunk))
            }
            ChunkParserError::TrailingGarbage { bytes } => {
                write!(f, "Trailing garbage: {:?}", bytes)
            }
        }
    }
}