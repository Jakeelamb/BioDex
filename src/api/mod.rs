pub mod ensembl;
pub mod gbif;
pub mod inat;
pub mod ncbi;
pub mod ollama;
pub mod wikipedia;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("XML parsing failed: {0}")]
    Xml(#[from] quick_xml::DeError),
    #[error("JSON parsing failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("No results found for query: {0}")]
    NotFound(String),
    #[error("API error: {0}")]
    Api(String),
}

pub type Result<T> = std::result::Result<T, ApiError>;
