pub mod ensembl;
pub mod gbif;
pub mod inat;
pub mod ncbi;
pub mod ollama;
pub mod wikipedia;

use thiserror::Error;

/// A single connection pool and TLS context shared by every API adapter.
/// Cloning `reqwest::Client` is cheap and preserves connection reuse.
pub fn http_client() -> reqwest::Client {
    use std::sync::OnceLock;

    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
        .clone()
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("JSON parsing failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("No results found for query: {0}")]
    NotFound(String),
    #[error("Lookup for {requested} resolved to a different taxon: {resolved}")]
    IdentityMismatch { requested: String, resolved: String },
    #[error("API error: {0}")]
    Api(String),
}

pub type Result<T> = std::result::Result<T, ApiError>;
