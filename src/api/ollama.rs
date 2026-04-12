use super::{wikipedia::WikiArticle, wikipedia::WikiLifeHistoryFallback, ApiError, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DEFAULT_OLLAMA_URL: &str = "http://127.0.0.1:11434";
const DEFAULT_OLLAMA_MODEL: &str = "gemma4:latest";
const DEFAULT_OLLAMA_TIMEOUT_SECS: u64 = 15;
const OLLAMA_CONTEXT_LIMIT_CHARS: usize = 12_000;

pub struct OllamaClient {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    timeout: Duration,
    enabled: AtomicBool,
}

#[derive(Serialize)]
struct GenerateRequest {
    model: String,
    prompt: String,
    stream: bool,
    format: &'static str,
    options: GenerateOptions,
}

#[derive(Serialize)]
struct GenerateOptions {
    temperature: f32,
    num_predict: u32,
}

#[derive(Deserialize)]
struct GenerateResponse {
    response: String,
}

#[derive(Debug, Deserialize)]
struct OllamaLifeHistoryResponse {
    lifespan_years: Option<f64>,
    lifespan_evidence: Option<String>,
    length_meters: Option<f64>,
    length_evidence: Option<String>,
    height_meters: Option<f64>,
    height_evidence: Option<String>,
    mass_kilograms: Option<f64>,
    mass_evidence: Option<String>,
    #[serde(default)]
    reproduction_modes: Vec<String>,
    #[serde(default)]
    reproduction_evidence: Vec<String>,
}

impl OllamaClient {
    pub fn new() -> Self {
        let endpoint = std::env::var("POKETEXT_OLLAMA_URL")
            .unwrap_or_else(|_| DEFAULT_OLLAMA_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let model = std::env::var("POKETEXT_OLLAMA_MODEL")
            .unwrap_or_else(|_| DEFAULT_OLLAMA_MODEL.to_string());
        let timeout = std::env::var("POKETEXT_OLLAMA_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_OLLAMA_TIMEOUT_SECS));

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            client,
            endpoint,
            model,
            timeout,
            enabled: AtomicBool::new(true),
        }
    }

    pub async fn extract_life_history(
        &self,
        title: &str,
        article: &WikiArticle,
    ) -> Result<WikiLifeHistoryFallback> {
        if !self.enabled.load(Ordering::Relaxed) {
            return Err(ApiError::Api("Ollama enrichment disabled".to_string()));
        }

        let request = GenerateRequest {
            model: self.model.clone(),
            prompt: self.build_prompt(title, article),
            stream: false,
            format: "json",
            options: GenerateOptions {
                temperature: 0.0,
                num_predict: 256,
            },
        };

        let payload = tokio::time::timeout(self.timeout, async {
            let response = self
                .client
                .post(format!("{}/api/generate", self.endpoint))
                .json(&request)
                .send()
                .await?;

            response.json::<GenerateResponse>().await
        })
        .await;

        let payload = match payload {
            Ok(Ok(payload)) => payload,
            Ok(Err(error)) => {
                self.enabled.store(false, Ordering::Relaxed);
                return Err(ApiError::Request(error));
            }
            Err(_) => {
                self.enabled.store(false, Ordering::Relaxed);
                return Err(ApiError::Api("Ollama enrichment timed out".to_string()));
            }
        };

        let parsed: OllamaLifeHistoryResponse = serde_json::from_str(&payload.response)?;
        Ok(parsed.into_fallback(article))
    }

    fn build_prompt(&self, title: &str, article: &WikiArticle) -> String {
        format!(
            "Extract life-history facts for the taxon below.\n\
Return JSON only.\n\
Use null when a field is not explicitly supported by the source text.\n\
Convert values to SI units.\n\
Evidence fields must be verbatim substrings copied from the source text.\n\
Reproduction modes must be short labels like Sexual, Asexual, Oviparous, Viviparous, Ovoviviparous, Hermaphroditic.\n\n\
JSON schema:\n\
{{\n\
  \"lifespan_years\": number | null,\n\
  \"lifespan_evidence\": string | null,\n\
  \"length_meters\": number | null,\n\
  \"length_evidence\": string | null,\n\
  \"height_meters\": number | null,\n\
  \"height_evidence\": string | null,\n\
  \"mass_kilograms\": number | null,\n\
  \"mass_evidence\": string | null,\n\
  \"reproduction_modes\": string[],\n\
  \"reproduction_evidence\": string[]\n\
}}\n\n\
Taxon: {title}\n\n\
Source text:\n{context}",
            context = article.llm_context(OLLAMA_CONTEXT_LIMIT_CHARS)
        )
    }
}

impl OllamaLifeHistoryResponse {
    fn into_fallback(self, article: &WikiArticle) -> WikiLifeHistoryFallback {
        let source = normalize_for_match(&article.plain_text());

        WikiLifeHistoryFallback {
            lifespan_years: validate_numeric(self.lifespan_years, self.lifespan_evidence, &source),
            length_meters: validate_numeric(self.length_meters, self.length_evidence, &source),
            height_meters: validate_numeric(self.height_meters, self.height_evidence, &source),
            mass_kilograms: validate_numeric(self.mass_kilograms, self.mass_evidence, &source),
            reproduction_modes: validate_reproduction_modes(
                self.reproduction_modes,
                self.reproduction_evidence,
                &source,
            ),
        }
    }
}

fn validate_numeric(value: Option<f64>, evidence: Option<String>, source: &str) -> Option<f64> {
    let evidence = evidence
        .as_deref()
        .map(normalize_for_match)
        .filter(|snippet| !snippet.is_empty())?;

    if source.contains(&evidence) {
        value
    } else {
        None
    }
}

fn validate_reproduction_modes(
    modes: Vec<String>,
    evidence: Vec<String>,
    source: &str,
) -> Vec<String> {
    if modes.is_empty() {
        return Vec::new();
    }

    let has_supported_evidence = evidence
        .iter()
        .map(|snippet| normalize_for_match(snippet))
        .any(|snippet| !snippet.is_empty() && source.contains(&snippet));

    if has_supported_evidence {
        WikiLifeHistoryFallback::normalize_reproduction_modes(modes)
    } else {
        Vec::new()
    }
}

fn normalize_for_match(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
