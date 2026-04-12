use std::fmt::Display;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub fn timing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| {
        std::env::var("POKETEXT_TIMING")
            .ok()
            .map(|value| {
                let normalized = value.trim().to_ascii_lowercase();
                !normalized.is_empty() && normalized != "0" && normalized != "false"
            })
            .unwrap_or(false)
    })
}

pub fn start_span() -> Option<Instant> {
    timing_enabled().then(Instant::now)
}

pub fn log_elapsed(label: &str, start: Option<Instant>) {
    if let Some(start) = start {
        log_duration(label, start.elapsed());
    }
}

pub fn log_duration(label: &str, duration: Duration) {
    if timing_enabled() {
        eprintln!("[timing] {:<28} {:>6} ms", label, duration.as_millis());
    }
}

pub fn log_value(label: &str, value: impl Display) {
    if timing_enabled() {
        eprintln!("[timing] {} {}", label, value);
    }
}
