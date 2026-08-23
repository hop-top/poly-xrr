use thiserror::Error;

#[derive(Debug, Error)]
pub enum XrrError {
    #[error("xrr: cassette miss for adapter={adapter} fp={fingerprint}")]
    CassetteMiss { adapter: String, fingerprint: String },

    #[error("xrr: io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("xrr: serde error: {0}")]
    Serde(#[from] serde_yaml::Error),

    #[error("xrr: json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("xrr: invalid stream cassette: {0}")]
    InvalidStream(String),

    #[error("xrr: stream shape mismatch: {0}")]
    ShapeMismatch(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cassette_miss_display() {
        let e = XrrError::CassetteMiss {
            adapter: "exec".into(),
            fingerprint: "a3f9c1b2".into(),
        };
        assert_eq!(
            e.to_string(),
            "xrr: cassette miss for adapter=exec fp=a3f9c1b2"
        );
    }
}
