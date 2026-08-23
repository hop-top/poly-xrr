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

    /// API misuse at a session boundary (wrong mode, missing adapter id,
    /// double finish) — not a cassette or replay condition.
    #[error("xrr: {0}")]
    Usage(String),

    /// End-of-stream signal: replay's clean terminal on the recv side, and
    /// the post-completion send signal (spec: Matching and Replay
    /// Semantics). Not a failure — the adapter maps it to its own
    /// stream-done value.
    #[error("xrr: end of stream")]
    StreamEnd,

    /// The recorded terminal error of a streamed interaction, re-emitted
    /// verbatim on replay in place of end-of-stream.
    #[error("{0}")]
    StreamRecordedError(String),

    /// Replay-time divergence from the recording: byte-divergent send at
    /// i < S, or half-close after fewer than S sends. Terminal for the
    /// stream handle — every subsequent operation repeats it.
    #[error("xrr: stream mismatch at {op} {ordinal}: {detail}")]
    StreamMismatch {
        op: &'static str,
        ordinal: usize,
        detail: String,
    },
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
