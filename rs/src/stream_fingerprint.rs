//! Streamed fingerprint algorithms + per-session occurrence counters
//! (spec/cassette-format-streaming.md, Fingerprinting / gRPC mapping).
//! Re-exported through `crate::stream`.

use std::collections::HashMap;
use std::sync::Mutex;

use sha2::{Digest, Sha256};

use crate::{error::XrrError, stream::StreamType};

fn sha256_8(data: &[u8]) -> String {
    hex::encode(&Sha256::digest(data)[..4])
}

/// v1 building block: `sha256(message_bytes)[:8]`.
pub fn msg_hash(message: &[u8]) -> String {
    sha256_8(message)
}

fn json_str(s: &str) -> String {
    serde_json::to_string(s).expect("string serializes")
}

/// gRPC server-stream fingerprint: the single request message is available
/// at open and is content-addressed via `msg_hash`, mirroring unary.
/// Canonical JSON built byte-for-byte in sorted key order.
pub fn grpc_server_fingerprint(service: &str, method: &str, message: &[u8]) -> String {
    let canonical = format!(
        r#"{{"method":{},"msg_hash":{},"service":{},"stream":"server"}}"#,
        json_str(method),
        json_str(&msg_hash(message)),
        json_str(service),
    );
    sha256_8(canonical.as_bytes())
}

/// gRPC client/bidi fingerprint: no message at open, so the 0-based
/// occurrence counter `n` disambiguates repeated opens of one tuple.
/// Always included, even when 0.
pub fn grpc_counter_fingerprint(
    service: &str,
    method: &str,
    stream_type: StreamType,
    n: u64,
) -> Result<String, XrrError> {
    if stream_type == StreamType::Server {
        return Err(XrrError::InvalidStream(
            "server streams are content-addressed; use grpc_server_fingerprint".into(),
        ));
    }
    let canonical = format!(
        r#"{{"method":{},"n":{},"service":{},"stream":"{}"}}"#,
        json_str(method),
        n,
        json_str(service),
        stream_type.as_str(),
    );
    Ok(sha256_8(canonical.as_bytes()))
}

/// Per-session occurrence counters: one session object is one counter
/// domain, keyed by the adapter's identifying tuple, incremented at each
/// open, counted identically in record and replay modes.
#[derive(Debug, Default)]
pub struct StreamCounters {
    counts: Mutex<HashMap<String, u64>>,
}

impl StreamCounters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the 0-based occurrence for this open, then increments.
    pub fn next(&self, service: &str, method: &str, stream_type: StreamType) -> u64 {
        let key = format!("{service}/{method}/{}", stream_type.as_str());
        let mut counts = self.counts.lock().expect("counter lock");
        let entry = counts.entry(key).or_insert(0);
        let n = *entry;
        *entry += 1;
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The six embedded spec vectors (cassette-format-streaming.md,
    // Fingerprint Algorithms) — reproduced byte-for-byte.

    #[test]
    fn spec_vector_msg_hashes() {
        assert_eq!(msg_hash(br#"{"path":"/etc/hosts"}"#), "f1e315a5");
        assert_eq!(msg_hash(br#"{"path":"/var/log/big.log"}"#), "164658bd");
    }

    #[test]
    fn spec_vector_server_fingerprints() {
        assert_eq!(
            grpc_server_fingerprint("files.FileService", "Download", br#"{"path":"/etc/hosts"}"#),
            "58a4bf3f"
        );
        assert_eq!(
            grpc_server_fingerprint(
                "files.FileService",
                "Download",
                br#"{"path":"/var/log/big.log"}"#
            ),
            "9e8c4d4c"
        );
    }

    #[test]
    fn spec_vector_counter_fingerprints() {
        assert_eq!(
            grpc_counter_fingerprint("files.FileService", "Upload", StreamType::Client, 0)
                .unwrap(),
            "2bebfd6f"
        );
        assert_eq!(
            grpc_counter_fingerprint("chat.ChatService", "Converse", StreamType::Bidi, 0)
                .unwrap(),
            "c6233d2e"
        );
    }

    #[test]
    fn canonical_json_matches_spec_byte_for_byte() {
        // Canonical strings shown in the spec's vector table.
        let canonical = format!(
            r#"{{"method":{},"msg_hash":{},"service":{},"stream":"server"}}"#,
            json_str("Download"),
            json_str("f1e315a5"),
            json_str("files.FileService"),
        );
        assert_eq!(
            canonical,
            r#"{"method":"Download","msg_hash":"f1e315a5","service":"files.FileService","stream":"server"}"#
        );
        let canonical = format!(
            r#"{{"method":{},"n":{},"service":{},"stream":"{}"}}"#,
            json_str("Upload"),
            0,
            json_str("files.FileService"),
            StreamType::Client.as_str(),
        );
        assert_eq!(
            canonical,
            r#"{"method":"Upload","n":0,"service":"files.FileService","stream":"client"}"#
        );
    }

    #[test]
    fn counter_fingerprint_rejects_server_type() {
        assert!(
            grpc_counter_fingerprint("s.S", "M", StreamType::Server, 0).is_err()
        );
    }

    #[test]
    fn counters_are_per_tuple() {
        let c = StreamCounters::new();
        assert_eq!(c.next("s.S", "M", StreamType::Client), 0);
        assert_eq!(c.next("s.S", "M", StreamType::Client), 1);
        assert_eq!(c.next("s.S", "M", StreamType::Bidi), 0);
        assert_eq!(c.next("s.S", "Other", StreamType::Client), 0);
    }
}
