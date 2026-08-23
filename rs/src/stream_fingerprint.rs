//! Streamed fingerprint core + per-session occurrence counters
//! (spec/cassette-format-streaming.md, Fingerprinting / gRPC mapping).
//!
//! The split is structural: this layer owns canonical-JSON assembly, the
//! `"stream"` discriminator, hashing/truncation, and the counter lifecycle;
//! an adapter supplies only its canonical identity inputs, its payload
//! shape, and whether the open is counter-addressed. The gRPC helpers are
//! thin wrappers over the core. Re-exported through `crate::stream`.

use std::collections::{BTreeMap, HashMap};
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

/// Everything a replay needs to locate a streamed cassette at open time.
///
/// The adapter supplies its canonical fingerprint inputs (`identity`), its
/// open-request payload (`payload`), and whether the open is disambiguated
/// by the session's occurrence counter (`counter`); the core owns
/// canonical-JSON assembly, the `"stream"` discriminator,
/// hashing/truncation, and the counter lifecycle.
#[derive(Debug, Clone)]
pub struct StreamOpen {
    pub adapter_id: String,
    pub stream_type: StreamType,
    /// Canonical fingerprint inputs (for gRPC: service, method, and
    /// msg_hash for content-addressed server streams; for an SSE-style
    /// adapter: url). Keys `"stream"` and `"n"` are reserved for core
    /// injection. BTreeMap gives the spec's sorted-key canonical order;
    /// values must serialize deterministically (strings and integers in
    /// practice).
    pub identity: BTreeMap<String, serde_json::Value>,
    /// Counter-addressed open: the identity does not fully identify the
    /// interaction, so the session's occurrence counter — keyed by
    /// (adapter id, stream type, identity) — supplies the 0-based ordinal
    /// `n`, injected as canonical input `"n"` and informational payload
    /// field `"n"`.
    pub counter: bool,
    /// Adapter-defined open-request payload persisted to the req file.
    pub payload: serde_yaml::Mapping,
}

/// Canonical JSON for an open: the adapter identity plus the injected
/// `"stream"` discriminator, plus `"n"` when given. BTreeMap iteration is
/// sorted-key order and serde_json emits no insignificant whitespace —
/// exactly the spec's canonical JSON.
fn stream_canonical(open: &StreamOpen, n: Option<u64>) -> Result<String, XrrError> {
    let mut inputs: BTreeMap<&str, serde_json::Value> = BTreeMap::new();
    for (k, v) in &open.identity {
        if k == "stream" || k == "n" {
            return Err(XrrError::InvalidStream(format!(
                "stream identity key {k:?} is reserved for core injection"
            )));
        }
        inputs.insert(k, v.clone());
    }
    inputs.insert("stream", open.stream_type.as_str().into());
    if let Some(n) = n {
        inputs.insert("n", n.into());
    }
    Ok(serde_json::to_string(&inputs)?)
}

/// Streaming fingerprint for an open: `sha256(canonical_json)[:8]`.
/// Counter-addressed opens require the 0-based occurrence ordinal `n`
/// (always hashed, even when 0); content-addressed opens ignore it — their
/// identity already carries the content hash.
pub fn stream_fingerprint(open: &StreamOpen, n: Option<u64>) -> Result<String, XrrError> {
    let n = match (open.counter, n) {
        (true, None) => {
            return Err(XrrError::InvalidStream(
                "counter-addressed stream open requires an occurrence n".into(),
            ))
        }
        (true, some) => some,
        (false, _) => None,
    };
    Ok(sha256_8(stream_canonical(open, n)?.as_bytes()))
}

fn grpc_identity(service: &str, method: &str) -> BTreeMap<String, serde_json::Value> {
    BTreeMap::from([
        ("service".to_string(), serde_json::Value::from(service)),
        ("method".to_string(), serde_json::Value::from(method)),
    ])
}

/// gRPC server-stream fingerprint — thin wrapper over the core: the single
/// request message is available at open and is content-addressed via
/// `msg_hash`, mirroring unary.
pub fn grpc_server_fingerprint(service: &str, method: &str, message: &[u8]) -> String {
    let mut identity = grpc_identity(service, method);
    identity.insert("msg_hash".to_string(), msg_hash(message).into());
    let open = StreamOpen {
        adapter_id: "grpc".into(),
        stream_type: StreamType::Server,
        identity,
        counter: false,
        payload: serde_yaml::Mapping::new(),
    };
    stream_fingerprint(&open, None).expect("grpc identity is canonical")
}

/// gRPC client/bidi fingerprint — thin wrapper over the core: no message at
/// open, so the 0-based occurrence counter `n` disambiguates repeated opens
/// of one tuple. Always included, even when 0.
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
    let open = StreamOpen {
        adapter_id: "grpc".into(),
        stream_type,
        identity: grpc_identity(service, method),
        counter: true,
        payload: serde_yaml::Mapping::new(),
    };
    stream_fingerprint(&open, Some(n))
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

    fn next_key(&self, key: String) -> u64 {
        let mut counts = self.counts.lock().expect("counter lock");
        let entry = counts.entry(key).or_insert(0);
        let n = *entry;
        *entry += 1;
        n
    }

    /// Consume the occurrence for a counter-addressed open, keyed by the
    /// adapter id plus the canonical identity sans `"n"` — the adapter's
    /// identifying tuple.
    pub fn next_open(&self, open: &StreamOpen) -> Result<u64, XrrError> {
        let base = stream_canonical(open, None)?;
        Ok(self.next_key(format!("{}\0{}", open.adapter_id, base)))
    }

    /// gRPC-shaped convenience: counts exactly as a counter-addressed gRPC
    /// `StreamOpen` of the same tuple would, so manual fingerprinting and
    /// session opens share one counter domain.
    pub fn next(&self, service: &str, method: &str, stream_type: StreamType) -> u64 {
        let canonical = format!(
            r#"{{"method":{},"service":{},"stream":"{}"}}"#,
            json_str(method),
            json_str(service),
            stream_type.as_str(),
        );
        self.next_key(format!("grpc\0{canonical}"))
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
            grpc_counter_fingerprint("files.FileService", "Upload", StreamType::Client, 1)
                .unwrap(),
            "b27b5fe1"
        );
        assert_eq!(
            grpc_counter_fingerprint("chat.ChatService", "Converse", StreamType::Bidi, 0)
                .unwrap(),
            "c6233d2e"
        );
    }

    #[test]
    fn canonical_json_matches_spec_byte_for_byte() {
        // Canonical strings shown in the spec's vector table, produced by
        // the generic core.
        let mut identity = grpc_identity("files.FileService", "Download");
        identity.insert("msg_hash".to_string(), "f1e315a5".into());
        let open = StreamOpen {
            adapter_id: "grpc".into(),
            stream_type: StreamType::Server,
            identity,
            counter: false,
            payload: serde_yaml::Mapping::new(),
        };
        assert_eq!(
            stream_canonical(&open, None).unwrap(),
            r#"{"method":"Download","msg_hash":"f1e315a5","service":"files.FileService","stream":"server"}"#
        );

        let open = StreamOpen {
            adapter_id: "grpc".into(),
            stream_type: StreamType::Client,
            identity: grpc_identity("files.FileService", "Upload"),
            counter: true,
            payload: serde_yaml::Mapping::new(),
        };
        assert_eq!(
            stream_canonical(&open, Some(0)).unwrap(),
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
    fn reserved_identity_keys_rejected() {
        for reserved in ["stream", "n"] {
            let open = StreamOpen {
                adapter_id: "x".into(),
                stream_type: StreamType::Bidi,
                identity: BTreeMap::from([(reserved.to_string(), serde_json::Value::from(1))]),
                counter: false,
                payload: serde_yaml::Mapping::new(),
            };
            assert!(
                matches!(stream_fingerprint(&open, None), Err(XrrError::InvalidStream(_))),
                "identity key {reserved:?} must be rejected"
            );
        }
    }

    #[test]
    fn counter_open_requires_n() {
        let open = StreamOpen {
            adapter_id: "grpc".into(),
            stream_type: StreamType::Bidi,
            identity: grpc_identity("s.S", "M"),
            counter: true,
            payload: serde_yaml::Mapping::new(),
        };
        assert!(stream_fingerprint(&open, None).is_err());
    }

    #[test]
    fn counters_are_per_tuple() {
        let c = StreamCounters::new();
        assert_eq!(c.next("s.S", "M", StreamType::Client), 0);
        assert_eq!(c.next("s.S", "M", StreamType::Client), 1);
        assert_eq!(c.next("s.S", "M", StreamType::Bidi), 0);
        assert_eq!(c.next("s.S", "Other", StreamType::Client), 0);
    }

    #[test]
    fn manual_next_and_open_keyed_count_share_one_domain() {
        let c = StreamCounters::new();
        let open = StreamOpen {
            adapter_id: "grpc".into(),
            stream_type: StreamType::Client,
            identity: grpc_identity("s.S", "M"),
            counter: true,
            payload: serde_yaml::Mapping::new(),
        };
        assert_eq!(c.next("s.S", "M", StreamType::Client), 0);
        assert_eq!(c.next_open(&open).unwrap(), 1);
        assert_eq!(c.next("s.S", "M", StreamType::Client), 2);
    }
}
