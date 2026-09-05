//! Cross-port canonical-JSON escaping vectors (spec: Fingerprint Algorithm).
//!
//! The hazard input covers every string-escaping class that has forked
//! fingerprints across ports: HTML-sensitive & < >, a slash, non-ASCII,
//! U+2028/U+2029, the backspace and form-feed short forms, a control byte
//! (U+001F) and DEL. serde_json already emits RFC 8785 §3.2.2.2 string
//! serialization, so these pins guard against a future encoder swap.

use std::collections::BTreeMap;

use hop_top_xrr::adapters::exec::{ExecAdapter, ExecRequest};
use hop_top_xrr::adapters::fs::{FsAdapter, FsRequest};
use hop_top_xrr::stream::{stream_fingerprint, StreamOpen, StreamType};
use hop_top_xrr::Adapter;

fn hazard() -> String {
    format!(
        "a&b<c>/é{}{}\u{8}\u{c}\u{1f}\u{7f}",
        char::from_u32(0x2028).unwrap(),
        char::from_u32(0x2029).unwrap()
    )
}

#[test]
fn stream_fingerprint_matches_hazard_vector() {
    let mut identity = BTreeMap::new();
    identity.insert("k".to_string(), serde_json::Value::from(hazard()));
    let open = StreamOpen {
        adapter_id: "x".into(),
        stream_type: StreamType::Server,
        identity,
        counter: false,
        payload: Default::default(),
    };
    assert_eq!(stream_fingerprint(&open, None).unwrap(), "bcc2c6c3");
}

#[test]
fn serde_json_keeps_hazard_classes_raw() {
    // {"k":"a&b<c>/é<U+2028><U+2029>\b\f<U+001F escaped><DEL>","stream":"server"}
    let canonical =
        serde_json::to_string(&serde_json::json!({"k": hazard(), "stream": "server"})).unwrap();
    assert_eq!(
        hex::encode(canonical.as_bytes()),
        "7b226b223a226126623c633e2fc3a9e280a8e280a95c625c665c75303031667f222c2273747265616d223a22736572766572227d"
    );
}

#[test]
fn fs_fingerprint_matches_hazard_vector() {
    let req = FsRequest {
        op: "write".into(),
        path: hazard(),
        ..Default::default()
    };
    assert_eq!(FsAdapter.fingerprint(&req).unwrap(), "6f2fb087");
}

#[test]
fn exec_fingerprint_matches_hazard_vector() {
    let req = ExecRequest {
        argv: vec!["echo".into(), hazard()],
        stdin: String::new(),
        env: Default::default(),
    };
    assert_eq!(ExecAdapter.fingerprint(&req).unwrap(), "97618387");
}
