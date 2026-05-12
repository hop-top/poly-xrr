//! xrr adapter for filesystem mutation operations.
//!
//! Mirrors the Go fs adapter (go/adapters/fs/fs.go). Records and replays
//! mutating fs calls (WriteFile, Mkdir, Chmod, ...) using the same
//! cassette shape as the exec adapter. Reads are intentionally not
//! supported: tests should pre-seed disk state via fixtures and use xrr
//! only to assert on mutations.
//!
//! `data` is a UTF-8 string, not a byte slice — yaml.v3 serializes byte
//! slices as YAML sequence-of-ints (not !!binary), which would break
//! cross-runtime cassette portability. Callers MUST base64-encode
//! non-UTF-8 binary payloads themselves. See spec/cassette-format-v1.md
//! "Data Field Encoding".

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{error::XrrError, Adapter};

/// Op constants for `Request.op`. Adopters SHOULD use these rather than
/// literal strings.
pub mod op {
    pub const WRITE: &str = "write";
    pub const MKDIR: &str = "mkdir";
    pub const REMOVE: &str = "remove";
    pub const RENAME: &str = "rename";
    pub const CHMOD: &str = "chmod";
    pub const CHOWN: &str = "chown";
    pub const SYMLINK: &str = "symlink";
    pub const HARDLINK: &str = "hardlink";
    pub const TRUNCATE: &str = "truncate";
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

/// One fs mutation. `op` selects which fields are meaningful; the
/// adapter does not validate field presence — the wrapper is the right
/// place to enforce per-op invariants.
///
/// `Option` types for `mode`, `uid`, `gid`, `size` distinguish "field
/// unset" from "field set to zero". The fingerprint omits unset fields.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct FsRequest {
    pub op: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<i64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub flags: u32,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub recursive: bool,
}

/// Minimal observable outcome of a mutation. Errors flow through the
/// cassette envelope's `error` field, not through `FsResponse`.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct FsResponse {
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub duration_ms: i64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub bytes_written: i64,
}

pub struct FsAdapter;

impl Adapter for FsAdapter {
    type Req = FsRequest;
    type Resp = FsResponse;

    fn id(&self) -> &str {
        "fs"
    }

    /// Returns sha256(canonical JSON of selected fields)[:4] as hex.
    ///
    /// Field selection (must match Go fs adapter exactly):
    ///   - `op` and `path` always included.
    ///   - `data` hashed (full sha256 hex) and included as `data_sha256`
    ///     when non-empty. Raw bytes never enter the fingerprint.
    ///   - `mode`/`uid`/`gid`/`size` included iff `Some`.
    ///   - `dest` included iff non-empty.
    ///   - `flags` included iff non-zero.
    ///   - `recursive` included iff true.
    ///
    /// `BTreeMap` + `serde_json::to_string` produces lexicographically-
    /// sorted keys, matching Go's `encoding/json` over `map[string]any`.
    fn fingerprint(&self, req: &Self::Req) -> Result<String, XrrError> {
        let mut fields: BTreeMap<&str, Value> = BTreeMap::new();
        fields.insert("op", Value::String(req.op.clone()));
        fields.insert("path", Value::String(req.path.clone()));
        if !req.data.is_empty() {
            let mut h = Sha256::new();
            h.update(req.data.as_bytes());
            fields.insert(
                "data_sha256",
                Value::String(format!("{:x}", h.finalize())),
            );
        }
        if let Some(m) = req.mode {
            fields.insert("mode", Value::from(m));
        }
        if let Some(u) = req.uid {
            fields.insert("uid", Value::from(u));
        }
        if let Some(g) = req.gid {
            fields.insert("gid", Value::from(g));
        }
        if !req.dest.is_empty() {
            fields.insert("dest", Value::String(req.dest.clone()));
        }
        if let Some(s) = req.size {
            fields.insert("size", Value::from(s));
        }
        if req.flags != 0 {
            fields.insert("flags", Value::from(req.flags));
        }
        if req.recursive {
            fields.insert("recursive", Value::Bool(true));
        }

        let canonical = serde_json::to_string(&fields)?;
        let hash = Sha256::digest(canonical.as_bytes());
        Ok(hex::encode(&hash[..4]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_req() -> FsRequest {
        FsRequest {
            op: op::WRITE.into(),
            path: "$TMP/greeting.txt".into(),
            data: "hello, world\n".into(),
            mode: Some(420),
            ..Default::default()
        }
    }

    #[test]
    fn id_is_fs() {
        assert_eq!(FsAdapter.id(), "fs");
    }

    #[test]
    fn fingerprint_deterministic() {
        let a = FsAdapter;
        let r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        let fp2 = a.fingerprint(&r).unwrap();
        assert_eq!(fp1, fp2);
        assert_eq!(fp1.len(), 8);
    }

    /// Cross-runtime conformance: this exact request MUST hash to
    /// `667a7680` per spec/fixtures/fs-write/.
    #[test]
    fn fingerprint_conformance() {
        let fp = FsAdapter.fingerprint(&write_req()).unwrap();
        assert_eq!(fp, "667a7680", "spec conformance fingerprint mismatch");
    }

    #[test]
    fn fingerprint_discriminates_op() {
        let a = FsAdapter;
        let mut r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        r.op = op::MKDIR.into();
        let fp2 = a.fingerprint(&r).unwrap();
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn fingerprint_discriminates_path() {
        let a = FsAdapter;
        let mut r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        r.path = "$TMP/other.txt".into();
        let fp2 = a.fingerprint(&r).unwrap();
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn fingerprint_discriminates_data() {
        let a = FsAdapter;
        let mut r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        r.data = "different payload\n".into();
        let fp2 = a.fingerprint(&r).unwrap();
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn fingerprint_discriminates_mode() {
        // write_req() uses mode=420 (0o644); pick a different mode to
        // ensure the fingerprint changes.
        let a = FsAdapter;
        let mut r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        r.mode = Some(0o600);
        let fp2 = a.fingerprint(&r).unwrap();
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn empty_data_omitted_from_fingerprint() {
        // Two requests differing only in absence vs presence of empty
        // data MUST produce the same fingerprint (empty data is unset).
        let a = FsAdapter;
        let r1 = FsRequest {
            op: op::MKDIR.into(),
            path: "$TMP/d".into(),
            ..Default::default()
        };
        let r2 = FsRequest {
            op: op::MKDIR.into(),
            path: "$TMP/d".into(),
            data: String::new(),
            ..Default::default()
        };
        assert_eq!(a.fingerprint(&r1).unwrap(), a.fingerprint(&r2).unwrap());
    }

    #[test]
    fn unset_mode_differs_from_zero_mode() {
        // Option distinguishes unset from explicit zero.
        let a = FsAdapter;
        let r1 = FsRequest {
            op: op::CHMOD.into(),
            path: "$TMP/f".into(),
            ..Default::default()
        };
        let r2 = FsRequest {
            op: op::CHMOD.into(),
            path: "$TMP/f".into(),
            mode: Some(0),
            ..Default::default()
        };
        assert_ne!(a.fingerprint(&r1).unwrap(), a.fingerprint(&r2).unwrap());
    }
}
