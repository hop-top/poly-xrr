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
//!
//! Paths are fingerprinted in their post-normalizer form (see
//! [`FsAdapter::with_normalizer`]) so tmpdir-based tests key the same
//! cassette on every run and across language ports. See
//! spec/cassette-format-v1.md "Path Normalization".

use std::{collections::BTreeMap, fmt, sync::Arc};

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

/// Rewrites a path before it enters the fingerprint. Default is
/// identity. Returning `""` is allowed and taken literally — adopters
/// can drop path info if they really want to.
///
/// Build one with [`normalizer`] and compose several with [`chain`].
pub type PathNormalizer = Arc<dyn Fn(&str) -> String + Send + Sync>;

/// Wraps `f` as a shareable [`PathNormalizer`]. Only needed to hand
/// closures to [`chain`]; [`FsAdapter::with_normalizer`] takes a bare
/// closure directly.
pub fn normalizer(f: impl Fn(&str) -> String + Send + Sync + 'static) -> PathNormalizer {
    Arc::new(f)
}

/// Composes normalizers left to right: each rule sees the previous
/// rule's output. An empty chain is identity.
///
/// ```
/// use hop_top_xrr::adapters::fs::{chain, normalizer, FsAdapter};
///
/// let tmp = normalizer(|p: &str| p.replacen("/tmp/run-123", "$TMP", 1));
/// let sep = normalizer(|p: &str| p.replace('\\', "/"));
/// let adapter = FsAdapter::new().with_normalizer(chain([tmp, sep]));
/// assert_eq!(adapter.normalize("/tmp/run-123/a\\b"), "$TMP/a/b");
/// ```
pub fn chain(
    norms: impl IntoIterator<Item = PathNormalizer>,
) -> impl Fn(&str) -> String + Send + Sync + 'static {
    let norms: Vec<PathNormalizer> = norms.into_iter().collect();
    move |p: &str| norms.iter().fold(p.to_string(), |acc, n| n(&acc))
}

/// Adapter for fs mutations. Holds an optional [`PathNormalizer`] that
/// rewrites `path` and `dest` before they are hashed.
///
/// The [`Adapter`] trait has no serialize hook — cassette payloads are
/// plain serde output of the request handed to [`crate::Session::record`].
/// To honor the spec's "cassettes store post-normalizer paths" contract,
/// WRAPPERS must persist a request whose `path`/`dest` already carry
/// the normalized form: build it via [`FsAdapter::normalize_request`]
/// (or [`FsAdapter::normalize`] per field) and pass THAT request to the
/// session. What gets hashed and what gets stored then agree exactly,
/// which is what cross-runtime replay relies on.
///
/// Scope is per instance, never global: each test owns its normalizer.
///
/// ```
/// use hop_top_xrr::adapters::fs::{op, FsAdapter, FsRequest};
/// use hop_top_xrr::Adapter;
///
/// let tmp = "/var/folders/abc/T/Test123".to_string();
/// let adapter = FsAdapter::new()
///     .with_normalizer(move |p: &str| p.replacen(&tmp, "$TMP", 1));
/// let raw = FsRequest {
///     op: op::WRITE.into(),
///     path: "/var/folders/abc/T/Test123/greeting.txt".into(),
///     data: "hello, world\n".into(),
///     mode: Some(420),
///     ..Default::default()
/// };
/// // Persist the normalized copy; hash either — they agree.
/// let stored = adapter.normalize_request(&raw);
/// assert_eq!(stored.path, "$TMP/greeting.txt");
/// assert_eq!(adapter.fingerprint(&raw).unwrap(), "667a7680");
/// assert_eq!(adapter.fingerprint(&stored).unwrap(), "667a7680");
/// ```
#[derive(Clone, Default)]
pub struct FsAdapter {
    normalizer: Option<PathNormalizer>,
}

impl fmt::Debug for FsAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsAdapter")
            .field("normalizer", &self.normalizer.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl FsAdapter {
    /// An fs adapter with identity path normalization.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `self` with `f` installed as the path normalizer,
    /// replacing any previous one. Use [`chain`] to compose rules.
    pub fn with_normalizer(mut self, f: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        self.normalizer = Some(Arc::new(f));
        self
    }

    /// Applies the installed normalizer to `p`. Wrappers call this
    /// when building `path` / `dest` so the values stored on the
    /// cassette envelope agree with what the fingerprint hashes.
    ///
    /// Empty input short-circuits and returns `""` without invoking the
    /// normalizer — the optional `dest` can be passed through
    /// unconditionally.
    pub fn normalize(&self, p: &str) -> String {
        if p.is_empty() {
            return String::new();
        }
        match &self.normalizer {
            Some(n) => n(p),
            None => p.to_string(),
        }
    }

    /// Returns a copy of `req` with `path` and `dest` normalized and
    /// every other field untouched (`data` is never rewritten, even
    /// when it embeds a path). This is the request wrappers should
    /// hand to [`crate::Session::record`].
    pub fn normalize_request(&self, req: &FsRequest) -> FsRequest {
        FsRequest {
            path: self.normalize(&req.path),
            dest: self.normalize(&req.dest),
            ..req.clone()
        }
    }
}

impl Adapter for FsAdapter {
    type Req = FsRequest;
    type Resp = FsResponse;

    fn id(&self) -> &str {
        "fs"
    }

    /// Returns sha256(canonical JSON of selected fields) — first 4
    /// bytes encoded as 8 hex chars.
    ///
    /// Field selection (must match Go fs adapter exactly):
    ///   - `op` and `path` always included; `path` is path-normalized.
    ///   - `data` hashed (full sha256 hex) and included as `data_sha256`
    ///     when non-empty. Raw bytes never enter the fingerprint.
    ///   - `mode`/`uid`/`gid`/`size` included iff `Some`.
    ///   - `dest` included iff non-empty after path normalization.
    ///   - `flags` included iff non-zero.
    ///   - `recursive` included iff true.
    ///
    /// `BTreeMap` + `serde_json::to_string` produces lexicographically-
    /// sorted keys, matching Go's `encoding/json` over `map[string]any`.
    fn fingerprint(&self, req: &Self::Req) -> Result<String, XrrError> {
        let mut fields: BTreeMap<&str, Value> = BTreeMap::new();
        fields.insert("op", Value::String(req.op.clone()));
        fields.insert("path", Value::String(self.normalize(&req.path)));
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
        // spec: dest participates only when non-empty AFTER normalization.
        let dest = self.normalize(&req.dest);
        if !dest.is_empty() {
            fields.insert("dest", Value::String(dest));
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
        assert_eq!(FsAdapter::new().id(), "fs");
    }

    #[test]
    fn fingerprint_deterministic() {
        let a = FsAdapter::new();
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
        let fp = FsAdapter::new().fingerprint(&write_req()).unwrap();
        assert_eq!(fp, "667a7680", "spec conformance fingerprint mismatch");
    }

    #[test]
    fn fingerprint_discriminates_op() {
        let a = FsAdapter::new();
        let mut r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        r.op = op::MKDIR.into();
        let fp2 = a.fingerprint(&r).unwrap();
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn fingerprint_discriminates_path() {
        let a = FsAdapter::new();
        let mut r = write_req();
        let fp1 = a.fingerprint(&r).unwrap();
        r.path = "$TMP/other.txt".into();
        let fp2 = a.fingerprint(&r).unwrap();
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn fingerprint_discriminates_data() {
        let a = FsAdapter::new();
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
        let a = FsAdapter::new();
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
        let a = FsAdapter::new();
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
        let a = FsAdapter::new();
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
