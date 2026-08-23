use std::path::PathBuf;

use chrono::Utc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::XrrError;
use crate::redact::{Redactor, ENVELOPE_META_KEYS};
use crate::stream::StreamedPair;

#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    xrr: String,
    adapter: String,
    fingerprint: String,
    recorded_at: String,
    /// v1 optional resp-only field: recorded error message from the
    /// original interaction. Non-empty ⇒ replay re-emits an error.
    /// `.req.yaml` MUST NOT carry it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    payload: T,
}

pub struct FileCassette {
    dir: PathBuf,
    // None means "resolve from the environment at write time", which is
    // what `new` installs so redaction is on by default.
    redactor: Option<Redactor>,
}

impl FileCassette {
    /// Creates a FileCassette that reads/writes to dir.
    ///
    /// Redaction is enabled by default, configured from the XRR_REDACT_*
    /// environment variables. Use [`FileCassette::with_redactor`] to
    /// supply an explicit policy.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            redactor: None,
        }
    }

    /// Creates a FileCassette with an explicit redaction policy,
    /// bypassing the XRR_REDACT_* environment variables.
    pub fn with_redactor(dir: impl Into<PathBuf>, redactor: Redactor) -> Self {
        Self {
            dir: dir.into(),
            redactor: Some(redactor),
        }
    }

    // Resolves the redactor to use for one write. When none was
    // injected, config is read from the environment on each write so a
    // test that flips XRR_REDACT_* mid-process sees the change.
    fn active_redactor(&self) -> Redactor {
        self.redactor.clone().unwrap_or_else(Redactor::from_env)
    }

    pub fn save<Req: Serialize, Resp: Serialize>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
        req: &Req,
        resp: &Resp,
    ) -> Result<(), XrrError> {
        self.save_with_error(adapter_id, fingerprint, req, resp, None)
    }

    /// Save a pair whose original interaction ended in an error: the
    /// message lands in the v1 resp envelope `error` field.
    pub fn save_with_error<Req: Serialize, Resp: Serialize>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
        req: &Req,
        resp: &Resp,
        error: Option<&str>,
    ) -> Result<(), XrrError> {
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.write(adapter_id, fingerprint, "req", &now, req, None)?;
        self.write(adapter_id, fingerprint, "resp", &now, resp, error)?;
        Ok(())
    }

    fn write<T: Serialize>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
        kind: &str,
        recorded_at: &str,
        payload: &T,
        error: Option<&str>,
    ) -> Result<(), XrrError> {
        let env = Envelope {
            xrr: "1".into(),
            adapter: adapter_id.into(),
            fingerprint: fingerprint.into(),
            recorded_at: recorded_at.into(),
            error: error.map(Into::into),
            payload,
        };
        // Encode to a value tree first so redaction can walk the generic
        // structure without knowing any adapter's concrete types. The
        // scrubbed tree is what gets serialized — a secret never reaches
        // the YAML string, let alone the file. Envelope metadata is never
        // scrubbed: the fingerprint in particular must match the filename.
        let mut tree = serde_yaml::to_value(&env)?;
        let redactor = self.active_redactor();
        if let serde_yaml::Value::Mapping(map) = &mut tree {
            for (k, v) in map.iter_mut() {
                match k.as_str() {
                    Some(key) if !ENVELOPE_META_KEYS.contains(&key) => {
                        redactor.redact_node(v, key);
                    }
                    _ => {}
                }
            }
        }
        let data = serde_yaml::to_string(&tree)?;
        let path = self
            .dir
            .join(format!("{}-{}.{}.yaml", adapter_id, fingerprint, kind));
        std::fs::write(path, data)?;
        Ok(())
    }

    pub fn load<Req: DeserializeOwned, Resp: DeserializeOwned>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
    ) -> Result<(Req, Resp), XrrError> {
        let (req, resp, _error) = self.load_with_error(adapter_id, fingerprint)?;
        Ok((req, resp))
    }

    /// Load a pair plus the v1 resp envelope `error` field. Non-empty ⇒
    /// the recorded interaction failed and replay must surface the error
    /// alongside the response payload.
    pub fn load_with_error<Req: DeserializeOwned, Resp: DeserializeOwned>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
    ) -> Result<(Req, Resp, Option<String>), XrrError> {
        let (req, _) = self.read::<Req>(adapter_id, fingerprint, "req")?;
        let (resp, error) = self.read::<Resp>(adapter_id, fingerprint, "resp")?;
        Ok((req, resp, error))
    }

    /// Load a streamed pair (v1 `stream` envelope extension), parsed and
    /// validated. Missing file ⇒ cassette miss; unary pair ⇒ shape
    /// mismatch.
    pub fn load_stream(
        &self,
        adapter_id: &str,
        fingerprint: &str,
    ) -> Result<StreamedPair, XrrError> {
        StreamedPair::load(&self.dir, adapter_id, fingerprint)
    }

    /// Write a streamed pair via the format-layer emitter (v1 naming,
    /// last-write-wins).
    pub fn save_stream(&self, pair: &StreamedPair) -> Result<(), XrrError> {
        pair.save(&self.dir)
    }

    fn read<T: DeserializeOwned>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
        kind: &str,
    ) -> Result<(T, Option<String>), XrrError> {
        let path = self
            .dir
            .join(format!("{}-{}.{}.yaml", adapter_id, fingerprint, kind));
        let data = std::fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                XrrError::CassetteMiss {
                    adapter: adapter_id.into(),
                    fingerprint: fingerprint.into(),
                }
            } else {
                XrrError::Io(e)
            }
        })?;

        // Deserialize into raw value map, then extract payload + error.
        let raw: serde_yaml::Value = serde_yaml::from_str(&data)?;
        let payload = raw
            .get("payload")
            .ok_or_else(|| {
                XrrError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("missing payload in {}", kind),
                ))
            })?
            .clone();
        let error = raw
            .get("error")
            .and_then(|v| v.as_str())
            .map(String::from);
        let result: T = serde_yaml::from_value(payload)?;
        Ok((result, error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::exec::{ExecRequest, ExecResponse};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn make_req() -> ExecRequest {
        ExecRequest {
            argv: vec!["gh".into(), "pr".into(), "view".into()],
            stdin: "".into(),
            env: HashMap::new(),
        }
    }

    fn make_resp() -> ExecResponse {
        ExecResponse {
            stdout: "ok\n".into(),
            stderr: "".into(),
            exit_code: 0,
            duration_ms: 10,
        }
    }

    #[test]
    fn roundtrip() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        let req = make_req();
        let resp = make_resp();

        cassette.save("exec", "abcd1234", &req, &resp).unwrap();
        let (loaded_req, loaded_resp): (ExecRequest, ExecResponse) =
            cassette.load("exec", "abcd1234").unwrap();

        assert_eq!(loaded_req.argv, req.argv);
        assert_eq!(loaded_resp.stdout, resp.stdout);
        assert_eq!(loaded_resp.exit_code, 0);
    }

    #[test]
    fn error_field_roundtrips_on_resp_only() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        cassette
            .save_with_error("exec", "deadbeef", &make_req(), &make_resp(), Some("exit status 1"))
            .unwrap();

        let (_req, _resp, error): (ExecRequest, ExecResponse, _) =
            cassette.load_with_error("exec", "deadbeef").unwrap();
        assert_eq!(error.as_deref(), Some("exit status 1"));

        // The req file MUST NOT carry the error field.
        let req_raw = std::fs::read_to_string(
            tmp.path().join("exec-deadbeef.req.yaml"),
        )
        .unwrap();
        assert!(!req_raw.contains("error:"), "req must not carry error:\n{req_raw}");
        let resp_raw = std::fs::read_to_string(
            tmp.path().join("exec-deadbeef.resp.yaml"),
        )
        .unwrap();
        assert!(resp_raw.contains("error: exit status 1"), "resp carries error:\n{resp_raw}");
    }

    #[test]
    fn absent_error_field_loads_as_none() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        cassette.save("exec", "abcd1234", &make_req(), &make_resp()).unwrap();
        let (_req, _resp, error): (ExecRequest, ExecResponse, _) =
            cassette.load_with_error("exec", "abcd1234").unwrap();
        assert_eq!(error, None);
    }

    #[test]
    fn miss_returns_cassette_miss_error() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        let result: Result<(ExecRequest, ExecResponse), _> =
            cassette.load("exec", "deadbeef");
        assert!(matches!(result, Err(XrrError::CassetteMiss { .. })));
    }
}
