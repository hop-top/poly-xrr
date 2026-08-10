use std::path::PathBuf;

use chrono::Utc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::XrrError;
use crate::redact::{Redactor, ENVELOPE_META_KEYS};

#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    xrr: String,
    adapter: String,
    fingerprint: String,
    recorded_at: String,
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
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.write(adapter_id, fingerprint, "req", &now, req)?;
        self.write(adapter_id, fingerprint, "resp", &now, resp)?;
        Ok(())
    }

    fn write<T: Serialize>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
        kind: &str,
        recorded_at: &str,
        payload: &T,
    ) -> Result<(), XrrError> {
        let env = Envelope {
            xrr: "1".into(),
            adapter: adapter_id.into(),
            fingerprint: fingerprint.into(),
            recorded_at: recorded_at.into(),
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
        let req = self.read::<Req>(adapter_id, fingerprint, "req")?;
        let resp = self.read::<Resp>(adapter_id, fingerprint, "resp")?;
        Ok((req, resp))
    }

    fn read<T: DeserializeOwned>(
        &self,
        adapter_id: &str,
        fingerprint: &str,
        kind: &str,
    ) -> Result<T, XrrError> {
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

        // Deserialize into raw value map, then extract payload.
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
        let result: T = serde_yaml::from_value(payload)?;
        Ok(result)
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
    fn miss_returns_cassette_miss_error() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        let result: Result<(ExecRequest, ExecResponse), _> =
            cassette.load("exec", "deadbeef");
        assert!(matches!(result, Err(XrrError::CassetteMiss { .. })));
    }
}
