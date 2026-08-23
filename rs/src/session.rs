use crate::{
    cassette::FileCassette, error::XrrError, stream::StreamCounters, Adapter,
};

pub enum Mode {
    Record,
    Replay,
    Passthrough,
}

pub struct Session {
    mode: Mode,
    cassette: FileCassette,
    stream_counters: StreamCounters,
}

impl Session {
    pub fn new(mode: Mode, cassette: FileCassette) -> Self {
        Self {
            mode,
            cassette,
            stream_counters: StreamCounters::new(),
        }
    }

    /// Occurrence counters for streamed fingerprints: one session object
    /// is one counter domain, counted identically in record and replay.
    pub fn stream_counters(&self) -> &StreamCounters {
        &self.stream_counters
    }

    pub fn record<A: Adapter>(
        &self,
        adapter: &A,
        req: &A::Req,
        do_: impl FnOnce() -> Result<A::Resp, XrrError>,
    ) -> Result<A::Resp, XrrError> {
        match self.mode {
            Mode::Record => {
                let resp = do_()?;
                let fp = adapter.fingerprint(req)?;
                self.cassette.save(adapter.id(), &fp, req, &resp)?;
                Ok(resp)
            }
            Mode::Replay => {
                let fp = adapter.fingerprint(req)?;
                let (_req, resp): (A::Req, A::Resp) =
                    self.cassette.load(adapter.id(), &fp)?;
                Ok(resp)
            }
            Mode::Passthrough => do_(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::exec::{ExecAdapter, ExecRequest, ExecResponse};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn req() -> ExecRequest {
        ExecRequest {
            argv: vec!["echo".into(), "hello".into()],
            stdin: "".into(),
            env: HashMap::new(),
        }
    }

    fn resp() -> ExecResponse {
        ExecResponse {
            stdout: "hello\n".into(),
            stderr: "".into(),
            exit_code: 0,
            duration_ms: 1,
        }
    }

    #[test]
    fn record_saves_and_returns() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        let session = Session::new(Mode::Record, cassette);
        let adapter = ExecAdapter;
        let r = req();

        let result = session.record(&adapter, &r, || Ok(resp())).unwrap();
        assert_eq!(result.stdout, "hello\n");

        // Verify file was written.
        let fp = adapter.fingerprint(&r).unwrap();
        let path = tmp
            .path()
            .join(format!("exec-{}.req.yaml", fp));
        assert!(path.exists());
    }

    #[test]
    fn replay_loads_without_calling_do() {
        let tmp = TempDir::new().unwrap();
        let adapter = ExecAdapter;
        let r = req();
        let fp = adapter.fingerprint(&r).unwrap();

        // Pre-save cassette files.
        let cassette = FileCassette::new(tmp.path());
        cassette.save("exec", &fp, &r, &resp()).unwrap();

        let cassette2 = FileCassette::new(tmp.path());
        let session = Session::new(Mode::Replay, cassette2);

        let mut called = false;
        let result = session
            .record(&adapter, &r, || {
                called = true;
                Ok(resp())
            })
            .unwrap();

        assert!(!called, "do_ should not be called in replay mode");
        assert_eq!(result.stdout, "hello\n");
    }

    #[test]
    fn replay_miss_returns_cassette_miss() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        let session = Session::new(Mode::Replay, cassette);
        let adapter = ExecAdapter;
        let r = req();

        let result = session.record(&adapter, &r, || Ok(resp()));
        assert!(matches!(result, Err(XrrError::CassetteMiss { .. })));
    }

    #[test]
    fn passthrough_calls_do_without_saving() {
        let tmp = TempDir::new().unwrap();
        let cassette = FileCassette::new(tmp.path());
        let session = Session::new(Mode::Passthrough, cassette);
        let adapter = ExecAdapter;
        let r = req();

        let mut called = false;
        let result = session.record(&adapter, &r, || {
            called = true;
            Ok(resp())
        }).unwrap();

        assert!(called);
        assert_eq!(result.exit_code, 0);

        // No files should exist.
        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .collect();
        assert!(entries.is_empty());
    }
}
