use crate::{
    cassette::FileCassette,
    error::XrrError,
    stream::StreamCounters,
    stream_scrub::{scrub_frame, StreamDirection, StreamScrub, StreamScrubInfo},
    Adapter,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Record,
    Replay,
    Passthrough,
}

pub struct Session {
    mode: Mode,
    cassette: FileCassette,
    stream_counters: StreamCounters,
    stream_scrub: Option<StreamScrub>,
}

impl Session {
    pub fn new(mode: Mode, cassette: FileCassette) -> Self {
        Self {
            mode,
            cassette,
            stream_counters: StreamCounters::new(),
            stream_scrub: None,
        }
    }

    /// A session whose streamed interactions pass every frame through
    /// `scrub`. Identical to [`Session::new`] when the hook is absent:
    /// frames record and replay verbatim.
    ///
    /// Install the SAME hook when recording and when replaying: scrubbing
    /// is symmetric by design (see [`crate::stream_scrub::StreamScrub`]),
    /// and a session replaying a scrubbed cassette without the hook fails
    /// with a stream mismatch.
    pub fn with_stream_scrub(mode: Mode, cassette: FileCassette, scrub: StreamScrub) -> Self {
        Self {
            mode,
            cassette,
            stream_counters: StreamCounters::new(),
            stream_scrub: Some(scrub),
        }
    }

    pub(crate) fn stream_scrub(&self) -> Option<&StreamScrub> {
        self.stream_scrub.as_ref()
    }

    /// Apply the session's frame scrub hook to `data`, returning it
    /// unchanged when no hook is installed.
    ///
    /// Adapters whose open identity derives from message bytes (the gRPC
    /// server-stream `msg_hash`) MUST compute the derived identity over
    /// this function's output, in record and replay mode alike, so both
    /// modes address the cassette by the scrubbed content. Frames handed to
    /// the core (`record_send`/`record_recv`, replay `send`) are scrubbed
    /// by the core itself — adapters pass them raw and never double-scrub.
    pub fn scrub_stream_frame(
        &self,
        dir: StreamDirection,
        info: &StreamScrubInfo,
        data: &[u8],
    ) -> Vec<u8> {
        scrub_frame(self.stream_scrub.as_ref(), dir, info, data)
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub(crate) fn cassette(&self) -> &FileCassette {
        &self.cassette
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
