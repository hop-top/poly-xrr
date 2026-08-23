// Record-side redaction integration tests — mirror the Go coverage:
// secrets never hit disk, env overrides, deterministic placeholders,
// fingerprint-stable filenames, redacted cassettes still replay.
//
// Tests that read or mutate XRR_REDACT_* env vars share a lock so
// parallel test threads cannot observe each other's environment.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock};

use hop_top_xrr::adapters::exec::{ExecAdapter, ExecRequest, ExecResponse};
use hop_top_xrr::{
    Adapter, FileCassette, Mode, Redactor, Session, ENV_REDACT_ALLOW, ENV_REDACT_DISABLE,
};
use tempfile::TempDir;

const SECRET_TOKEN: &str = "ghp_supersecrettokenvalue0123456789abcd";

fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

// Restores the original value of an env var on drop, so a panicking
// test cannot leak configuration into its siblings.
struct EnvGuard {
    name: &'static str,
    original: Option<String>,
}

impl EnvGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let original = std::env::var(name).ok();
        std::env::set_var(name, value);
        Self { name, original }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(v) => std::env::set_var(self.name, v),
            None => std::env::remove_var(self.name),
        }
    }
}

fn secret_req() -> ExecRequest {
    ExecRequest {
        argv: vec!["gh".into(), "pr".into(), "view".into(), "1".into()],
        stdin: "".into(),
        env: HashMap::from([
            ("GITHUB_TOKEN".to_string(), SECRET_TOKEN.to_string()),
            ("PATH".to_string(), "/usr/local/bin:/usr/bin".to_string()),
        ]),
    }
}

fn ok_resp() -> ExecResponse {
    ExecResponse {
        stdout: "ok\n".into(),
        stderr: "".into(),
        exit_code: 0,
        duration_ms: 1,
    }
}

fn record(dir: &Path, req: &ExecRequest) {
    let session = Session::new(Mode::Record, FileCassette::new(dir));
    session
        .record(&ExecAdapter, req, || Ok(ok_resp()))
        .expect("record must succeed");
}

// Concatenates every file written under dir so a test can assert on
// "nothing anywhere in the cassette dir contains this string".
fn read_all(dir: &Path) -> String {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .expect("read cassette dir")
        .map(|e| e.expect("dir entry").path())
        .collect();
    entries.sort();
    entries
        .iter()
        .filter(|p| p.is_file())
        .map(|p| std::fs::read_to_string(p).expect("read cassette file"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn strip_recorded_at(s: &str) -> String {
    s.lines()
        .filter(|line| !line.starts_with("recorded_at:"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn secret_env_never_hits_disk() {
    let _g = env_lock();
    let tmp = TempDir::new().unwrap();
    record(tmp.path(), &secret_req());

    let on_disk = read_all(tmp.path());
    assert!(
        !on_disk.contains(SECRET_TOKEN),
        "secret env value leaked into cassette"
    );
    assert!(
        on_disk.contains("<redacted:GITHUB_TOKEN>"),
        "expected deterministic placeholder"
    );
    // Benign env survives — redaction must not nuke useful debugging context.
    assert!(on_disk.contains("/usr/local/bin:/usr/bin"));
}

#[test]
fn value_pattern_secret_never_hits_disk() {
    // The env var name gives no hint, only the value shape does.
    let _g = env_lock();
    let tmp = TempDir::new().unwrap();
    let req = ExecRequest {
        argv: vec!["deploy".into()],
        stdin: "".into(),
        env: HashMap::from([("DEPLOY_HANDLE".to_string(), SECRET_TOKEN.to_string())]),
    };
    record(tmp.path(), &req);

    let on_disk = read_all(tmp.path());
    assert!(!on_disk.contains(SECRET_TOKEN));
    assert!(on_disk.contains("<redacted:DEPLOY_HANDLE>"));
}

#[test]
fn redaction_is_fingerprint_stable() {
    // Fingerprints are computed from {argv, stdin} only, so redacting
    // env cannot shift them. If this fails, ports would disagree on
    // cassette filenames.
    let _g = env_lock();
    let names = |dir: &Path| -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };

    let (a, b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    record(a.path(), &secret_req());
    record(b.path(), &secret_req());
    assert_eq!(
        names(a.path()),
        names(b.path()),
        "re-recording must produce identical cassette filenames"
    );

    let fp_direct = ExecAdapter.fingerprint(&secret_req()).unwrap();
    for name in names(a.path()) {
        assert!(
            name.contains(&fp_direct),
            "cassette filename must carry the canonical fingerprint"
        );
    }
}

#[test]
fn byte_identical_across_runs() {
    // Placeholders must be stable, not derived from a counter or the
    // secret's hash, or committed cassettes would churn on re-record.
    let _g = env_lock();
    let req = ExecRequest {
        argv: vec!["gh".into(), "auth".into(), "status".into()],
        stdin: "".into(),
        env: HashMap::from([
            ("GITHUB_TOKEN".to_string(), SECRET_TOKEN.to_string()),
            ("AWS_SECRET_ACCESS_KEY".to_string(), "abc123".to_string()),
        ]),
    };
    let run = || -> String {
        let tmp = TempDir::new().unwrap();
        record(tmp.path(), &req);
        let fp = ExecAdapter.fingerprint(&req).unwrap();
        let data =
            std::fs::read_to_string(tmp.path().join(format!("exec-{fp}.req.yaml"))).unwrap();
        // recorded_at is a timestamp and legitimately varies; drop it.
        strip_recorded_at(&data)
    };
    assert_eq!(run(), run(), "redacted cassette bytes must be stable across runs");
}

#[test]
fn redacted_cassette_still_replays() {
    // Redaction must not break the record→replay round trip. The
    // response payload replays intact; redacted request fields are not
    // part of matching.
    let _g = env_lock();
    let tmp = TempDir::new().unwrap();
    let req = secret_req();
    record(tmp.path(), &req);

    let session = Session::new(Mode::Replay, FileCassette::new(tmp.path()));
    let got = session
        .record(&ExecAdapter, &req, || {
            panic!("do() must not be called in replay mode")
        })
        .expect("redacted cassette must replay");
    assert_eq!(got.stdout, "ok\n");
    assert_eq!(got.exit_code, 0);
}

#[test]
fn disabled_via_env_leaves_secrets() {
    let _g = env_lock();
    let _e = EnvGuard::set(ENV_REDACT_DISABLE, "1");
    let tmp = TempDir::new().unwrap();
    record(tmp.path(), &secret_req());
    assert!(
        read_all(tmp.path()).contains(SECRET_TOKEN),
        "disable flag must restore verbatim recording"
    );
}

#[test]
fn allow_list_via_env_preserves_named_field_only() {
    let _g = env_lock();
    let _e = EnvGuard::set(ENV_REDACT_ALLOW, "GITHUB_TOKEN");
    let tmp = TempDir::new().unwrap();
    let req = ExecRequest {
        argv: vec!["gh".into()],
        stdin: "".into(),
        env: HashMap::from([
            ("GITHUB_TOKEN".to_string(), SECRET_TOKEN.to_string()),
            ("AWS_SECRET_ACCESS_KEY".to_string(), "leakme".to_string()),
        ]),
    };
    record(tmp.path(), &req);

    let on_disk = read_all(tmp.path());
    assert!(
        on_disk.contains(SECRET_TOKEN),
        "allow-listed key must be preserved"
    );
    assert!(
        !on_disk.contains("leakme"),
        "non-allow-listed secret must still be redacted"
    );
}

#[test]
fn nested_structures_and_non_string_scalars_preserved() {
    let _g = env_lock();
    let tmp = TempDir::new().unwrap();
    let cassette = FileCassette::new(tmp.path());
    let req = serde_json::json!({
        "argv": ["svc"],
        "config": {
            "retries": 3,
            "verbose": true,
            "api_key": SECRET_TOKEN,
            "endpoint": "https://example.com",
            "nested_map": {"password": "hunter2"},
        },
    });
    cassette
        .save("exec", "aabbccdd", &req, &serde_json::json!({"stdout": "ok"}))
        .unwrap();

    let on_disk = read_all(tmp.path());
    assert!(!on_disk.contains(SECRET_TOKEN));
    assert!(!on_disk.contains("hunter2"));
    assert!(on_disk.contains("<redacted:API_KEY>"));
    assert!(on_disk.contains("<redacted:PASSWORD>"));
    // Non-string scalars keep their YAML type (not quoted into strings).
    assert!(on_disk.contains("retries: 3"));
    assert!(on_disk.contains("verbose: true"));
    assert!(on_disk.contains("https://example.com"));
}

#[test]
fn explicit_redactor_bypasses_env() {
    let _g = env_lock();
    let _e = EnvGuard::set(ENV_REDACT_DISABLE, "1");
    let tmp = TempDir::new().unwrap();
    let cassette = FileCassette::with_redactor(tmp.path(), Redactor::default());
    cassette
        .save(
            "exec",
            "aabbccdd",
            &serde_json::json!({"env": {"GITHUB_TOKEN": SECRET_TOKEN}}),
            &serde_json::json!({"stdout": "ok"}),
        )
        .unwrap();

    let on_disk = read_all(tmp.path());
    assert!(!on_disk.contains(SECRET_TOKEN));
    assert!(on_disk.contains("<redacted:GITHUB_TOKEN>"));
}
