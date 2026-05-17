// E2E adapter tests: exec, http, redis, sql.
// Each test: Record → Replay → assert match; plus CassetteMiss on unknown request.
//
// Stories: US-0101, US-0102, US-0104, US-0105

use std::collections::HashMap;

use tempfile::TempDir;
use hop_top_xrr::{
    Adapter, FileCassette, Mode, Session, XrrError,
    adapters::{
        exec::{ExecAdapter, ExecRequest, ExecResponse},
        http::{HttpAdapter, HttpRequest, HttpResponse},
        redis::{RedisAdapter, RedisRequest, RedisResponse},
        sql::{SqlAdapter, SqlRequest, SqlResponse},
    },
};

// ── helpers ──────────────────────────────────────────────────────────────────

fn session(dir: &std::path::Path, mode: Mode) -> Session {
    Session::new(mode, FileCassette::new(dir))
}

// ── exec ─────────────────────────────────────────────────────────────────────

// US-0101, US-0104
#[test]
fn exec_record_replay() {
    let tmp = TempDir::new().unwrap();

    let req = ExecRequest {
        argv: vec!["echo".into(), "xrr".into()],
        stdin: "".into(),
        env: HashMap::new(),
    };
    let recorded_resp = ExecResponse {
        stdout: "xrr\n".into(),
        stderr: "".into(),
        exit_code: 0,
        duration_ms: 5,
    };

    // Record
    let rec_resp = session(tmp.path(), Mode::Record)
        .record(&ExecAdapter, &req, || Ok(recorded_resp.clone()))
        .unwrap();
    assert_eq!(rec_resp.stdout, "xrr\n");
    assert_eq!(rec_resp.exit_code, 0);

    // Replay — do_ must NOT be called; response must match recorded value
    let mut do_called = false;
    let rep_resp = session(tmp.path(), Mode::Replay)
        .record(&ExecAdapter, &req, || {
            do_called = true;
            Ok(ExecResponse {
                stdout: "WRONG".into(),
                stderr: "".into(),
                exit_code: 1,
                duration_ms: 0,
            })
        })
        .unwrap();

    assert!(!do_called, "do_ must not run during replay");
    assert_eq!(rep_resp.stdout, "xrr\n");
    assert_eq!(rep_resp.exit_code, 0);
}

// US-0101 US-0102 — real subprocess round-trip
#[test]
fn exec_real_subprocess_round_trip() {
    let tmp = TempDir::new().unwrap();
    let adapter = ExecAdapter;

    let req = ExecRequest {
        argv: vec!["echo".into(), "hello".into()],
        stdin: "".into(),
        env: HashMap::new(),
    };

    // Run the real process during record.
    let recorded = session(tmp.path(), Mode::Record)
        .record(&adapter, &req, || {
            let out = std::process::Command::new(&req.argv[0])
                .args(&req.argv[1..])
                .output()
                .expect("echo must exist");
            Ok(ExecResponse {
                stdout: String::from_utf8_lossy(&out.stdout).into(),
                stderr: String::from_utf8_lossy(&out.stderr).into(),
                exit_code: out.status.code().unwrap_or(-1),
                duration_ms: 0,
            })
        })
        .unwrap();

    assert_eq!(recorded.stdout, "hello\n");
    assert_eq!(recorded.exit_code, 0);

    // Replay must return same stdout without running the process again.
    let mut do_called = false;
    let replayed = session(tmp.path(), Mode::Replay)
        .record(&adapter, &req, || {
            do_called = true;
            Ok(ExecResponse {
                stdout: "WRONG".into(),
                stderr: "".into(),
                exit_code: 1,
                duration_ms: 0,
            })
        })
        .unwrap();

    assert!(!do_called, "do_ must not run during replay");
    assert_eq!(replayed.stdout, "hello\n");
    assert_eq!(replayed.exit_code, 0);
}

// US-0105
#[test]
fn exec_replay_miss_returns_cassette_miss() {
    let tmp = TempDir::new().unwrap();

    let req = ExecRequest {
        argv: vec!["unknown-command".into()],
        stdin: "".into(),
        env: HashMap::new(),
    };

    let result = session(tmp.path(), Mode::Replay).record(
        &ExecAdapter,
        &req,
        || Ok(ExecResponse { stdout: "".into(), stderr: "".into(), exit_code: 0, duration_ms: 0 }),
    );

    assert!(
        matches!(result, Err(XrrError::CassetteMiss { .. })),
        "expected CassetteMiss, got {:?}",
        result
    );
}

// ── http ─────────────────────────────────────────────────────────────────────

// US-0101, US-0104
#[test]
fn http_record_replay() {
    let tmp = TempDir::new().unwrap();

    let req = HttpRequest {
        method: "GET".into(),
        url: "https://api.example.com/v1/ping".into(),
        headers: HashMap::new(),
        body: vec![],
    };
    let recorded_resp = HttpResponse {
        status: 200,
        headers: HashMap::new(),
        body: b"pong".to_vec(),
    };

    // Record
    let rec_resp = session(tmp.path(), Mode::Record)
        .record(&HttpAdapter, &req, || Ok(recorded_resp.clone()))
        .unwrap();
    assert_eq!(rec_resp.status, 200);
    assert_eq!(rec_resp.body, b"pong");

    // Replay
    let mut do_called = false;
    let rep_resp = session(tmp.path(), Mode::Replay)
        .record(&HttpAdapter, &req, || {
            do_called = true;
            Ok(HttpResponse { status: 500, headers: HashMap::new(), body: vec![] })
        })
        .unwrap();

    assert!(!do_called, "do_ must not run during replay");
    assert_eq!(rep_resp.status, 200);
    assert_eq!(rep_resp.body, b"pong");
}

// US-0104 — GET vs POST to same URL must produce different fingerprints
#[test]
fn http_different_methods_different_fingerprints() {
    let adapter = HttpAdapter;

    let get_req = HttpRequest {
        method: "GET".into(),
        url: "https://api.example.com/users".into(),
        headers: HashMap::new(),
        body: vec![],
    };
    let post_req = HttpRequest {
        method: "POST".into(),
        url: "https://api.example.com/users".into(),
        headers: HashMap::new(),
        body: b"{\"name\":\"alice\"}".to_vec(),
    };

    let fp_get = adapter.fingerprint(&get_req).unwrap();
    let fp_post = adapter.fingerprint(&post_req).unwrap();
    assert_ne!(fp_get, fp_post, "GET and POST to same URL must not collide");
}

// US-0105
#[test]
fn http_replay_miss_returns_cassette_miss() {
    let tmp = TempDir::new().unwrap();

    let req = HttpRequest {
        method: "POST".into(),
        url: "https://api.example.com/v1/never-recorded".into(),
        headers: HashMap::new(),
        body: b"payload".to_vec(),
    };

    let result = session(tmp.path(), Mode::Replay).record(
        &HttpAdapter,
        &req,
        || Ok(HttpResponse { status: 200, headers: HashMap::new(), body: vec![] }),
    );

    assert!(
        matches!(result, Err(XrrError::CassetteMiss { .. })),
        "expected CassetteMiss, got {:?}",
        result
    );
}

// ── redis ────────────────────────────────────────────────────────────────────

// US-0101, US-0104
#[test]
fn redis_record_replay() {
    let tmp = TempDir::new().unwrap();

    let req = RedisRequest {
        command: "GET".into(),
        args: vec!["session:42".into()],
    };
    let recorded_resp = RedisResponse {
        result: serde_json::json!("token-abc"),
    };

    // Record
    let rec_resp = session(tmp.path(), Mode::Record)
        .record(&RedisAdapter, &req, || Ok(recorded_resp.clone()))
        .unwrap();
    assert_eq!(rec_resp.result, serde_json::json!("token-abc"));

    // Replay
    let mut do_called = false;
    let rep_resp = session(tmp.path(), Mode::Replay)
        .record(&RedisAdapter, &req, || {
            do_called = true;
            Ok(RedisResponse { result: serde_json::json!(null) })
        })
        .unwrap();

    assert!(!do_called, "do_ must not run during replay");
    assert_eq!(rep_resp.result, serde_json::json!("token-abc"));
}

// US-0102 — result can be a list (LRANGE-style)
#[test]
fn redis_replay_list_result() {
    let tmp = TempDir::new().unwrap();
    let adapter = RedisAdapter;

    let req = RedisRequest {
        command: "LRANGE".into(),
        args: vec!["mylist".into(), "0".into(), "-1".into()],
    };
    let original = RedisResponse {
        result: serde_json::json!(["a", "b", "c"]),
    };

    session(tmp.path(), Mode::Record)
        .record(&adapter, &req, || Ok(original.clone()))
        .unwrap();

    let replayed = session(tmp.path(), Mode::Replay)
        .record(&adapter, &req, || Ok(RedisResponse { result: serde_json::json!(null) }))
        .unwrap();

    assert_eq!(replayed.result, serde_json::json!(["a", "b", "c"]));
}

// US-0105
#[test]
fn redis_replay_miss_returns_cassette_miss() {
    let tmp = TempDir::new().unwrap();

    let req = RedisRequest {
        command: "SET".into(),
        args: vec!["nope".into(), "val".into()],
    };

    let result = session(tmp.path(), Mode::Replay).record(
        &RedisAdapter,
        &req,
        || Ok(RedisResponse { result: serde_json::json!(null) }),
    );

    assert!(
        matches!(result, Err(XrrError::CassetteMiss { .. })),
        "expected CassetteMiss, got {:?}",
        result
    );
}

// ── sql ──────────────────────────────────────────────────────────────────────

// US-0101, US-0104
#[test]
fn sql_record_replay() {
    let tmp = TempDir::new().unwrap();

    let req = SqlRequest {
        query: "SELECT id, name FROM users WHERE id = $1".into(),
        args: vec![serde_json::json!(7)],
    };
    let recorded_resp = SqlResponse {
        rows: vec![serde_json::json!({"id": 7, "name": "alice"})],
        rows_affected: 1,
    };

    // Record
    let rec_resp = session(tmp.path(), Mode::Record)
        .record(&SqlAdapter, &req, || Ok(recorded_resp.clone()))
        .unwrap();
    assert_eq!(rec_resp.rows_affected, 1);
    assert_eq!(rec_resp.rows[0]["name"], "alice");

    // Replay
    let mut do_called = false;
    let rep_resp = session(tmp.path(), Mode::Replay)
        .record(&SqlAdapter, &req, || {
            do_called = true;
            Ok(SqlResponse { rows: vec![], rows_affected: 0 })
        })
        .unwrap();

    assert!(!do_called, "do_ must not run during replay");
    assert_eq!(rep_resp.rows_affected, 1);
    assert_eq!(rep_resp.rows[0]["name"], "alice");
}

// US-0104 — whitespace/case-equivalent queries must share the same fingerprint
#[test]
fn sql_query_normalization_same_fingerprint() {
    let adapter = SqlAdapter;

    let req1 = SqlRequest { query: "SELECT  *  FROM  t".into(), args: vec![] };
    let req2 = SqlRequest { query: "select * from t".into(), args: vec![] };

    assert_eq!(
        adapter.fingerprint(&req1).unwrap(),
        adapter.fingerprint(&req2).unwrap(),
        "whitespace/case variants must produce the same fingerprint"
    );
}

// US-0102 — multi-row result round-trips intact
#[test]
fn sql_replay_multiple_rows() {
    let tmp = TempDir::new().unwrap();
    let adapter = SqlAdapter;

    let req = SqlRequest {
        query: "SELECT id, name FROM users".into(),
        args: vec![],
    };
    let rows = vec![
        serde_json::json!({"id": 1, "name": "Alice"}),
        serde_json::json!({"id": 2, "name": "Bob"}),
    ];
    let original = SqlResponse { rows: rows.clone(), rows_affected: 0 };

    session(tmp.path(), Mode::Record)
        .record(&adapter, &req, || Ok(original.clone()))
        .unwrap();

    let replayed = session(tmp.path(), Mode::Replay)
        .record(&adapter, &req, || Ok(SqlResponse { rows: vec![], rows_affected: 0 }))
        .unwrap();

    assert_eq!(replayed.rows, rows);
    assert_eq!(replayed.rows_affected, 0);
}

// US-0105
#[test]
fn sql_replay_miss_returns_cassette_miss() {
    let tmp = TempDir::new().unwrap();

    let req = SqlRequest {
        query: "DELETE FROM ghosts".into(),
        args: vec![],
    };

    let result = session(tmp.path(), Mode::Replay).record(
        &SqlAdapter,
        &req,
        || Ok(SqlResponse { rows: vec![], rows_affected: 0 }),
    );

    assert!(
        matches!(result, Err(XrrError::CassetteMiss { .. })),
        "expected CassetteMiss, got {:?}",
        result
    );
}
