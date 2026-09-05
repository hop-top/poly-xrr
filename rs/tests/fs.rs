//! fs adapter path-normalizer contract.
//!
//! Mirrors go/adapters/fs/fs_test.go: the normalizer rewrites `path`
//! and `dest` before they enter the fingerprint, wrappers apply the
//! same rewrite to the request they persist, and identity is the
//! default so the spec/fixtures/fs-write pin holds untouched.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use hop_top_xrr::{
    adapters::fs::{chain, normalizer, op, FsAdapter, FsRequest, FsResponse},
    Adapter, FileCassette, Mode, Session,
};

const SPEC_PIN: &str = "667a7680";

fn write_req(path: &str) -> FsRequest {
    FsRequest {
        op: op::WRITE.into(),
        path: path.into(),
        data: "hello, world\n".into(),
        mode: Some(420),
        ..Default::default()
    }
}

fn strip(root: &str) -> impl Fn(&str) -> String + Send + Sync + 'static {
    let root = root.to_string();
    move |p: &str| p.replacen(&root, "$TMP", 1)
}

#[test]
fn identity_by_default_keeps_spec_pin() {
    let fp_new = FsAdapter::new()
        .fingerprint(&write_req("$TMP/greeting.txt"))
        .unwrap();
    let fp_default = FsAdapter::default()
        .fingerprint(&write_req("$TMP/greeting.txt"))
        .unwrap();
    assert_eq!(fp_new, SPEC_PIN, "spec conformance fingerprint mismatch");
    assert_eq!(fp_default, SPEC_PIN, "Default must be identity");
    assert_eq!(FsAdapter::new().normalize("/var/x"), "/var/x");
}

#[test]
fn normalizer_applied_to_path_in_fingerprint() {
    let plain = FsAdapter::new();
    let norm = FsAdapter::new().with_normalizer(strip("/var/folders/abc/T/Test123"));

    let raw = write_req("/var/folders/abc/T/Test123/greeting.txt");
    let pre = write_req("$TMP/greeting.txt");

    let fp_raw_norm = norm.fingerprint(&raw).unwrap();
    let fp_pre_norm = norm.fingerprint(&pre).unwrap();
    assert_eq!(
        fp_raw_norm, fp_pre_norm,
        "raw and pre-normalized must agree"
    );
    assert_eq!(
        fp_raw_norm, SPEC_PIN,
        "normalized raw path must hit the spec pin"
    );

    let fp_raw_plain = plain.fingerprint(&raw).unwrap();
    assert_ne!(
        fp_raw_plain, fp_raw_norm,
        "plain adapter must see the raw path"
    );
}

#[test]
fn normalizer_applied_to_dest() {
    let norm = FsAdapter::new().with_normalizer(strip("/tmp"));
    let plain = FsAdapter::new();
    for o in [op::RENAME, op::SYMLINK, op::HARDLINK] {
        let raw = FsRequest {
            op: o.into(),
            path: "/tmp/a".into(),
            dest: "/tmp/b".into(),
            ..Default::default()
        };
        let pre = FsRequest {
            op: o.into(),
            path: "$TMP/a".into(),
            dest: "$TMP/b".into(),
            ..Default::default()
        };
        assert_eq!(
            norm.fingerprint(&raw).unwrap(),
            norm.fingerprint(&pre).unwrap(),
            "{o}: dest must be normalized"
        );
        // Path-only normalization would leave dest raw and disagree.
        let half = FsRequest {
            op: o.into(),
            path: "$TMP/a".into(),
            dest: "/tmp/b".into(),
            ..Default::default()
        };
        assert_ne!(
            plain.fingerprint(&half).unwrap(),
            norm.fingerprint(&raw).unwrap(),
            "{o}: dest left raw must not match"
        );
    }
}

#[test]
fn two_tempdirs_fingerprint_identically_when_root_stripped() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let root_a = a.path().to_string_lossy().into_owned();
    let root_b = b.path().to_string_lossy().into_owned();
    assert_ne!(root_a, root_b);

    let file_a = format!("{root_a}/greeting.txt");
    let file_b = format!("{root_b}/greeting.txt");

    let norm_a = FsAdapter::new().with_normalizer(strip(&root_a));
    let norm_b = FsAdapter::new().with_normalizer(strip(&root_b));
    let fp_a = norm_a.fingerprint(&write_req(&file_a)).unwrap();
    let fp_b = norm_b.fingerprint(&write_req(&file_b)).unwrap();
    assert_eq!(fp_a, fp_b);
    assert_eq!(fp_a, SPEC_PIN);

    let plain = FsAdapter::new();
    assert_ne!(
        plain.fingerprint(&write_req(&file_a)).unwrap(),
        plain.fingerprint(&write_req(&file_b)).unwrap(),
        "without a normalizer the tmpdir leaks into the fingerprint"
    );
}

#[test]
fn normalize_request_rewrites_path_and_dest_only() {
    let norm = FsAdapter::new().with_normalizer(strip("/tmp/run-123"));
    let raw = FsRequest {
        op: op::RENAME.into(),
        path: "/tmp/run-123/old".into(),
        data: "/tmp/run-123/old".into(),
        dest: "/tmp/run-123/new".into(),
        mode: Some(0o600),
        ..Default::default()
    };
    let n = norm.normalize_request(&raw);
    assert_eq!(n.path, "$TMP/old");
    assert_eq!(n.dest, "$TMP/new");
    assert_eq!(n.data, raw.data, "data is never normalized");
    assert_eq!(n.mode, raw.mode);
    assert_eq!(n.op, raw.op);
    // Hashing the normalized copy under identity equals hashing the raw
    // request under the normalizing adapter: what is stored == what is hashed.
    assert_eq!(
        FsAdapter::new().fingerprint(&n).unwrap(),
        norm.fingerprint(&raw).unwrap()
    );
}

#[test]
fn normalized_request_persists_post_normalizer_paths() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_string_lossy().into_owned();
    let norm = FsAdapter::new().with_normalizer(strip(&root));

    let raw = FsRequest {
        op: op::RENAME.into(),
        path: format!("{root}/old"),
        dest: format!("{root}/new"),
        ..Default::default()
    };
    let req = norm.normalize_request(&raw);

    let yaml = serde_yaml::to_string(&req).unwrap();
    assert!(yaml.contains("path: $TMP/old"), "{yaml}");
    assert!(yaml.contains("dest: $TMP/new"), "{yaml}");
    assert!(!yaml.contains(&root), "raw tmpdir leaked: {yaml}");

    let sess = Session::new(Mode::Record, FileCassette::new(dir.path()));
    sess.record(&norm, &req, || Ok(FsResponse::default()))
        .unwrap();
    let fp = norm.fingerprint(&raw).unwrap();
    let on_disk = std::fs::read_to_string(dir.path().join(format!("fs-{fp}.req.yaml"))).unwrap();
    assert!(on_disk.contains("path: $TMP/old"), "{on_disk}");
    assert!(on_disk.contains("dest: $TMP/new"), "{on_disk}");
    assert!(
        !on_disk.contains(&root),
        "raw tmpdir leaked to cassette: {on_disk}"
    );
}

#[test]
fn normalize_empty_short_circuits() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let a = FsAdapter::new().with_normalizer(move |_p: &str| {
        seen.fetch_add(1, Ordering::SeqCst);
        "NEVER".to_string()
    });
    assert_eq!(a.normalize(""), "");
    let req = FsRequest {
        op: op::CHMOD.into(),
        path: String::new(),
        mode: Some(0o644),
        ..Default::default()
    };
    a.fingerprint(&req).unwrap();
    let n = a.normalize_request(&req);
    assert_eq!(n.path, "");
    assert_eq!(n.dest, "");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "empty path/dest must not invoke normalizer"
    );
}

#[test]
fn normalizer_result_is_taken_literally() {
    // Returning "" is allowed. A non-empty dest that normalizes to ""
    // is then empty for fingerprint purposes and drops out, per spec
    // ("dest — when non-empty, after path normalization").
    let blank = FsAdapter::new().with_normalizer(|_p: &str| String::new());
    let with_dest = FsRequest {
        op: op::RENAME.into(),
        path: "/a".into(),
        dest: "/b".into(),
        ..Default::default()
    };
    let no_dest = FsRequest {
        op: op::RENAME.into(),
        path: "/a".into(),
        ..Default::default()
    };
    assert_eq!(
        blank.fingerprint(&with_dest).unwrap(),
        blank.fingerprint(&no_dest).unwrap()
    );
    assert_eq!(blank.normalize("/anything"), "");
}

#[test]
fn dest_gated_on_normalized_value() {
    // spec: `dest` participates only when non-empty AFTER normalization.
    let a = FsAdapter::new().with_normalizer(|p: &str| {
        if p == "/x/drop" {
            String::new()
        } else {
            p.to_string()
        }
    });
    let no_dest = FsRequest {
        op: op::RENAME.into(),
        path: "/a".into(),
        ..Default::default()
    };
    let dropped = FsRequest {
        dest: "/x/drop".into(),
        ..no_dest.clone()
    };
    let kept = FsRequest {
        dest: "/x/keep".into(),
        ..no_dest.clone()
    };
    assert_eq!(
        a.fingerprint(&dropped).unwrap(),
        a.fingerprint(&no_dest).unwrap(),
        "dest normalized to \"\" must drop out of the fingerprint"
    );
    assert_ne!(
        a.fingerprint(&kept).unwrap(),
        a.fingerprint(&no_dest).unwrap()
    );
    // The persisted copy agrees: serde skips the empty dest.
    let yaml = serde_yaml::to_string(&a.normalize_request(&dropped)).unwrap();
    assert!(!yaml.contains("dest"), "{yaml}");
}

#[test]
fn empty_dest_stays_omitted_regardless_of_normalizer() {
    let a = FsAdapter::new().with_normalizer(|p: &str| {
        if p.is_empty() {
            "/ghost".to_string()
        } else {
            p.to_string()
        }
    });
    let no_dest = FsRequest {
        op: op::RENAME.into(),
        path: "/a".into(),
        ..Default::default()
    };
    let empty = FsRequest {
        dest: String::new(),
        ..no_dest.clone()
    };
    assert_eq!(
        a.fingerprint(&empty).unwrap(),
        a.fingerprint(&no_dest).unwrap()
    );
}

#[test]
fn chain_composes_left_to_right() {
    let tmp = normalizer(|p: &str| p.replacen("/tmp", "$TMP", 1));
    let home = normalizer(|p: &str| p.replacen("/home/u", "$HOME", 1));
    let a = FsAdapter::new().with_normalizer(chain([tmp, home]));
    assert_eq!(a.normalize("/tmp/foo"), "$TMP/foo");
    assert_eq!(a.normalize("/home/u/foo"), "$HOME/foo");
    assert_eq!(
        a.fingerprint(&write_req("/tmp/foo")).unwrap(),
        a.fingerprint(&write_req("$TMP/foo")).unwrap()
    );

    // Order matters: the second rule sees the first rule's output.
    let one = normalizer(|p: &str| format!("{p}1"));
    let two = normalizer(|p: &str| format!("{p}2"));
    assert_eq!(chain([one.clone(), two.clone()])("p"), "p12");
    assert_eq!(chain([two, one])("p"), "p21");

    // Empty chain is identity.
    assert_eq!(chain(Vec::new())("p"), "p");
}

#[test]
fn with_normalizer_replaces_previous() {
    let a = FsAdapter::new()
        .with_normalizer(|p: &str| format!("first:{p}"))
        .with_normalizer(|p: &str| format!("second:{p}"));
    assert_eq!(a.normalize("x"), "second:x");
}

#[test]
fn adapter_is_clone_and_thread_safe() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    let a = FsAdapter::new().with_normalizer(strip("/tmp"));
    assert_send_sync(&a);
    let b = a.clone();
    assert_eq!(a.normalize("/tmp/x"), b.normalize("/tmp/x"));
    assert_eq!(a.id(), "fs");
}

#[test]
fn spec_fixture_still_loads_and_matches_pin() {
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../spec/fixtures/fs-write");
    let cassette = FileCassette::new(&fixture);
    let (req, _resp): (FsRequest, FsResponse) = cassette.load("fs", SPEC_PIN).unwrap();
    assert_eq!(
        req.path, "$TMP/greeting.txt",
        "cassette stores the post-normalizer path"
    );
    assert_eq!(FsAdapter::new().fingerprint(&req).unwrap(), SPEC_PIN);
}
