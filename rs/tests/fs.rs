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
fn normalizer_mapping_dest_to_empty_drops_it() {
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

// --- serde_yaml wire form ---------------------------------------------------
//
// Mirrors TestSerializeRoundtrip / TestSerializeOmitsZeroOptionals /
// TestSerializeBase64Payload / TestSerializeResponseRoundtrip in Go. The
// structs carry no PartialEq, so equality is asserted per field and by
// re-serializing: identical YAML text means nothing was lost either way.

fn assert_req_eq(got: &FsRequest, want: &FsRequest) {
    assert_eq!(got.op, want.op);
    assert_eq!(got.path, want.path);
    assert_eq!(got.data, want.data);
    assert_eq!(got.mode, want.mode);
    assert_eq!(got.uid, want.uid);
    assert_eq!(got.gid, want.gid);
    assert_eq!(got.dest, want.dest);
    assert_eq!(got.size, want.size);
    assert_eq!(got.flags, want.flags);
    assert_eq!(got.recursive, want.recursive);
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// std has no base64 and the adapter only needs the string to be opaque, so
// a minimal RFC 4648 codec keeps the test free of extra dev-dependencies.
fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n =
            chunk.iter().fold(0u32, |acc, b| (acc << 8) | u32::from(*b)) << (8 * (3 - chunk.len()));
        for i in 0..=chunk.len() {
            out.push(B64[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
        for _ in chunk.len()..3 {
            out.push('=');
        }
    }
    out
}

fn b64_decode(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for chunk in s.trim_end_matches('=').as_bytes().chunks(4) {
        let n = chunk.iter().fold(0u32, |acc, c| {
            let idx = B64.iter().position(|b| b == c).expect("base64 alphabet");
            (acc << 6) | idx as u32
        }) << (6 * (4 - chunk.len()));
        for i in 0..chunk.len() - 1 {
            out.push(((n >> (16 - 8 * i)) & 0xff) as u8);
        }
    }
    out
}

#[test]
fn request_roundtrips_through_yaml() {
    let req = FsRequest {
        op: op::WRITE.into(),
        path: "/etc/hosts".into(),
        data: "127.0.0.1 localhost\n".into(),
        mode: Some(0o644),
        flags: 0,
        ..Default::default()
    };
    let yaml = serde_yaml::to_string(&req).unwrap();
    assert!(yaml.contains("mode: 420"), "{yaml}");

    let got: FsRequest = serde_yaml::from_str(&yaml).unwrap();
    assert_req_eq(&got, &req);
    assert_eq!(got.mode, Some(0o644), "mode survives the round-trip");
    assert_eq!(
        serde_yaml::to_string(&got).unwrap(),
        yaml,
        "re-serializing the round-tripped request must reproduce the text"
    );
}

#[test]
fn request_yaml_omits_unset_optionals() {
    let bare = FsRequest {
        op: op::WRITE.into(),
        path: "/x".into(),
        data: "y".into(),
        ..Default::default()
    };
    let yaml = serde_yaml::to_string(&bare).unwrap();
    for forbidden in [
        "dest:",
        "mode:",
        "uid:",
        "gid:",
        "size:",
        "flags:",
        "recursive:",
    ] {
        assert!(
            !yaml.contains(forbidden),
            "bare write must omit {forbidden:?}: {yaml}"
        );
    }

    let no_data = FsRequest {
        op: op::MKDIR.into(),
        path: "/d".into(),
        ..Default::default()
    };
    let yaml = serde_yaml::to_string(&no_data).unwrap();
    assert!(
        !yaml.contains("data:"),
        "empty data must be omitted: {yaml}"
    );

    // Presence-bearing fields stay when set, even to zero.
    let zero_mode = FsRequest {
        mode: Some(0),
        ..bare.clone()
    };
    let yaml = serde_yaml::to_string(&zero_mode).unwrap();
    assert!(yaml.contains("mode: 0"), "Some(0) must emit mode: {yaml}");

    let flagged = FsRequest {
        flags: 1,
        recursive: true,
        ..bare.clone()
    };
    let yaml = serde_yaml::to_string(&flagged).unwrap();
    assert!(yaml.contains("flags: 1"), "{yaml}");
    assert!(yaml.contains("recursive: true"), "{yaml}");

    // Replay side: a minimal cassette written by another port (keys
    // omitted, not null) loads with the same defaults it was hashed with.
    let got: FsRequest = serde_yaml::from_str("op: write\npath: /x\n").unwrap();
    assert_req_eq(
        &got,
        &FsRequest {
            op: op::WRITE.into(),
            path: "/x".into(),
            ..Default::default()
        },
    );
    assert_eq!(got.mode, None);
    assert_eq!(got.flags, 0);
    assert!(!got.recursive);
}

#[test]
fn base64_payload_roundtrips_through_yaml() {
    // Spec "Data Field Encoding": binary callers base64-encode before the
    // adapter sees `data`; serde_yaml stores the string verbatim (no
    // !!binary); the caller decodes on the way back.
    let raw = [0x00u8, 0xff, 0xc3, 0x28, 0x80, 0x01, 0x02, 0x03];
    let encoded = b64_encode(&raw);
    assert_eq!(encoded, "AP/DKIABAgM=");

    let req = FsRequest {
        op: op::WRITE.into(),
        path: "/bin/x".into(),
        data: encoded.clone(),
        ..Default::default()
    };
    let yaml = serde_yaml::to_string(&req).unwrap();
    assert!(yaml.contains(&encoded), "{yaml}");
    assert!(!yaml.contains("!!binary"), "{yaml}");

    let got: FsRequest = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(got.data, encoded, "base64 string must round-trip exactly");
    assert_eq!(
        b64_decode(&got.data),
        raw,
        "caller recovers the original bytes"
    );
    // Opaque to the fingerprint too: text or base64, only the bytes matter.
    let a = FsAdapter::new();
    assert_eq!(a.fingerprint(&got).unwrap(), a.fingerprint(&req).unwrap());
}

#[test]
fn response_roundtrips_through_yaml() {
    let resp = FsResponse {
        duration_ms: 42,
        bytes_written: 1024,
    };
    let yaml = serde_yaml::to_string(&resp).unwrap();
    let got: FsResponse = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(got.duration_ms, 42);
    assert_eq!(got.bytes_written, 1024);
    assert_eq!(serde_yaml::to_string(&got).unwrap(), yaml);

    // Zero counters are omitted on the way out ...
    let yaml = serde_yaml::to_string(&FsResponse::default()).unwrap();
    assert_eq!(yaml, "{}\n", "all-zero response serializes as an empty map");
    let got: FsResponse = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!((got.duration_ms, got.bytes_written), (0, 0));

    // ... and accepted explicitly on the way in (spec failure envelope).
    let got: FsResponse = serde_yaml::from_str("duration_ms: 0\nbytes_written: 0\n").unwrap();
    assert_eq!((got.duration_ms, got.bytes_written), (0, 0));
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
