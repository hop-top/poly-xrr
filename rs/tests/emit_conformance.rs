// Cross-port re-emit conformance (spec/emitted/README.md).
//
// Self-load round-trips (stream_conformance.rs) cannot see an emit slip the
// emitting port's own reader tolerates. Each port therefore checks in its own
// re-emission of every streamed golden pair under spec/emitted/<port>/, and
// every port loads every tree back to the golden model:
// - reemission_pinned: spec/emitted/rs equals a fresh save of every pair
//   (file set and bytes); XRR_UPDATE_EMITTED=1 regenerates (`make emit-rs`).
// - cross_port_reemissions_load_to_golden: each port's tree loads through the
//   Rust strict reader field-for-field equal to the golden pair.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use hop_top_xrr::stream::StreamedPair;
use serde::Deserialize;
use tempfile::TempDir;

const THIS_PORT: &str = "rs";

#[derive(Deserialize)]
struct Manifest {
    interactions: Vec<Interaction>,
}

#[derive(Deserialize)]
struct Interaction {
    adapter: String,
    fingerprint: String,
    #[serde(default)]
    streamed: bool,
}

fn spec_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../spec")
        .join(name)
}

fn dir_name(p: &Path) -> String {
    p.file_name().unwrap().to_string_lossy().into_owned()
}

/// Fixture dirs with at least one streamed entry, sorted by name.
fn streamed_fixture_dirs() -> Vec<(String, Vec<Interaction>)> {
    let mut dirs: Vec<(String, Vec<Interaction>)> = std::fs::read_dir(spec_dir("fixtures"))
        .expect("read fixtures dir")
        .filter_map(|e| {
            let p = e.expect("dir entry").path();
            if !p.is_dir() {
                return None;
            }
            let raw = std::fs::read_to_string(p.join("manifest.yaml")).expect("read manifest");
            let m: Manifest = serde_yaml::from_str(&raw).expect("parse manifest");
            let entries: Vec<Interaction> =
                m.interactions.into_iter().filter(|i| i.streamed).collect();
            (!entries.is_empty()).then(|| (dir_name(&p), entries))
        })
        .collect();
    dirs.sort_by(|a, b| a.0.cmp(&b.0));
    dirs
}

/// Every regular file under `root`, keyed by `/`-joined relative path.
fn read_tree(root: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        for e in std::fs::read_dir(dir).expect("read dir") {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                walk(root, &p, out);
            } else {
                let rel = p
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                out.insert(rel, std::fs::read_to_string(&p).expect("read file"));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// Runs `save` over every streamed golden pair; returns the emitted files
/// keyed by `<fixture>/<adapter>-<fp>.<kind>.yaml`.
fn reemit_streamed_fixtures() -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();
    for (name, entries) in streamed_fixture_dirs() {
        let golden = spec_dir("fixtures").join(&name);
        let tmp = TempDir::new().unwrap();
        for i in &entries {
            let ctx = format!("{name}/{}-{}", i.adapter, i.fingerprint);
            let pair = StreamedPair::load(&golden, &i.adapter, &i.fingerprint)
                .unwrap_or_else(|e| panic!("{ctx}: load failed: {e}"));
            pair.save(tmp.path())
                .unwrap_or_else(|e| panic!("{ctx}: save failed: {e}"));
            for kind in ["req", "resp"] {
                let file = format!("{}-{}.{kind}.yaml", i.adapter, i.fingerprint);
                let text = std::fs::read_to_string(tmp.path().join(&file)).expect("read emitted");
                files.insert(format!("{name}/{file}"), text);
            }
        }
    }
    files
}

/// Field-for-field pair equality; messages compared over decoded bytes
/// (encoding choice is free on re-emit per spec).
fn assert_pair_eq(a: &StreamedPair, b: &StreamedPair, ctx: &str) {
    assert_eq!(a.req.adapter, b.req.adapter, "{ctx}: req adapter");
    assert_eq!(
        a.req.fingerprint, b.req.fingerprint,
        "{ctx}: req fingerprint"
    );
    assert_eq!(
        a.req.recorded_at, b.req.recorded_at,
        "{ctx}: req recorded_at"
    );
    assert_eq!(a.req.payload, b.req.payload, "{ctx}: req payload");
    assert_eq!(
        a.req.stream.stream_type, b.req.stream.stream_type,
        "{ctx}: type"
    );
    assert_eq!(
        a.req.stream.half_close, b.req.stream.half_close,
        "{ctx}: half_close"
    );
    assert_eq!(a.resp.adapter, b.resp.adapter, "{ctx}: resp adapter");
    assert_eq!(
        a.resp.fingerprint, b.resp.fingerprint,
        "{ctx}: resp fingerprint"
    );
    assert_eq!(
        a.resp.recorded_at, b.resp.recorded_at,
        "{ctx}: resp recorded_at"
    );
    assert_eq!(a.resp.payload, b.resp.payload, "{ctx}: resp payload");
    assert_eq!(a.resp.error, b.resp.error, "{ctx}: resp error");
    assert_eq!(a.resp.stream.end, b.resp.stream.end, "{ctx}: end");
    for (side, fa, fb) in [
        ("req", &a.req.stream.frames, &b.req.stream.frames),
        ("resp", &a.resp.stream.frames, &b.resp.stream.frames),
    ] {
        assert_eq!(fa.len(), fb.len(), "{ctx}: {side} frame count");
        for (x, y) in fa.iter().zip(fb.iter()) {
            assert_eq!(x.seq, y.seq, "{ctx}: {side} frame seq");
            assert_eq!(
                x.bytes, y.bytes,
                "{ctx}: {side} frame bytes (seq {})",
                x.seq
            );
            assert_eq!(
                x.at_ms, y.at_ms,
                "{ctx}: {side} frame at_ms (seq {})",
                x.seq
            );
        }
    }
}

#[test]
fn reemission_pinned() {
    let want = reemit_streamed_fixtures();
    let tree = spec_dir("emitted").join(THIS_PORT);

    if std::env::var_os("XRR_UPDATE_EMITTED").is_some() {
        if tree.exists() {
            std::fs::remove_dir_all(&tree).unwrap();
        }
        for (rel, text) in &want {
            let path = tree.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, text).unwrap();
        }
        return;
    }

    assert!(
        tree.is_dir(),
        "missing {}: regenerate with `make emit-rs`",
        tree.display()
    );
    let got = read_tree(&tree);
    assert_eq!(
        got.keys().collect::<Vec<_>>(),
        want.keys().collect::<Vec<_>>(),
        "file set drifted: regenerate with `make emit-rs`"
    );
    for (rel, text) in &want {
        assert_eq!(
            &got[rel], text,
            "{rel} drifted: regenerate with `make emit-rs`"
        );
    }
}

#[test]
fn cross_port_reemissions_load_to_golden() {
    let root = spec_dir("emitted");
    let mut ports: Vec<String> = std::fs::read_dir(&root)
        .unwrap_or_else(|e| {
            panic!(
                "missing {}: regenerate with `make emit-all` ({e})",
                root.display()
            )
        })
        .filter_map(|e| {
            let p = e.expect("dir entry").path();
            p.is_dir().then(|| dir_name(&p))
        })
        .collect();
    ports.sort();
    assert!(!ports.is_empty(), "no port trees under {}", root.display());

    for port in &ports {
        for (name, entries) in streamed_fixture_dirs() {
            let golden = spec_dir("fixtures").join(&name);
            let emitted = root.join(port).join(&name);
            for i in &entries {
                let ctx = format!(
                    "{port} re-emission of {name}/{}-{}",
                    i.adapter, i.fingerprint
                );
                let want = StreamedPair::load(&golden, &i.adapter, &i.fingerprint).unwrap();
                let got = StreamedPair::load(&emitted, &i.adapter, &i.fingerprint)
                    .unwrap_or_else(|e| panic!("{ctx}: {e} (regenerate with `make emit-{port}`)"));
                assert_pair_eq(&want, &got, &ctx);
            }
        }
    }
}
