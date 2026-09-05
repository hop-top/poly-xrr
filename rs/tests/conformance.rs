use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use hop_top_xrr::{
    adapters::exec::{ExecAdapter, ExecRequest, ExecResponse},
    adapters::fs::{FsAdapter, FsRequest, FsResponse},
    adapters::http::{HttpAdapter, HttpRequest},
    adapters::redis::{RedisAdapter, RedisRequest},
    adapters::sql::{SqlAdapter, SqlRequest},
    stream::StreamedPair,
    Adapter, FileCassette,
};

#[derive(Deserialize)]
struct Manifest {
    interactions: Vec<Interaction>,
}

#[derive(Deserialize)]
struct Interaction {
    adapter: String,
    fingerprint: String,
    /// Streamed entries route through the streaming load path
    /// (manifest extension; defaults to false when absent).
    #[serde(default)]
    streamed: bool,
    /// Unary entry whose fingerprint is a computed value: rebuild the
    /// adapter's request from the req payload and recompute it with the
    /// adapter's algorithm (defaults to false when absent).
    #[serde(default)]
    verify_fingerprint: bool,
}

/// Recompute a unary fingerprint from the raw req payload with the adapter's
/// own algorithm. Loading alone cannot expose a canonical-JSON escaping fork
/// — the files load in every port; the derived key is what differs.
///
/// The typed request is rebuilt from the payload value rather than through a
/// typed `load`: `HttpRequest.body` is bytes while the v1 envelope carries
/// the body as a string.
fn recompute_unary_fingerprint(adapter: &str, payload: &serde_yaml::Value) -> String {
    match adapter {
        "exec" => {
            let req: ExecRequest = serde_yaml::from_value(payload.clone()).expect("exec req");
            ExecAdapter.fingerprint(&req).expect("exec fingerprint")
        }
        "fs" => {
            let req: FsRequest = serde_yaml::from_value(payload.clone()).expect("fs req");
            FsAdapter.fingerprint(&req).expect("fs fingerprint")
        }
        "sql" => {
            let req: SqlRequest = serde_yaml::from_value(payload.clone()).expect("sql req");
            SqlAdapter.fingerprint(&req).expect("sql fingerprint")
        }
        "redis" => {
            let req: RedisRequest = serde_yaml::from_value(payload.clone()).expect("redis req");
            RedisAdapter.fingerprint(&req).expect("redis fingerprint")
        }
        "http" => {
            // Fail fast on malformed fixtures: a missing or non-string
            // method/url must not silently recompute from "".
            let text = |key: &str| -> String {
                match payload.get(key) {
                    Some(v) => v
                        .as_str()
                        .unwrap_or_else(|| panic!("http fixture: `{key}` must be a string"))
                        .to_string(),
                    None => panic!("http fixture: `{key}` is required"),
                }
            };
            let body = match payload.get("body") {
                Some(v) => v
                    .as_str()
                    .unwrap_or_else(|| panic!("http fixture: `body` must be a string"))
                    .to_string(),
                None => String::new(),
            };
            let headers: HashMap<String, String> = payload
                .get("headers")
                .cloned()
                .map(|v| serde_yaml::from_value(v).expect("http headers"))
                .unwrap_or_default();
            let req = HttpRequest {
                method: text("method"),
                url: text("url"),
                headers,
                body: body.into_bytes(),
            };
            HttpAdapter.fingerprint(&req).expect("http fingerprint")
        }
        other => panic!("verify_fingerprint: no unary fingerprint model for adapter {other}"),
    }
}

/// Walk spec/fixtures/ dirs, load each manifest and verify all cassette
/// pairs load without error.
#[test]
fn test_conformance_fixtures() {
    // Path relative to workspace root (where cargo test is run from).
    let fixtures_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../spec/fixtures");

    assert!(
        fixtures_root.exists(),
        "fixtures dir not found at {:?}",
        fixtures_root
    );

    let mut total = 0usize;

    for entry in std::fs::read_dir(&fixtures_root).expect("read fixtures dir") {
        let entry = entry.expect("dir entry");
        let fixture_dir = entry.path();
        if !fixture_dir.is_dir() {
            continue;
        }

        let manifest_path = fixture_dir.join("manifest.yaml");
        if !manifest_path.exists() {
            continue;
        }

        let manifest_str =
            std::fs::read_to_string(&manifest_path).expect("read manifest");
        let manifest: Manifest =
            serde_yaml::from_str(&manifest_str).expect("parse manifest");

        let cassette = FileCassette::new(&fixture_dir);

        for interaction in &manifest.interactions {
            if interaction.streamed {
                let result = StreamedPair::load(
                    &fixture_dir,
                    &interaction.adapter,
                    &interaction.fingerprint,
                );
                assert!(
                    result.is_ok(),
                    "failed to load streamed {}/{}: {:?}",
                    fixture_dir.display(),
                    interaction.fingerprint,
                    result.err()
                );
                total += 1;
                continue;
            }
            match interaction.adapter.as_str() {
                "exec" => {
                    let result: Result<(ExecRequest, ExecResponse), _> =
                        cassette.load(&interaction.adapter, &interaction.fingerprint);
                    assert!(
                        result.is_ok(),
                        "failed to load {}/{}: {:?}",
                        fixture_dir.display(),
                        interaction.fingerprint,
                        result.err()
                    );
                }
                "fs" => {
                    let result: Result<(FsRequest, FsResponse), _> =
                        cassette.load(&interaction.adapter, &interaction.fingerprint);
                    assert!(
                        result.is_ok(),
                        "failed to load {}/{}: {:?}",
                        fixture_dir.display(),
                        interaction.fingerprint,
                        result.err()
                    );
                }
                other => {
                    // For adapters not yet modelled, just verify files exist.
                    let req_path = fixture_dir.join(format!(
                        "{}-{}.req.yaml",
                        other, interaction.fingerprint
                    ));
                    let resp_path = fixture_dir.join(format!(
                        "{}-{}.resp.yaml",
                        other, interaction.fingerprint
                    ));
                    assert!(req_path.exists(), "missing req: {:?}", req_path);
                    assert!(resp_path.exists(), "missing resp: {:?}", resp_path);
                }
            }
            if interaction.verify_fingerprint {
                let (req, _resp): (serde_yaml::Value, serde_yaml::Value) = cassette
                    .load(&interaction.adapter, &interaction.fingerprint)
                    .expect("load raw pair");
                assert_eq!(
                    recompute_unary_fingerprint(&interaction.adapter, &req),
                    interaction.fingerprint,
                    "unary fingerprint recomputation mismatch in {}/{}",
                    fixture_dir.display(),
                    interaction.fingerprint
                );
            }
            total += 1;
        }
    }

    assert!(total > 0, "no interactions found in fixtures");
    println!("conformance: {} interactions verified", total);
}
