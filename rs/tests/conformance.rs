use std::path::Path;

use serde::Deserialize;
use hop_top_xrr::{
    adapters::exec::{ExecRequest, ExecResponse},
    adapters::fs::{FsRequest, FsResponse},
    FileCassette,
};

#[derive(Deserialize)]
struct Manifest {
    interactions: Vec<Interaction>,
}

#[derive(Deserialize)]
struct Interaction {
    adapter: String,
    fingerprint: String,
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
            total += 1;
        }
    }

    assert!(total > 0, "no interactions found in fixtures");
    println!("conformance: {} interactions verified", total);
}
