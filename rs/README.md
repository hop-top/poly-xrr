# xrr — Rust SDK

> Auto-published from [poly-xrr](https://github.com/hop-top/poly-xrr).
> Do not open issues or PRs here — contribute to poly-xrr instead.

## Install

```bash
cargo add xrr
```

## Usage

```rust
use hop_top_xrr::{FileCassette, Mode, Session};

let sess = Session::new(Mode::Record, FileCassette::new("fixtures/my-test"));
let resp = sess.record(&adapter, &req, || do_request())?;
```

`Session::record(adapter, req, do_)` fingerprints `req` through `adapter`;
in `Mode::Record` it runs `do_` and saves the pair, in `Mode::Replay` it
loads the response from the cassette without calling `do_`.

### fs adapter: path normalization

Cassettes store paths in post-normalizer form, so tmpdir-based tests key
the same cassette on every run. Install a normalizer per adapter instance
and persist the normalized request:

```rust
use hop_top_xrr::adapters::fs::{chain, normalizer, FsAdapter};

let tmp = tempdir.path().to_string_lossy().into_owned();
let fs = FsAdapter::new().with_normalizer(chain([
    normalizer(move |p: &str| p.replacen(&tmp, "$TMP", 1)),
    normalizer(|p: &str| p.replace('\\', "/")),
]));
let req = fs.normalize_request(&raw_req); // path + dest rewritten, data untouched
let resp = sess.record(&fs, &req, || do_write())?;
```

## License

MIT — see [LICENSE](LICENSE)
