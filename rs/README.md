# xrr — Rust SDK

> Auto-published from [poly-xrr](https://github.com/hop-top/poly-xrr).
> Do not open issues or PRs here — contribute to poly-xrr instead.

## Install

```bash
cargo add xrr
```

## Usage

```rust
let mut sess = Session::new(cassette("fixtures/my-test"));
let resp = sess.record("http-get-users", &adapter)?;
sess.close();
```

## License

MIT — see [LICENSE](LICENSE)
