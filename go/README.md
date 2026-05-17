# xrr — Go SDK

> Auto-published from [poly-xrr](https://github.com/hop-top/poly-xrr).
> Do not open issues or PRs here — contribute to poly-xrr instead.

## Install

```bash
go get hop.top/xrr
```

## Usage

```go
sess := xrr.New(xrr.WithCassette("fixtures/my-test"))
defer sess.Close()

resp, err := sess.Record("http-get-users", adapter)
```

## License

MIT — see [LICENSE](LICENSE)
