# xrr — TypeScript SDK

> Auto-published from [poly-xrr](https://github.com/hop-top/poly-xrr).
> Do not open issues or PRs here — contribute to poly-xrr instead.

## Install

```bash
npm install @hop-top/xrr
```

## Usage

```ts
const sess = new Session({ cassette: "fixtures/my-test" });
const resp = await sess.record("http-get-users", adapter);
sess.close();
```

## License

MIT — see [LICENSE](LICENSE)
