# xrr — PHP SDK

> Auto-published from [poly-xrr](https://github.com/hop-top/poly-xrr).
> Do not open issues or PRs here — contribute to poly-xrr instead.

## Install

```bash
composer require hop-top/xrr
```

## Usage

```php
$sess = new Session(cassette: 'fixtures/my-test');
$resp = $sess->record('http-get-users', $adapter);
$sess->close();
```

## License

MIT — see [LICENSE](LICENSE)
