# Changelog

## [0.1.0-alpha.6](https://github.com/hop-top/poly-xrr/compare/xrr/v0.1.0-alpha.5...xrr/v0.1.0-alpha.6) (2026-09-05)


### ⚠ BREAKING CHANGES

* **go:** cassettes whose fingerprinted fields contain `&`, `<` or `>` (any adapter), or U+2028/U+2029 (stream identities), re-fingerprint under RFC 8785 escaping; re-record them.

### Bug Fixes

* canonicalize fingerprint JSON per RFC 8785 across ports ([791ccc7](https://github.com/hop-top/poly-xrr/commit/791ccc7e3cf792a0dc5c06c179b9a72ad550d509))
* gate fs dest on the normalized value in Go and Python ([#52](https://github.com/hop-top/poly-xrr/issues/52)) ([4fad662](https://github.com/hop-top/poly-xrr/commit/4fad6628dc08af8dd27a44ee5637544a12e07835))
* **go:** canonicalize fingerprint JSON per RFC 8785 ([4a3b6aa](https://github.com/hop-top/poly-xrr/commit/4a3b6aaf0a7dbf34a306ffa7efd2ba47a559f858))

## [0.1.0-alpha.5](https://github.com/hop-top/poly-xrr/compare/xrr/v0.1.0-alpha.4...xrr/v0.1.0-alpha.5) (2026-08-30)


### Features

* **go/grpc:** streaming adapter — server, client, bidi record/replay ([a019d5e](https://github.com/hop-top/poly-xrr/commit/a019d5e4099d781a944241519b98ee5c45cfa6a5))
* **go:** frame-level scrub hook for streamed cassettes ([8911fcf](https://github.com/hop-top/poly-xrr/commit/8911fcfa457f02eadf162c54192ac6950bbea720))
* **go:** redact secrets from cassettes at record time ([ff28193](https://github.com/hop-top/poly-xrr/commit/ff281930988c67a8a27c527583bcb0a0ba4c0d57))
* **go:** redact secrets from cassettes at record time ([181b999](https://github.com/hop-top/poly-xrr/commit/181b999e84f35fb286101a5e84b604468a1c9d3f))
* **go:** stream-aware session and cassette IO ([0d59c28](https://github.com/hop-top/poly-xrr/commit/0d59c28b468fd85b241ab0384fb75059f9f3d812))
* **go:** transport-level gRPC capture via WithContextDialer ([f3f88b5](https://github.com/hop-top/poly-xrr/commit/f3f88b58808c36ce34b39c92da6dedffd2809e58))
* **go:** transport-level gRPC capture via WithContextDialer ([9107d5e](https://github.com/hop-top/poly-xrr/commit/9107d5e11512ce425e816a2ef656071eb7896e31))
* merge record-time secret redaction across all ports ([3db3528](https://github.com/hop-top/poly-xrr/commit/3db35280309649eb260d7430686c1658d35fdccd))
* record-time secret redaction across all ports ([a9a3664](https://github.com/hop-top/poly-xrr/commit/a9a3664c1cba9c77cc68a41c75dcbff5990420f1))
* **spec:** normative frame scrub hook contract ([3e20dd7](https://github.com/hop-top/poly-xrr/commit/3e20dd78f377e8b961cbcccd476c468e47619895))
* stream recording across cassette format and ports ([38f71bd](https://github.com/hop-top/poly-xrr/commit/38f71bdb4032fde482bc8dd45bafbcbba0a49ab2))


### Bug Fixes

* **go/grpc:** deterministic proto marshal ([6444dcf](https://github.com/hop-top/poly-xrr/commit/6444dcf92f53eeddacf5dbf0b4160ee39c90abde))
* **go:** standard-only JSON escaping in stream canonical ([fcd0539](https://github.com/hop-top/poly-xrr/commit/fcd0539e246c6d56dbe9e95c07f3af740220f72e))


### Refactoring

* **go:** adapter-supplied stream fingerprint inputs ([81c0c2e](https://github.com/hop-top/poly-xrr/commit/81c0c2e5d1a256068f6211d4a6d1df99932cf079))
* **go:** surface recording errors, drop dead state, unit-test demux ([8c2c03d](https://github.com/hop-top/poly-xrr/commit/8c2c03d39ddf82c6c615047c12a5859d146b3b5b))
* **go:** use core redactor for transport header sanitization ([3961bc3](https://github.com/hop-top/poly-xrr/commit/3961bc3c2b1a9a2875498000be5b9c4afed22e4e))

## [0.1.0-alpha.4](https://github.com/hop-top/poly-xrr/compare/xrr/v0.1.0-alpha.3...xrr/v0.1.0-alpha.4) (2026-05-17)


### Features

* exec cwd fingerprint (Go-only extension) + XRR_MODE/XRR_CASSETTE_DIR env convention ([76ee9de](https://github.com/hop-top/poly-xrr/commit/76ee9ded2dc4c92d7f2888983284c52708c8403f))
* **exec:** ExitCodeFromError + wrap_command_runner example ([#2](https://github.com/hop-top/poly-xrr/issues/2)) ([e5dd9d4](https://github.com/hop-top/poly-xrr/commit/e5dd9d4a3915c5d360bde853c07fd26efb1d481a))
* **fs:** adapter for filesystem mutations + 5-language port + daemon docs ([9545797](https://github.com/hop-top/poly-xrr/commit/954579711af2da0965f1f96883f6800a1920876c))
* **fs:** adapter for filesystem mutations with string-typed Data field ([c7cdc56](https://github.com/hop-top/poly-xrr/commit/c7cdc561be443b0295b6615fcccad67650357203))
* **fs:** fingerprint over canonical JSON with omit-on-zero ([64e6f7d](https://github.com/hop-top/poly-xrr/commit/64e6f7d64e98c1c7d0f421bc67aa15731fa69451))
* **fs:** scaffold fs adapter package with Request/Response/Adapter types ([3ac58f0](https://github.com/hop-top/poly-xrr/commit/3ac58f0e885f21ae689a32f24d1769ce6afaa708))
* **fs:** wrap_fs_runner example demonstrates adoption pattern ([f468c67](https://github.com/hop-top/poly-xrr/commit/f468c67d45df2a3b4fe74cc40e18ecca3fc8213f))
* **session:** persist do() error in cassette + replay re-emits it ([#3](https://github.com/hop-top/poly-xrr/issues/3)) ([e2925db](https://github.com/hop-top/poly-xrr/commit/e2925dbd4f488b46d3b6c4dc90a9e8a97efebb2b))
* **xrr:** SessionFromEnv + XRR_MODE/XRR_CASSETTE_DIR convention (T-0039) ([586a76d](https://github.com/hop-top/poly-xrr/commit/586a76d41a3e2732584f0e76e45b50fb8b6a4e24))


### Bug Fixes

* **exec:** include Cwd in fingerprint as Go-only extension (T-0040) ([0413919](https://github.com/hop-top/poly-xrr/commit/0413919afc2129687173c7c58f0fa3371eae8dab))
* **fs:** cassette payload paths must agree with fingerprint inputs ([523ff83](https://github.com/hop-top/poly-xrr/commit/523ff836d26245f04068875bfc5b4651ebb9e1bc))
