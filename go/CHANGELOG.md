# Changelog

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
