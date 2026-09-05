# Changelog

## [0.1.0-alpha.7](https://github.com/hop-top/poly-xrr/compare/xrr-rs/v0.1.0-alpha.6...xrr-rs/v0.1.0-alpha.7) (2026-09-05)


### Features

* **rs:** fs adapter path normalizer hook ([#47](https://github.com/hop-top/poly-xrr/issues/47)) ([8555fdc](https://github.com/hop-top/poly-xrr/commit/8555fdc4201aed9be5119912258a00f1716c1240))


### Bug Fixes

* canonicalize fingerprint JSON per RFC 8785 across ports ([791ccc7](https://github.com/hop-top/poly-xrr/commit/791ccc7e3cf792a0dc5c06c179b9a72ad550d509))

## [0.1.0-alpha.6](https://github.com/hop-top/poly-xrr/compare/xrr-rs/v0.1.0-alpha.5...xrr-rs/v0.1.0-alpha.6) (2026-08-30)


### Features

* merge record-time secret redaction across all ports ([3db3528](https://github.com/hop-top/poly-xrr/commit/3db35280309649eb260d7430686c1658d35fdccd))
* record-time secret redaction across all ports ([a9a3664](https://github.com/hop-top/poly-xrr/commit/a9a3664c1cba9c77cc68a41c75dcbff5990420f1))
* **rs:** frame-level scrub hook for streamed cassettes ([1bc4e3f](https://github.com/hop-top/poly-xrr/commit/1bc4e3fa3f85e05421b4e9023600c3be38866daf))
* **rs:** gRPC streaming adapter ([753331b](https://github.com/hop-top/poly-xrr/commit/753331b4780e047f64de98c078fe280326f809a7))
* **rs:** gRPC streaming adapter over tonic ([96f2768](https://github.com/hop-top/poly-xrr/commit/96f276884cee6527f2a65995f999e56b79ddc551))
* **rs:** redact secrets from cassettes at record time ([3763a80](https://github.com/hop-top/poly-xrr/commit/3763a807318c58d3f7b5229d05366fbfc21ff9b0))
* **rs:** stream record/replay session machinery ([fc9b1c7](https://github.com/hop-top/poly-xrr/commit/fc9b1c759dd17c46b847cd8593b826f26cf281ad))
* **rs:** streamed-interaction format support ([37ec604](https://github.com/hop-top/poly-xrr/commit/37ec604273422e34f8f2c481a80e0840d43265f9))
* **spec:** normative frame scrub hook contract ([3e20dd7](https://github.com/hop-top/poly-xrr/commit/3e20dd78f377e8b961cbcccd476c468e47619895))
* stream recording across cassette format and ports ([38f71bd](https://github.com/hop-top/poly-xrr/commit/38f71bdb4032fde482bc8dd45bafbcbba0a49ab2))

## [0.1.0-alpha.5](https://github.com/hop-top/poly-xrr/compare/xrr-rs/v0.1.0-alpha.4...xrr-rs/v0.1.0-alpha.5) (2026-05-17)


### Bug Fixes

* **rs:** rename crate to hop-top-xrr + add required metadata ([f7dbf35](https://github.com/hop-top/poly-xrr/commit/f7dbf352ab529d6837c3a65f323a8e27ce95ad6a))

## [0.1.0-alpha.4](https://github.com/hop-top/poly-xrr/compare/xrr-rs/v0.1.0-alpha.3...xrr-rs/v0.1.0-alpha.4) (2026-05-17)


### Features

* **fs:** adapter for filesystem mutations + 5-language port + daemon docs ([9545797](https://github.com/hop-top/poly-xrr/commit/954579711af2da0965f1f96883f6800a1920876c))
* **rs/fs:** port fs adapter for cross-runtime conformance ([9d9c90f](https://github.com/hop-top/poly-xrr/commit/9d9c90fb1cdb26d39fb5b12bcdbf5ad2cb2a20cb))
