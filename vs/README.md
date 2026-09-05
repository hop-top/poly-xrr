# vs — Replacing existing tools with xrr

Side-by-side migration guides: before (existing OSS package) → after (xrr).

| Directory | Packages replaced |
|-----------|------------------|
| [go/](go/README.md) | go-vcr · govcr · copyist · go-sqlmock · miniredis · go-redis/redismock · grpcmock |
| [py/](py/README.md) | vcrpy · pytest-recording · fakeredis · responses · httpretty |
| [ts/](ts/README.md) | Polly.JS · nock · msw · fetch-mock · redis-mock |
| [php/](php/README.md) | php-vcr · Guzzle MockHandler · M6Web/RedisMock |
| [rs/](rs/README.md) | rVCR · surf-vcr · wiremock-rs · httpmock · redis-test · SeaORM MockDatabase |

## Why replace

All packages above share at least one of these limitations xrr does not have:

- **HTTP only** — no exec, Redis, SQL, gRPC, or fs in the same session
- **Language-specific cassettes** — cannot replay a Go-recorded cassette in Python
- **Expectation-based** — hand-write every mock; no real interaction captured
- **Inactive** — Polly.JS archived 2021; php-vcr minimal maintenance since 2023
- **Infrastructure required** — mock servers, Docker, in-memory daemons
