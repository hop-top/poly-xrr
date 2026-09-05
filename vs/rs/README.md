# Replacing Rust HTTP/interaction mocking with xrr

Covers: rVCR · surf-vcr · wiremock · httpmock · redis-test · SeaORM MockDatabase

---

## rVCR → xrr (HTTP)

`rVCR` records HTTP via `reqwest`; Rust-specific cassettes, reqwest-only.

### Before (rVCR)

```pseudocode
use rvcr::{VCRMiddleware, VCRMode};
use reqwest::Client;
use reqwest_middleware::ClientBuilder;

let middleware = VCRMiddleware::try_from("fixtures/my-cassette.bin")
    .unwrap()
    .with_mode(VCRMode::Record);

let client = ClientBuilder::new(Client::new())
    .with(middleware)
    .build();

let resp = client.get("https://api.example.com/users").send().await.unwrap();
// cassette written as Rust-specific binary/YAML
// reqwest only — no other HTTP clients
// cassette cannot replay in Go or Python
// no exec, Redis, SQL support
```

### After (xrr)

```pseudocode
use xrr::{Session, Mode, FileCassette};
use xrr::adapters::http::{HttpAdapter, HttpRequest};

let cassette = FileCassette::new("cassettes/");
let adapter = HttpAdapter::new();
let req = HttpRequest { method: "GET".into(), url: "https://api.example.com/users".into(), .. };

// Record
let mut rec = Session::new(Mode::Record, cassette.clone());
let resp = rec.record(&adapter, &req, || async { real_http_get(&req).await }).await?;

// Replay — no network
let mut rep = Session::new(Mode::Replay, cassette);
let resp2 = rep.record(&adapter, &req, || async { unreachable!() }).await?;
// cassette replays in Go, Python, TypeScript, PHP unchanged
```

### Key differences

- rVCR: reqwest middleware only; xrr: any HTTP client via closure
- rVCR: Rust-specific cassette format; xrr: language-agnostic YAML
- rVCR: HTTP only; xrr: HTTP + exec + Redis + SQL + fs
- rVCR: requires `reqwest-middleware`; xrr: no client dependency

---

## surf-vcr → xrr (HTTP)

`surf-vcr` records HTTP via `surf` client only; same cross-language limitations.

### Before (surf-vcr)

```pseudocode
use surf_vcr::{VcrMiddleware, VcrMode};

let client = surf::Client::new()
    .with(VcrMiddleware::new(VcrMode::Record, "fixtures/cassette.yaml").await?);

let resp = client.get("https://api.example.com/users").await?;
// surf-specific; YAML but Rust-specific serialization
// HTTP only; no multi-channel; no cross-language replay
```

### After (xrr)

```pseudocode
// same as rVCR → xrr above
// xrr is client-agnostic; wrap any HTTP call in a closure
```

---

## wiremock-rs / httpmock → xrr (HTTP)

Both run a local mock server; expectation-based, no recording.

### Before (wiremock-rs)

```pseudocode
use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, path};

let server = MockServer::start().await;
Mock::given(method("GET"))
    .and(path("/users"))
    .respond_with(ResponseTemplate::new(200)
        .set_body_json(json!([{"id": 1, "name": "Alice"}])))
    .mount(&server)
    .await;

let resp = reqwest::get(format!("{}/users", server.uri())).await?;
// hand-written mock — must anticipate every field
// server started per test; resource overhead
// no recording; no cassette persistence; Rust-only
```

### After (xrr)

```pseudocode
// No mock server; no hand-written expectations
let mut rec = Session::new(Mode::Record, FileCassette::new("cassettes/"));
rec.record(&adapter, &req, || async { real_http_get(&req).await }).await?;

let mut rep = Session::new(Mode::Replay, FileCassette::new("cassettes/"));
let resp = rep.record(&adapter, &req, || async { unreachable!() }).await?;
```

### Key differences

- wiremock/httpmock: starts TCP server per test; xrr: zero infrastructure
- Both: hand-write every mock field; xrr: capture real response automatically
- Both: Rust-only; xrr cassettes cross-language
- wiremock/httpmock: no cassette persistence; xrr: cassettes in VCS

---

## redis-test → xrr (Redis)

`redis-test` is expectation-based; no recording.

### Before (redis-test)

```pseudocode
use redis_test::{MockRedisConnection, IntoRedisValue};
use redis::cmd;

let mut mock = MockRedisConnection::new(vec![
    cmd("GET").arg("session:42").into_redis_value("user-data"),
]);

let val: String = redis::cmd("GET")
    .arg("session:42")
    .query(&mut mock)?;
// hand-written mock sequence — must match exact command order
// breaks if command order changes; Rust-only
// no cassette persistence
```

### After (xrr)

```pseudocode
use xrr::adapters::redis::{RedisAdapter, RedisRequest};

let adapter = RedisAdapter::new();
let req = RedisRequest { command: "GET".into(), args: vec!["session:42".into()] };

let mut rec = Session::new(Mode::Record, FileCassette::new("cassettes/"));
rec.record(&adapter, &req, || async { real_redis.get("session:42").await }).await?;

let mut rep = Session::new(Mode::Replay, FileCassette::new("cassettes/"));
let resp = rep.record(&adapter, &req, || async { unreachable!() }).await?;
// cassette replays in Go/Python/TS/PHP
```

### Key differences

- redis-test: ordered expectation sequence; xrr: fingerprint-matched cassette lookup
- redis-test: must anticipate command order; xrr: any order, matched by content
- redis-test: Rust-only; xrr cassettes cross-language

---

## SeaORM MockDatabase → xrr (SQL)

SeaORM's mock database is ORM-specific; no recording.

### Before (SeaORM MockDatabase)

```pseudocode
use sea_orm::{MockDatabase, DatabaseBackend};

let db = MockDatabase::new(DatabaseBackend::Postgres)
    .append_query_results([[
        Model { id: 1, name: "Alice".to_string() },
        Model { id: 2, name: "Bob".to_string() },
    ]])
    .into_connection();

let users = Entity::find().all(&db).await?;
// hand-written model instances — SeaORM-only
// no recording; ORM-specific; no cross-language cassette
```

### After (xrr)

```pseudocode
use xrr::adapters::sql::{SqlAdapter, SqlRequest};

let adapter = SqlAdapter::new();
let req = SqlRequest { query: "SELECT id, name FROM users".into(), args: vec![] };

let mut rec = Session::new(Mode::Record, FileCassette::new("cassettes/"));
rec.record(&adapter, &req, || async {
    let rows = sqlx::query("SELECT id, name FROM users").fetch_all(&pool).await?;
    SqlResponse::from_rows(rows)
}).await?;

// Replay — no DB, no SeaORM mock setup
let mut rep = Session::new(Mode::Replay, FileCassette::new("cassettes/"));
let resp = rep.record(&adapter, &req, || async { unreachable!() }).await?;
```

### Key differences

- SeaORM mock: ORM-specific; xrr: works with sqlx, diesel, raw sql — any driver
- SeaORM mock: hand-write model instances; xrr: records real row data
- SeaORM mock: Rust-only; xrr cassettes shared across all xrr ports
