# Replacing Go HTTP/interaction mocking with xrr

Covers: go-vcr · govcr · go-sqlmock · copyist · miniredis · go-redis/redismock

---

## go-vcr → xrr (HTTP)

`go-vcr` records HTTP only; cassettes are Go-specific YAML.

### Before (go-vcr)

```pseudocode
import "github.com/dnaeon/go-vcr/v4/pkg/recorder"

r, _ := recorder.New("fixtures/my-cassette")
defer r.Stop()

client := &http.Client{Transport: r}
resp, _ := client.Get("https://api.example.com/users")
// cassette written to fixtures/my-cassette.yaml (Go-specific format)
// cannot replay in Python or TypeScript
```

### After (xrr)

```pseudocode
import (
    xrr  "hop.top/xrr"
    xhttp "hop.top/xrr/adapters/http"
)

s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette("cassettes/"))
resp, _ := s.Record(ctx, xhttp.NewAdapter(), &xhttp.Request{
    Method: "GET", URL: "https://api.example.com/users",
}, func() (xrr.Response, error) {
    return http.Get("https://api.example.com/users")
})
// cassette written to cassettes/http-<fp>.{req,resp}.yaml
// same cassette replays in Python, TypeScript, PHP, Rust
```

### Key differences

- go-vcr wraps `http.Client`; xrr wraps the call site — works with any HTTP lib
- go-vcr cassettes are Go-specific; xrr cassettes replay in any xrr port
- go-vcr: HTTP only; xrr: HTTP + exec + gRPC + Redis + SQL + fs in one session

---

## copyist → xrr (SQL)

`copyist` records SQL interactions but is Postgres-only and Go-specific.

### Before (copyist)

```pseudocode
import "github.com/cockroachdb/copyist"

copyist.Register("postgres")

func TestMyQuery(t *testing.T) {
    copyist.Open(t)
    defer copyist.Close()

    db, _ := sql.Open("copyist_postgres", "postgres://...")
    rows, _ := db.Query("SELECT id, name FROM users")
    // cassette written in cockroachdb-specific text format
    // Postgres pq/pgx drivers only; no MySQL, SQLite
    // format not readable by Python or other languages
}
```

### After (xrr)

```pseudocode
s := xrr.NewSession(mode, xrr.NewFileCassette("cassettes/"))
resp, _ := s.Record(ctx, sql.NewAdapter(), &sql.Request{
    Query: "SELECT id, name FROM users",
}, func() (xrr.Response, error) {
    rows, _ := db.Query("SELECT id, name FROM users")
    return sql.RowsToResponse(rows)
})
// works with any SQL driver; YAML cassette replays in Python, TS, PHP, Rust
```

### Key differences

- copyist: Postgres pq/pgx drivers only; xrr: any SQL driver
- copyist: Go-specific recording format; xrr: language-agnostic YAML
- copyist: must register at package init; xrr: explicit session at call site

---

## go-sqlmock → xrr (SQL)

`go-sqlmock` is expectation-based; no cassette recording.

### Before (go-sqlmock)

```pseudocode
import "github.com/DATA-DOG/go-sqlmock"

db, mock, _ := sqlmock.New()
mock.ExpectQuery("SELECT id, name FROM users").
    WillReturnRows(sqlmock.NewRows([]string{"id","name"}).
        AddRow(1, "Alice").AddRow(2, "Bob"))

// manual expectation setup per test
// no recording of real interactions
// breaks when query changes slightly
```

### After (xrr)

```pseudocode
// Record once against real DB
s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette("cassettes/"))
s.Record(ctx, sql.NewAdapter(), &sql.Request{Query: "SELECT id, name FROM users"}, realQuery)

// Replay in CI — no DB, no manual expectations
s2 := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette("cassettes/"))
resp, _ := s2.Record(ctx, sql.NewAdapter(), &sql.Request{Query: "SELECT id, name FROM users"}, nil)
```

### Key differences

- go-sqlmock: hand-write every expectation; xrr: record once, replay forever
- go-sqlmock: breaks on whitespace/case changes; xrr normalizes queries before fingerprinting
- go-sqlmock: mock-only (no real interaction captured); xrr: records actual DB responses

---

## miniredis → xrr (Redis)

`miniredis` runs a real in-memory Redis server; no cassette persistence.

### Before (miniredis)

```pseudocode
import "github.com/alicebob/miniredis/v2"

s := miniredis.RunT(t)
client := redis.NewClient(&redis.Options{Addr: s.Addr()})

// full in-memory Redis — fast, but:
// no recording of real Redis interactions
// cannot share cassettes with Python consumer of same data
// server startup required per test
```

### After (xrr)

```pseudocode
// Record against real Redis once
s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette("cassettes/"))
s.Record(ctx, xredis.NewAdapter(), &xredis.Request{
    Command: "GET", Args: []string{"session:42"},
}, func() (xrr.Response, error) { return realRedisClient.Get(ctx, "session:42") })

// CI: replay — no Redis server running
s2 := xrr.NewSession(xrr.ModeReplay, xrr.NewFileCassette("cassettes/"))
resp, _ := s2.Record(ctx, xredis.NewAdapter(), req, nil)
```

### Key differences

- miniredis: requires server process (even if in-memory); xrr: zero infrastructure
- miniredis: no cassette persistence; xrr: cassettes committed to VCS
- miniredis: Go-only; xrr cassettes replay in Python/TS/PHP/Rust consumers

---

## go-redis/redismock → xrr (Redis)

`redismock` is expectation-based; no recording.

### Before (go-redis/redismock)

```pseudocode
import "github.com/go-redis/redismock/v9"

db, mock := redismock.NewClientMock()
mock.ExpectGet("session:42").SetVal("user-data")

// manual setup per command; must anticipate every call
// no cassette; no cross-language sharing
```

### After (xrr)

```pseudocode
// same pattern as miniredis → xrr above
// record real Redis interactions; replay from cassette
// cross-language: Python service can replay same cassette
```

---

## grpcmock → xrr (gRPC)

`grpcmock` is expectation-based; no cassette persistence.

### Before (grpcmock)

```pseudocode
import "github.com/nhatthm/grpcmock"

grpcmock.MockUnaryMethod(t, "UserService.GetUser",
    grpcmock.Return(&pb.User{Id: 1, Name: "Alice"}))

// hand-write every expectation; no real interaction captured
// Go-only; no cross-language cassette
```

### After (xrr)

```pseudocode
s := xrr.NewSession(xrr.ModeRecord, xrr.NewFileCassette("cassettes/"))
s.Record(ctx, xgrpc.NewAdapter(), &xgrpc.Request{
    Service: "UserService", Method: "GetUser",
    Message: marshalProto(&pb.GetUserRequest{Id: 1}),
}, func() (xrr.Response, error) {
    return grpcClient.GetUser(ctx, &pb.GetUserRequest{Id: 1})
})
// gRPC is Go-only in xrr; cassette format still cross-language-readable
```
