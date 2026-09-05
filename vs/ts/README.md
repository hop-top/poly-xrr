# Replacing TypeScript/JS HTTP/interaction mocking with xrr

Covers: Polly.JS · nock · msw · fetch-mock · redis-mock

---

## Polly.JS → xrr (HTTP)

Polly.JS records HTTP (Fetch/XHR); archived since 2021, no cross-language cassettes.

### Before (Polly.JS)

```pseudocode
import { Polly } from "@pollyjs/core";
import NodeHTTPAdapter from "@pollyjs/adapter-node-http";
import FSPersister from "@pollyjs/persister-fs";

Polly.register(NodeHTTPAdapter);
Polly.register(FSPersister);

const polly = new Polly("my-cassette", {
    adapters: ["node-http"],
    persister: "fs",
    persisterOptions: { fs: { recordingsDir: "recordings" } },
});

const resp = await fetch("https://api.example.com/users");
await polly.stop();
// cassette written as HAR JSON — JS-specific shape
// no exec, Redis, SQL support
// project archived — no updates since 2021
```

### After (xrr)

```pseudocode
import { Session, Mode, FileCassette } from "@hop-top/xrr";
import { HttpAdapter } from "@hop-top/xrr/adapters/http";

const adapter = new HttpAdapter();
const req = { method: "GET", url: "https://api.example.com/users" };

// Record
const recSession = new Session(Mode.Record, new FileCassette("cassettes/"));
const resp = await recSession.record(adapter, req, () => fetch(req.url));

// Replay — no network
const repSession = new Session(Mode.Replay, new FileCassette("cassettes/"));
const resp2 = await repSession.record(adapter, req, async () => { throw new Error("should not run") });
// cassette replays in Go, Python, PHP, Rust unchanged
```

### Key differences

- Polly.JS: archived (2021); xrr: actively maintained
- Polly.JS: HTTP only; xrr: HTTP + exec + Redis + SQL + fs
- Polly.JS: HAR JSON (JS-specific); xrr: language-agnostic YAML
- Polly.JS: complex plugin registration; xrr: one import, one session

---

## nock → xrr (HTTP)

`nock` intercepts Node.js HTTP globally; expectation-based, no recording by default.

### Before (nock)

```pseudocode
import nock from "nock";

nock("https://api.example.com")
    .get("/users")
    .reply(200, [{ id: 1, name: "Alice" }]);

const resp = await fetch("https://api.example.com/users");
// hand-written reply — must anticipate every field
// no recording of real API responses
// global HTTP interception — side effects bleed across tests
```

### After (xrr)

```pseudocode
// Record real API once — captures full real response
const recSession = new Session(Mode.Record, new FileCassette("cassettes/"));
await recSession.record(adapter, req, () => fetch(url));

// Replay — scoped to session, no global patching
const repSession = new Session(Mode.Replay, new FileCassette("cassettes/"));
const resp = await repSession.record(adapter, req, async () => null);
```

### Key differences

- nock: global HTTP interception (side effects); xrr: scoped per session
- nock: expectation-based (hand-write replies); xrr: record real responses
- nock: JS-only; xrr cassettes cross-language
- nock: breaks silently when real API changes; xrr: re-record to update cassette

---

## msw (Mock Service Worker) → xrr (HTTP)

`msw` intercepts at service worker / Node.js layer; expectation-based, no recording.

### Before (msw)

```pseudocode
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";

const server = setupServer(
    http.get("https://api.example.com/users", () =>
        HttpResponse.json([{ id: 1, name: "Alice" }])
    )
);
beforeAll(() => server.listen());
afterAll(() => server.close());

// manual handler per endpoint; no real interaction captured
// excellent DX but zero recording support
```

### After (xrr)

```pseudocode
// Drop msw handlers; record real endpoint once
const session = new Session(Mode.Record, new FileCassette("cassettes/"));
await session.record(http_adapter, req, () => realFetch(req));

// In tests: replay — same DX, but cassette = real captured data
const repSession = new Session(Mode.Replay, new FileCassette("cassettes/"));
const resp = await repSession.record(http_adapter, req, async () => null);
```

### Key differences

- msw: service worker / Node handler (excellent for browser tests); xrr: Node only
- msw: hand-craft every response; xrr: capture real responses automatically
- msw: no cassette persistence; xrr: cassettes in VCS
- msw: JS-only; xrr cassettes cross-language

> Note: msw excels for browser integration tests. xrr is better for backend/Node
> service tests where cross-language cassette sharing matters.

---

## redis-mock → xrr (Redis)

`redis-mock` is an in-memory Redis mock; no cassette persistence, no recording.

### Before (redis-mock)

```pseudocode
import redisMock from "redis-mock";

const client = redisMock.createClient();
client.set("session:42", "user-data");
client.get("session:42", (err, val) => {
    // val === "user-data"
    // hand-populated mock state
    // no recording of real Redis interactions
    // TS-only; no cross-language sharing
});
```

### After (xrr)

```pseudocode
import { RedisAdapter } from "@hop-top/xrr/adapters/redis";

const adapter = new RedisAdapter();
const req = { command: "GET", args: ["session:42"] };

// Record against real Redis once
const recSession = new Session(Mode.Record, new FileCassette("cassettes/"));
await recSession.record(adapter, req, () => realRedisClient.get("session:42"));

// Replay — no Redis, no mock setup
const repSession = new Session(Mode.Replay, new FileCassette("cassettes/"));
const resp = await repSession.record(adapter, req, async () => null);
// same cassette replays in Go/Python consumer
```

### Key differences

- redis-mock: must pre-populate state; xrr: records real state from live Redis
- redis-mock: JS-only; xrr cassettes shared with Go/Python/PHP/Rust
- redis-mock: no cassette persistence; xrr: cassettes in VCS

---

## No exec recording tool → xrr (exec)

TypeScript has no equivalent for recording shell command interactions.

### Before (common pattern)

```pseudocode
import { jest } from "@jest/globals";
import * as childProcess from "child_process";

jest.spyOn(childProcess, "execSync").mockReturnValue(
    Buffer.from("title: My PR\n")
);
// synthetic output — drift risk when gh changes format
// invisible mock — no cassette in VCS
```

### After (xrr)

```pseudocode
import { ExecAdapter } from "@hop-top/xrr/adapters/exec";
import { spawnSync } from "child_process";

const adapter = new ExecAdapter();
const req = { argv: ["gh", "pr", "view", "42"] };

// Record real gh output once
const recSession = new Session(Mode.Record, new FileCassette("cassettes/"));
await recSession.record(adapter, req, () => {
    const r = spawnSync(req.argv[0], req.argv.slice(1));
    return { stdout: r.stdout.toString(), stderr: r.stderr.toString(), exit_code: r.status };
});

// Replay in CI — gh never called
const repSession = new Session(Mode.Replay, new FileCassette("cassettes/"));
const resp = await repSession.record(adapter, req, async () => null);
```
