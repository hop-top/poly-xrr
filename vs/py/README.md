# Replacing Python HTTP/interaction mocking with xrr

Covers: vcrpy · pytest-recording · fakeredis · responses · httpretty

---

## vcrpy → xrr (HTTP)

`vcrpy` records HTTP only; cassettes are Python-specific YAML.

### Before (vcrpy)

```pseudocode
import vcr

@vcr.use_cassette("fixtures/my-cassette.yaml")
def test_get_users():
    resp = requests.get("https://api.example.com/users")
    assert resp.status_code == 200
    # cassette written in vcrpy YAML format
    # format is Python-specific; cannot replay in Go or TypeScript
    # HTTP only — exec, Redis, SQL calls not captured
```

### After (xrr)

```pseudocode
from xrr import Session, Mode, FileCassette
from xrr.adapters.http import HttpAdapter, HttpRequest

def test_get_users(tmp_path):
    adapter = HttpAdapter()
    req = HttpRequest(method="GET", url="https://api.example.com/users")

    # Record
    with Session(Mode.RECORD, FileCassette(tmp_path)) as s:
        resp = s.record(adapter, req, lambda: real_http_get(req))

    # Replay — no network
    with Session(Mode.REPLAY, FileCassette(tmp_path)) as s:
        resp = s.record(adapter, req, lambda: None)
    assert resp.status == 200
    # cassette replays in Go, TypeScript, PHP, Rust unchanged
```

### Key differences

- vcrpy: decorator-based, patches `requests`/`httpx` globally; xrr: explicit call-site wrapping
- vcrpy: Python-specific cassette format; xrr: language-agnostic YAML
- vcrpy: HTTP only; xrr: HTTP + exec + Redis + SQL + fs in one session
- vcrpy: `@vcr.use_cassette` auto-records on first run; xrr: explicit `Mode.RECORD` / `Mode.REPLAY`

---

## pytest-recording → xrr (HTTP)

`pytest-recording` is a thin vcrpy wrapper; same limitations apply.

### Before (pytest-recording)

```pseudocode
# pytest.ini: --record-mode=once
import pytest

@pytest.mark.vcr
def test_get_users():
    resp = requests.get("https://api.example.com/users")
    # cassette auto-named from test function
    # vcrpy YAML format; Python-specific
```

### After (xrr)

```pseudocode
# No magic decorator — explicit session
def test_get_users(tmp_path):
    with Session(Mode.REPLAY, FileCassette("cassettes/")) as s:
        resp = s.record(http_adapter, req, real_call)
    assert resp.status == 200

# Control record/replay via XRR_MODE env var in CI
# XRR_MODE=replay pytest tests/
```

### Key differences

- pytest-recording: magic `@pytest.mark.vcr` + pytest plugin; xrr: no plugin required
- pytest-recording: cassette path auto-derived from test name; xrr: explicit cassette dir
- Both commit cassettes to VCS; xrr cassettes replay across languages

---

## fakeredis → xrr (Redis)

`fakeredis` runs a pure-Python in-memory Redis; no cassette persistence.

### Before (fakeredis)

```pseudocode
import fakeredis

r = fakeredis.FakeRedis()
r.set("session:42", "user-data")
val = r.get("session:42")
# full in-memory Redis — no recording of real interactions
# Python-only; Go consumer of same data cannot share state
# no persistent cassette; must re-setup every test
```

### After (xrr)

```pseudocode
from xrr.adapters.redis import RedisAdapter, RedisRequest

adapter = RedisAdapter()

# Record against real Redis once
with Session(Mode.RECORD, FileCassette("cassettes/")) as s:
    resp = s.record(adapter,
        RedisRequest(command="GET", args=["session:42"]),
        lambda: real_redis.get("session:42"))

# CI: replay — no Redis server, no fakeredis import
with Session(Mode.REPLAY, FileCassette("cassettes/")) as s:
    resp = s.record(adapter,
        RedisRequest(command="GET", args=["session:42"]),
        None)
```

### Key differences

- fakeredis: must pre-populate state manually; xrr: records real state from live Redis
- fakeredis: Python-only; xrr cassettes shared with Go/TS/PHP/Rust
- fakeredis: no cassette persistence; xrr: cassettes committed to VCS, reproducible

---

## responses / httpretty → xrr (HTTP mocking)

`responses` and `httpretty` are expectation-based HTTP mocks; no recording.

### Before (responses)

```pseudocode
import responses as rsps

@rsps.activate
def test_api():
    rsps.add(rsps.GET, "https://api.example.com/users",
             json={"users": []}, status=200)
    resp = requests.get("https://api.example.com/users")
    # hand-written mock; must anticipate every field
    # breaks when real API adds new fields
    # no cross-language sharing
```

### After (xrr)

```pseudocode
# Record real API response once — captures every field automatically
with Session(Mode.RECORD, FileCassette("cassettes/")) as s:
    resp = s.record(http_adapter, req, lambda: requests.get(url))

# Replay captures real shape; no hand-maintenance of mock fields
with Session(Mode.REPLAY, FileCassette("cassettes/")) as s:
    resp = s.record(http_adapter, req, None)
```

### Key differences

- responses/httpretty: hand-write every mock field; xrr: capture real response automatically
- responses/httpretty: mock breaks when real API changes shape; xrr: re-record to update
- responses/httpretty: Python-only; xrr cassettes cross-language
- xrr: cassettes in VCS = explicit, reviewable contract; mocks are invisible in test code

---

## No exec recording tool → xrr (exec)

Python has no OSS equivalent for recording shell command interactions.

### Before (common pattern)

```pseudocode
from unittest.mock import patch, MagicMock

with patch("subprocess.run") as mock_run:
    mock_run.return_value = MagicMock(
        stdout="title: My PR\n", stderr="", returncode=0)
    result = run_gh_command(["gh", "pr", "view", "42"])
    # hand-written mock — drift risk when gh output format changes
    # no real interaction captured
```

### After (xrr)

```pseudocode
from xrr.adapters.exec import ExecAdapter, ExecRequest

adapter = ExecAdapter()
req = ExecRequest(argv=["gh", "pr", "view", "42"])

# Record real gh output once
with Session(Mode.RECORD, FileCassette("cassettes/")) as s:
    resp = s.record(adapter, req, lambda: subprocess.run(req.argv, capture_output=True))

# Replay — gh never called; real output preserved in cassette
with Session(Mode.REPLAY, FileCassette("cassettes/")) as s:
    resp = s.record(adapter, req, None)
assert "My PR" in resp.stdout
```

### Key differences

- patch/MagicMock: synthetic output; xrr: real captured output
- patch/MagicMock: invisible to reviewers (inside test); xrr: cassette in VCS is explicit
- xrr: same cassette replays in Go evaluator or TS integration test
