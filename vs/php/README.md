# Replacing PHP HTTP/interaction mocking with xrr

Covers: php-vcr · Guzzle MockHandler · M6Web/RedisMock

---

## php-vcr → xrr (HTTP)

`php-vcr` records HTTP only; cassettes are PHP-specific, minimal maintenance since 2023.

### Before (php-vcr)

```pseudocode
use VCR\VCR;

VCR::turnOn();
VCR::insertCassette("fixtures/my-cassette.yml");

$response = file_get_contents("https://api.example.com/users");
// cassette written as PHP-specific YAML
// HTTP only — no exec, Redis, SQL
// cassette format cannot replay in Go or Python
// last release: June 2023; maintenance is minimal

VCR::eject();
VCR::turnOff();
```

### After (xrr)

```pseudocode
use HopTop\Xrr\{Session, Mode, FileCassette};
use HopTop\Xrr\Adapters\Http\{HttpAdapter, HttpRequest};

$adapter = new HttpAdapter();
$req = new HttpRequest(method: "GET", url: "https://api.example.com/users");

// Record
$session = new Session(Mode::Record, new FileCassette(sys_get_temp_dir() . "/cassettes"));
$resp = $session->record($adapter, $req, fn() => realHttpGet($req));

// Replay — no network
$session2 = new Session(Mode::Replay, new FileCassette(sys_get_temp_dir() . "/cassettes"));
$resp2 = $session2->record($adapter, $req, fn() => null);
// cassette replays in Go, Python, TypeScript, Rust unchanged
```

### Key differences

- php-vcr: stream wrapper + global interception; xrr: explicit call-site wrapping
- php-vcr: PHP-specific cassette format; xrr: language-agnostic YAML
- php-vcr: HTTP only; xrr: HTTP + exec + Redis + SQL + fs
- php-vcr: minimal maintenance; xrr: actively maintained across 5 languages

---

## Guzzle MockHandler → xrr (HTTP)

Guzzle's `MockHandler` is expectation-based; no recording, no cross-language sharing.

### Before (Guzzle MockHandler)

```pseudocode
use GuzzleHttp\{Client, Handler\MockHandler, HandlerStack};
use GuzzleHttp\Psr7\Response;

$mock = new MockHandler([
    new Response(200, [], json_encode(["users" => [["id" => 1, "name" => "Alice"]]])),
]);
$stack = HandlerStack::create($mock);
$client = new Client(["handler" => $stack]);

$response = $client->get("https://api.example.com/users");
// hand-written mock response — must anticipate every field
// breaks when real API adds/removes fields
// Guzzle-specific; no cross-language cassette
```

### After (xrr)

```pseudocode
// Record real API response once — captures full shape
$recSession = new Session(Mode::Record, new FileCassette("cassettes/"));
$resp = $recSession->record($adapter, $req, fn() => $guzzleClient->get($url));

// Replay — no Guzzle, no MockHandler setup, no hand-written fields
$repSession = new Session(Mode::Replay, new FileCassette("cassettes/"));
$resp2 = $repSession->record($adapter, $req, fn() => null);
```

### Key differences

- MockHandler: hand-craft every response field; xrr: capture real response automatically
- MockHandler: PHP/Guzzle-specific; xrr cassettes cross-language
- MockHandler: invisible mock in test code; xrr: cassette in VCS is explicit + reviewable
- xrr: re-record to update cassette when API changes; MockHandler: edit every field manually

---

## M6Web/RedisMock → xrr (Redis)

`M6Web/RedisMock` is expectation-based; no recording, Predis-only.

### Before (M6Web/RedisMock)

```pseudocode
use M6Web\Component\RedisMock\RedisMockFactory;

$factory = new RedisMockFactory();
$redisMock = $factory->getAdapter("Predis\Client", true);
$redisMock->set("session:42", "user-data");
$val = $redisMock->get("session:42");
// hand-populated state; no recording of real Redis
// Predis client only; no phpredis, no Symfony Redis
// PHP-only; no cross-language cassette
```

### After (xrr)

```pseudocode
use HopTop\Xrr\Adapters\Redis\{RedisAdapter, RedisRequest};

$adapter = new RedisAdapter();
$req = new RedisRequest(command: "GET", args: ["session:42"]);

// Record against real Redis once
$recSession = new Session(Mode::Record, new FileCassette("cassettes/"));
$resp = $recSession->record($adapter, $req, fn() => $redis->get("session:42"));

// CI: replay — no Redis, no Predis, no mock setup
$repSession = new Session(Mode::Replay, new FileCassette("cassettes/"));
$resp2 = $repSession->record($adapter, $req, fn() => null);
// same cassette replays in Go/Python/TS/Rust
```

### Key differences

- RedisMock: Predis-only; xrr: any Redis client
- RedisMock: must pre-populate state; xrr: records real state from live Redis
- RedisMock: PHP-only; xrr cassettes shared across all xrr ports
- RedisMock: no cassette persistence; xrr: cassettes in VCS

---

## No exec / SQL recording tool → xrr

PHP has no OSS equivalent for recording exec or SQL interactions.

### Before (exec — common pattern)

```pseudocode
// PHPUnit mock — hand-written
$commandRunner = $this->createMock(CommandRunner::class);
$commandRunner->method("run")
    ->with(["gh", "pr", "view", "42"])
    ->willReturn(["stdout" => "title: My PR\n", "exit_code" => 0]);
// synthetic output; drift risk; invisible to reviewers
```

### After (xrr — exec)

```pseudocode
use HopTop\Xrr\Adapters\Exec\{ExecAdapter, ExecRequest};

$adapter = new ExecAdapter();
$req = new ExecRequest(argv: ["gh", "pr", "view", "42"]);

$recSession = new Session(Mode::Record, new FileCassette("cassettes/"));
$resp = $recSession->record($adapter, $req, function () use ($req) {
    $proc = proc_open($req->argv, [1 => ["pipe","w"], 2 => ["pipe","w"]], $pipes);
    return new ExecResponse(stdout: stream_get_contents($pipes[1]), exit_code: proc_close($proc));
});

// Replay in CI — gh never called; real output in cassette
$repSession = new Session(Mode::Replay, new FileCassette("cassettes/"));
$resp2 = $repSession->record($adapter, $req, fn() => null);
```

### Before (SQL — common pattern)

```pseudocode
// PDO mock via PHPUnit
$pdoMock = $this->createMock(PDO::class);
$stmtMock = $this->createMock(PDOStatement::class);
$stmtMock->method("fetchAll")->willReturn([["id" => 1, "name" => "Alice"]]);
$pdoMock->method("query")->willReturn($stmtMock);
// hand-written per query; breaks when schema changes
```

### After (xrr — SQL)

```pseudocode
use HopTop\Xrr\Adapters\Sql\{SqlAdapter, SqlRequest};

$adapter = new SqlAdapter();
$req = new SqlRequest(query: "SELECT id, name FROM users");

$recSession = new Session(Mode::Record, new FileCassette("cassettes/"));
$resp = $recSession->record($adapter, $req, fn() => SqlResponse::fromPdo($pdo, $req->query));

// Replay — no DB, no PDO mock boilerplate
$repSession = new Session(Mode::Replay, new FileCassette("cassettes/"));
$resp2 = $repSession->record($adapter, $req, fn() => null);
```
