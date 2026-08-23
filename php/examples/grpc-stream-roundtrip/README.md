# gRPC streaming round-trip

Records streamed RPCs against a **live** gRPC server, then replays them with
the server **stopped** and asserts the client sees byte-identical output.

This is the extension-dependent half of the adapter's verification: the
PHPUnit suite covers both directions without `ext-grpc` (the record path
against a scripted batch double, the replay path against the spec
fixtures), while this harness proves the seam works against real wire
traffic.

## What it covers

Server-streaming, client-streaming (twice, exercising the `n=0` / `n=1`
occurrence counter), bidi, an empty server stream, and a mid-stream
`UNAVAILABLE` error.

## Requirements

- `ext-grpc` loaded (recording drives real batches).
- `protoc` with `grpc_php_plugin`, and Go with `protoc-gen-go` /
  `protoc-gen-go-grpc` for the reference server.

## Run

```bash
# 1. generate stubs
protoc --proto_path=. --php_out=php-gen --grpc_out=php-gen \
  --plugin=protoc-gen-grpc=$(which grpc_php_plugin) xrrtest.proto
protoc --proto_path=. --go_out=pb --go_opt=paths=source_relative \
  --go-grpc_out=pb --go-grpc_opt=paths=source_relative xrrtest.proto

# 2. start the server; it prints the port it bound
go run server.go 127.0.0.1:0

# 3. record against it, then stop it and replay
php roundtrip.php record  ./cassettes "$PORT" > record.txt
kill "$SERVER_PID"
php roundtrip.php replay  ./cassettes "$PORT" > replay.txt

diff record.txt replay.txt   # must be empty
```

Replay reads only the cassettes: point it at an unroutable address
(`203.0.113.1:9999`) and it still returns instantly with the recorded
conversation, because the replaying call objects never open a channel.
