<?php

declare(strict_types=1);

/**
 * gRPC streaming round-trip harness for the xrr PHP adapter.
 *
 * Phase 1 (record): drives a LIVE gRPC server through the adapter, writing
 * cassettes.
 * Phase 2 (replay): the server is already STOPPED; the same client code runs
 * against cassettes only. Any connection attempt would fail loudly, because
 * the target port is dead.
 *
 * Transcripts from both phases are printed and compared byte-for-byte by the
 * caller.
 *
 * usage: php roundtrip.php <record|replay> <cassette-dir> <port>
 */

require __DIR__ . '/vendor/autoload.php'; // generated stubs + xrr + grpc/grpc

use HopTop\Xrr\Adapters\Grpc\XrrCallInvoker;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Session;
use Xrrtest\Chunk;
use Xrrtest\StreamServiceClient;

[$script, $phase, $dir, $port] = $argv + [null, null, null, null];

$session = new Session(
    $phase === 'record' ? Mode::Record : Mode::Replay,
    new FileCassette($dir)
);

$client = new StreamServiceClient("127.0.0.1:$port", [
    'credentials'       => \Grpc\ChannelCredentials::createInsecure(),
    'grpc_call_invoker' => new XrrCallInvoker($session),
]);

$out = [];
$say = function (string $line) use (&$out): void {
    $out[] = $line;
};

// ── server streaming ──────────────────────────────────────────────────
$call = $client->Download(new Chunk(['value' => 'file.txt']));
foreach ($call->responses() as $chunk) {
    $say('server recv: ' . $chunk->getValue());
}
$say('server status: ' . $call->getStatus()->code);

// ── server streaming, empty ───────────────────────────────────────────
$call = $client->Download(new Chunk(['value' => 'empty']));
$n = 0;
foreach ($call->responses() as $chunk) {
    $n++;
}
$say("server-empty frames: $n");
$say('server-empty status: ' . $call->getStatus()->code);

// ── server streaming, mid-stream error ────────────────────────────────
$call = $client->Download(new Chunk(['value' => 'boom']));
foreach ($call->responses() as $chunk) {
    $say('error-stream recv: ' . $chunk->getValue());
}
$status = $call->getStatus();
$say('error-stream status: ' . $status->code . ' / ' . $status->details);

// ── client streaming ──────────────────────────────────────────────────
$call = $client->Upload();
$call->write(new Chunk(['value' => 'part-one']));
$call->write(new Chunk(['value' => 'part-two']));
[$summary, $status] = $call->wait();
$say('client recv count: ' . $summary->getCount());
$say('client status: ' . $status->code);

// ── client streaming, second occurrence (n=1) ─────────────────────────
$call = $client->Upload();
$call->write(new Chunk(['value' => 'solo']));
[$summary, $status] = $call->wait();
$say('client-repeat recv count: ' . $summary->getCount());
$say('client-repeat status: ' . $status->code);

// ── bidi ──────────────────────────────────────────────────────────────
$call = $client->Converse();
foreach (['ping-1', 'ping-2'] as $msg) {
    $call->write(new Chunk(['value' => $msg]));
    $reply = $call->read();
    $say('bidi recv: ' . $reply->getValue());
}
$call->writesDone();
$say('bidi status: ' . $call->getStatus()->code);

echo implode("\n", $out), "\n";
