<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Exception\MalformedStreamException;
use HopTop\Xrr\Exception\ShapeMismatchException;
use HopTop\Xrr\Stream\StreamParser;
use HopTop\Xrr\Stream\StreamType;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Yaml\Yaml;

class StreamParserTest extends TestCase
{
    /** @return array<string, mixed> */
    private function reqEnv(): array
    {
        return [
            'xrr'         => '1',
            'adapter'     => 'grpc',
            'fingerprint' => '58a4bf3f',
            'recorded_at' => '2026-08-23T12:00:00Z',
            'payload'     => ['service' => 'files.FileService', 'method' => 'Download'],
            'stream'      => [
                'type'   => 'server',
                'frames' => [
                    ['seq' => 0, 'message_b64' => 'eyJwYXRoIjoiL2V0Yy9ob3N0cyJ9', 'at_ms' => 0],
                ],
                'half_close' => ['seq' => 1, 'at_ms' => 0],
            ],
        ];
    }

    /** @return array<string, mixed> */
    private function respEnv(): array
    {
        return [
            'xrr'         => '1',
            'adapter'     => 'grpc',
            'fingerprint' => '58a4bf3f',
            'recorded_at' => '2026-08-23T12:00:00Z',
            'payload'     => ['status_code' => 0],
            'stream'      => [
                'frames' => [
                    ['seq' => 2, 'message_b64' => 'Y2h1bmstb25lCg==', 'at_ms' => 12],
                    ['seq' => 3, 'message_text' => 'chunk-two', 'at_ms' => 15],
                ],
                'end' => ['seq' => 4, 'at_ms' => 21],
            ],
        ];
    }

    public function testParsesValidServerStreamPair(): void
    {
        $pair = (new StreamParser())->parsePair($this->reqEnv(), $this->respEnv());

        $this->assertSame('grpc', $pair->adapter);
        $this->assertSame('58a4bf3f', $pair->fingerprint);
        $this->assertNull($pair->error);
        $this->assertSame(StreamType::Server, $pair->req->type);

        $this->assertCount(1, $pair->req->frames);
        $this->assertSame(0, $pair->req->frames[0]->seq);
        $this->assertSame('{"path":"/etc/hosts"}', $pair->req->frames[0]->bytes);
        $this->assertSame(0, $pair->req->frames[0]->atMs);

        $this->assertNotNull($pair->req->halfClose);
        $this->assertSame(1, $pair->req->halfClose->seq);

        $this->assertCount(2, $pair->resp->frames);
        $this->assertSame("chunk-one\n", $pair->resp->frames[0]->bytes);
        $this->assertSame('chunk-two', $pair->resp->frames[1]->bytes);
        $this->assertSame(4, $pair->resp->end->seq);
        $this->assertSame(21, $pair->resp->end->atMs);
    }

    public function testRecordedErrorIsExposed(): void
    {
        $resp          = $this->respEnv();
        $resp['error'] = 'rpc error: code = Unavailable desc = connection reset';

        $pair = (new StreamParser())->parsePair($this->reqEnv(), $resp);
        $this->assertSame('rpc error: code = Unavailable desc = connection reset', $pair->error);
    }

    public function testUnaryPairIsShapeMismatch(): void
    {
        $req  = $this->reqEnv();
        $resp = $this->respEnv();
        unset($req['stream'], $resp['stream']);

        $this->expectException(ShapeMismatchException::class);
        (new StreamParser())->parsePair($req, $resp);
    }

    public function testOneSidedStreamRejected(): void
    {
        $resp = $this->respEnv();
        unset($resp['stream']);

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testMissingTypeRejected(): void
    {
        $req = $this->reqEnv();
        unset($req['stream']['type']);

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($req, $this->respEnv());
    }

    public function testInvalidTypeRejected(): void
    {
        $req                   = $this->reqEnv();
        $req['stream']['type'] = 'duplex';

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($req, $this->respEnv());
    }

    public function testFrameWithoutSeqRejected(): void
    {
        $resp = $this->respEnv();
        unset($resp['stream']['frames'][0]['seq']);

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testFrameWithBothEncodingsRejected(): void
    {
        $resp = $this->respEnv();

        $resp['stream']['frames'][0]['message_text'] = 'chunk-one';

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testFrameWithNeitherEncodingRejected(): void
    {
        $resp = $this->respEnv();
        unset($resp['stream']['frames'][0]['message_b64']);

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testNonAscendingFramesRejected(): void
    {
        $resp = $this->respEnv();

        $resp['stream']['frames'][0]['seq'] = 3;
        $resp['stream']['frames'][1]['seq'] = 2;

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testEqualSeqInOneListRejected(): void
    {
        $resp = $this->respEnv();

        $resp['stream']['frames'][1]['seq'] = 2;

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testDuplicateSeqAcrossPairRejected(): void
    {
        $req = $this->reqEnv();

        $req['stream']['half_close']['seq'] = 2; // collides with resp frame seq 2

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($req, $this->respEnv());
    }

    public function testMissingEndRejected(): void
    {
        $resp = $this->respEnv();
        unset($resp['stream']['end']);

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testNonMaximalEndSeqRejected(): void
    {
        $req  = $this->reqEnv();
        $resp = $this->respEnv();

        // Still dense 0..4, but half_close (4) now outranks end (1).
        $req['stream']['half_close']['seq'] = 4;
        $resp['stream']['end']['seq']       = 1;

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($req, $resp);
    }

    public function testSparseSeqNumberingRejected(): void
    {
        $resp = $this->respEnv();

        // Events 0,1 (req) + 3,4,6 (resp) — gaps at 2 and 5.
        $resp['stream']['frames'][0]['seq'] = 3;
        $resp['stream']['frames'][1]['seq'] = 4;
        $resp['stream']['end']['seq']       = 6;

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testNegativeSeqRejected(): void
    {
        $req = $this->reqEnv();

        $req['stream']['frames'][0]['seq'] = -1;

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($req, $this->respEnv());
    }

    public function testInvalidBase64CharacterRejected(): void
    {
        $resp = $this->respEnv();

        $resp['stream']['frames'][0]['message_b64'] = 'YmxvYi1jaHVuayEh!';

        $this->expectException(MalformedStreamException::class);
        $this->expectExceptionMessage('base64');
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testBase64EmbeddedWhitespaceRejected(): void
    {
        $resp = $this->respEnv();

        // base64_decode($s, true) tolerates this; the parser must not.
        $resp['stream']['frames'][0]['message_b64'] = 'YmxvYi1jaHVu ayAy';

        $this->expectException(MalformedStreamException::class);
        $this->expectExceptionMessage('base64');
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testBase64BadPaddingRejected(): void
    {
        $resp = $this->respEnv();

        $resp['stream']['frames'][0]['message_b64'] = 'YWJjZA=';

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testEmptyBase64IsEmptyMessage(): void
    {
        $resp = $this->respEnv();

        $resp['stream']['frames'][0]['message_b64'] = '';

        $pair = (new StreamParser())->parsePair($this->reqEnv(), $resp);
        $this->assertSame('', $pair->resp->frames[0]->bytes);
    }

    public function testNonStringMessageTextRejected(): void
    {
        $resp = $this->respEnv();

        // What a YAML 1.1 reader would make of an unquoted `null` scalar.
        $resp['stream']['frames'][1]['message_text'] = null;

        $this->expectException(MalformedStreamException::class);
        (new StreamParser())->parsePair($this->reqEnv(), $resp);
    }

    public function testAbsentFramesKeyTreatedAsEmpty(): void
    {
        $req = $this->reqEnv();

        $req['payload']              = ['service' => 'chat.ChatService', 'method' => 'Ping', 'n' => 0];
        $req['stream']               = ['type' => 'bidi', 'half_close' => ['seq' => 0]];
        $resp                        = $this->respEnv();
        $resp['stream']              = ['end' => ['seq' => 1]];

        $pair = (new StreamParser())->parsePair($req, $resp);
        $this->assertSame([], $pair->req->frames);
        $this->assertSame([], $pair->resp->frames);
        $this->assertNull($pair->resp->end->atMs);
    }

    public function testAbsentAtMsTolerated(): void
    {
        $req = $this->reqEnv();
        unset($req['stream']['frames'][0]['at_ms'], $req['stream']['half_close']['at_ms']);

        $pair = (new StreamParser())->parsePair($req, $this->respEnv());
        $this->assertNull($pair->req->frames[0]->atMs);
        $this->assertNotNull($pair->req->halfClose);
        $this->assertNull($pair->req->halfClose->atMs);
    }

    public function testUnknownExtraFieldsIgnored(): void
    {
        $req = $this->reqEnv();

        $req['stream']['future']                = 'field';
        $req['stream']['frames'][0]['event_id'] = 'sse-extra';
        $req['stream']['half_close']['note']    = 'ignored';

        $pair = (new StreamParser())->parsePair($req, $this->respEnv());
        $this->assertSame(0, $pair->req->frames[0]->seq);
    }

    public function testScalarHazardsSurviveYamlRoundTrip(): void
    {
        $reqYaml = <<<'YAML'
            xrr: "1"
            adapter: sse
            fingerprint: "66ecc77a"
            recorded_at: "2026-08-23T12:00:00Z"
            payload:
              url: "https://example.test/events"
            stream:
              type: server
              frames: []
            YAML;
        $respYaml = <<<'YAML'
            xrr: "1"
            adapter: sse
            fingerprint: "66ecc77a"
            recorded_at: "2026-08-23T12:00:00Z"
            payload: {}
            stream:
              frames:
                - seq: 0
                  message_text: "on"
                - seq: 1
                  message_text: "12:30"
                - seq: 2
                  message_text: "null"
                - seq: 3
                  message_text: " leading"
              end:
                seq: 4
            YAML;

        /** @var array<string, mixed> $req */
        $req = Yaml::parse($reqYaml);
        /** @var array<string, mixed> $resp */
        $resp = Yaml::parse($respYaml);

        $pair  = (new StreamParser())->parsePair($req, $resp);
        $bytes = array_map(fn($f) => $f->bytes, $pair->resp->frames);
        $this->assertSame(['on', '12:30', 'null', ' leading'], $bytes);
    }
}
