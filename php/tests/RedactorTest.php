<?php

declare(strict_types=1);

namespace HopTop\Xrr\Tests;

use HopTop\Xrr\Adapters\ExecAdapter;
use HopTop\Xrr\FileCassette;
use HopTop\Xrr\Mode;
use HopTop\Xrr\Redactor;
use HopTop\Xrr\Session;
use PHPUnit\Framework\TestCase;

class RedactorTest extends TestCase
{
    private const SECRET_TOKEN = 'ghp_supersecrettokenvalue0123456789abcd';

    protected function tearDown(): void
    {
        putenv(Redactor::ENV_REDACT_DISABLE);
        putenv(Redactor::ENV_REDACT_ALLOW);
        putenv(Redactor::ENV_REDACT_DENY);
    }

    private static function tmpDir(): string
    {
        $dir = sys_get_temp_dir() . '/xrr_redact_' . uniqid();
        mkdir($dir);

        return $dir;
    }

    /**
     * Concatenates every file under $dir so a test can assert on
     * "nothing anywhere in the cassette dir contains this string".
     */
    private static function readAll(string $dir): string
    {
        $out   = '';
        $files = glob($dir . '/*') ?: [];
        foreach ($files as $file) {
            $out .= file_get_contents($file) . "\n";
        }

        return $out;
    }

    private static function stripRecordedAt(string $s): string
    {
        $lines = array_filter(
            explode("\n", $s),
            static fn (string $line): bool => !str_starts_with($line, 'recorded_at:')
        );

        return implode("\n", $lines);
    }

    /** @param array<string, mixed> $req */
    private static function record(string $dir, array $req, string $stdout): void
    {
        $sess = new Session(Mode::Record, new FileCassette($dir));
        $sess->record(new ExecAdapter(), $req, static fn (): array => [
            'stdout'    => $stdout,
            'exit_code' => 0,
        ]);
    }

    // --- key-name classification -----------------------------------------

    public function testSecretEnvNames(): void
    {
        $r      = new Redactor();
        $secret = [
            'GITHUB_TOKEN', 'AWS_SECRET_ACCESS_KEY', 'AWS_ACCESS_KEY_ID',
            'API_KEY', 'DB_PASSWORD', 'GOOGLE_CREDENTIALS', 'MY_SECRET',
            'npm_token', 'Stripe_Api_Key', 'SESSION_COOKIE', 'PRIVATE_KEY',
            'AUTH_TOKEN', 'PASSPHRASE', 'SOME_AUTH', 'CLIENT_SECRET',
        ];
        foreach ($secret as $k) {
            $this->assertTrue($r->isSecretKey($k), "expected {$k} to be classified secret");
        }
    }

    public function testBenignEnvNames(): void
    {
        $r      = new Redactor();
        $benign = [
            'PATH', 'HOME', 'LANG', 'PWD', 'SHELL', 'TERM', 'GOPATH',
            'CI', 'NODE_ENV', 'XRR_MODE', 'XRR_CASSETTE_DIR',
            // "key"/"token" as a substring of a non-credential word must not trip.
            'MONKEY_BUSINESS', 'TOKENIZER_MODE', 'KEYBOARD_LAYOUT',
        ];
        foreach ($benign as $k) {
            $this->assertFalse($r->isSecretKey($k), "expected {$k} to be benign");
        }
    }

    public function testSecretHeaderNames(): void
    {
        $r = new Redactor();
        // Header matching must be case-insensitive and dash/underscore agnostic.
        $secret = [
            'Authorization', 'authorization', 'Proxy-Authorization',
            'Cookie', 'Set-Cookie', 'X-Api-Key', 'x-api-key', 'X_API_KEY',
            'X-Auth-Token', 'X-Amz-Security-Token', 'X-CSRF-Token',
        ];
        foreach ($secret as $k) {
            $this->assertTrue($r->isSecretKey($k), "expected header {$k} to be classified secret");
        }
        foreach (['Content-Type', 'Accept', 'User-Agent', 'Content-Length'] as $k) {
            $this->assertFalse($r->isSecretKey($k), "expected header {$k} to be benign");
        }
    }

    // --- placeholder shape ------------------------------------------------

    public function testPlaceholderIsDeterministicAndNamed(): void
    {
        $r   = new Redactor();
        $got = $r->placeholder('Authorization');
        $this->assertSame('<redacted:AUTHORIZATION>', $got);
        // Stable across calls — no counters, no randomness, no hashing of value.
        $this->assertSame($got, $r->placeholder('Authorization'));
        $this->assertSame('<redacted:X-API-KEY>', $r->placeholder('X-Api-Key'));
        $this->assertSame('<redacted:GITHUB_TOKEN>', $r->placeholder('GITHUB_TOKEN'));
    }

    // --- value-pattern matching -------------------------------------------

    public function testSecretValuePatterns(): void
    {
        $r = new Redactor();
        // High-confidence, vendor-prefixed tokens. Name gives no hint here —
        // only the value shape does.
        $secret = [
            'ghp_0123456789abcdefghijklmnopqrstuvwxyz',
            'github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR',
            'AKIAIOSFODNN7EXAMPLE',
            'sk-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij0123456789',
            'xoxb-123456789012-1234567890123-abcdefghijklmnopqrstuvwx',
            '-----BEGIN RSA PRIVATE KEY-----',
            'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefghijklmnop',
        ];
        foreach ($secret as $v) {
            $this->assertTrue($r->isSecretValue($v), "expected value {$v} to match a secret pattern");
        }
    }

    public function testBenignValuesNotMatched(): void
    {
        $r      = new Redactor();
        $benign = [
            '', '/usr/local/bin:/usr/bin', 'en_US.UTF-8', 'true', '1',
            'https://api.example.com/v1/things?page=2',
            'application/json', 'Mozilla/5.0 (Macintosh)',
            'a3f9c1b2', 'hello world',
        ];
        foreach ($benign as $v) {
            $this->assertFalse($r->isSecretValue($v), "expected value {$v} NOT to match a secret pattern");
        }
    }

    // --- escape hatch + custom config -------------------------------------

    public function testAllowListPreservesValue(): void
    {
        $r = new Redactor(allow: ['GITHUB_TOKEN']);
        $this->assertFalse($r->isSecretKey('GITHUB_TOKEN'), 'allow-list must win over default deny');
        // Sibling secrets still redacted.
        $this->assertTrue($r->isSecretKey('AWS_SECRET_ACCESS_KEY'));
    }

    public function testCustomDenyKeys(): void
    {
        $r = new Redactor(deny: ['MY_CUSTOM_FIELD']);
        $this->assertTrue($r->isSecretKey('MY_CUSTOM_FIELD'));
        $this->assertTrue($r->isSecretKey('my_custom_field'), 'custom deny must be case-insensitive');
    }

    public function testDisabled(): void
    {
        $r = new Redactor(disabled: true);
        $this->assertFalse($r->isSecretKey('GITHUB_TOKEN'));
        $this->assertFalse($r->isSecretValue('ghp_0123456789abcdefghijklmnopqrstuvwxyz'));
    }

    public function testAllowListSuppressesValuePatternMatch(): void
    {
        // Allow-listing a key must also suppress value-pattern redaction for
        // that key, otherwise the escape hatch is useless for a var whose
        // value happens to look like a token.
        $r = new Redactor(allow: ['FIXTURE_TOKEN']);
        $this->assertSame(
            'ghp_0123456789abcdefghijklmnopqrstuvwxyz',
            $r->redactField('FIXTURE_TOKEN', 'ghp_0123456789abcdefghijklmnopqrstuvwxyz')
        );
    }

    // --- redactField composition ------------------------------------------

    public function testRedactField(): void
    {
        $r = new Redactor();

        // Secret by key name.
        $this->assertSame('<redacted:GITHUB_TOKEN>', $r->redactField('GITHUB_TOKEN', 'anything'));

        // Secret by value shape, benign key.
        $this->assertSame(
            '<redacted:MY_VAR>',
            $r->redactField('MY_VAR', 'ghp_0123456789abcdefghijklmnopqrstuvwxyz')
        );

        // Benign key + benign value passes through untouched.
        $this->assertSame('/usr/bin', $r->redactField('PATH', '/usr/bin'));

        // Empty value on a secret key: nothing to leak, leave it alone.
        $this->assertSame('', $r->redactField('GITHUB_TOKEN', ''));
    }

    // --- record-side integration ------------------------------------------

    public function testRecordSecretEnvNeverHitsDisk(): void
    {
        $dir = self::tmpDir();
        self::record($dir, [
            'argv' => ['gh', 'pr', 'view', '1'],
            'env'  => [
                'GITHUB_TOKEN' => self::SECRET_TOKEN,
                'PATH'         => '/usr/local/bin:/usr/bin',
            ],
        ], "ok\n");

        $onDisk = self::readAll($dir);
        $this->assertStringNotContainsString(self::SECRET_TOKEN, $onDisk, 'secret env value leaked into cassette');
        $this->assertStringContainsString('<redacted:GITHUB_TOKEN>', $onDisk);
        // Benign env survives — redaction must not nuke useful debugging context.
        $this->assertStringContainsString('/usr/local/bin:/usr/bin', $onDisk);
    }

    public function testRecordValuePatternSecretNeverHitsDisk(): void
    {
        // The env var name gives no hint, only the value shape does.
        $dir = self::tmpDir();
        self::record($dir, [
            'argv' => ['deploy'],
            'env'  => ['DEPLOY_HANDLE' => self::SECRET_TOKEN],
        ], "done\n");

        $onDisk = self::readAll($dir);
        $this->assertStringNotContainsString(self::SECRET_TOKEN, $onDisk);
        $this->assertStringContainsString('<redacted:DEPLOY_HANDLE>', $onDisk);
    }

    public function testRecordRedactionIsFingerprintStable(): void
    {
        // Fingerprints are computed from {argv, stdin} only, so redacting
        // env cannot shift them. If this fails, ports would disagree on
        // cassette filenames.
        $adapter = new ExecAdapter();
        $req     = [
            'argv' => ['gh', 'pr', 'view', '1'],
            'env'  => ['GITHUB_TOKEN' => self::SECRET_TOKEN],
        ];

        $names = [];
        foreach ([self::tmpDir(), self::tmpDir()] as $dir) {
            self::record($dir, $req, "ok\n");
            $files   = glob($dir . '/*') ?: [];
            $names[] = array_map(basename(...), $files);
        }
        $this->assertSame($names[0], $names[1], 're-recording must produce identical cassette filenames');

        $fpDirect = $adapter->fingerprint($req);
        foreach ($names[0] as $name) {
            $this->assertStringContainsString($fpDirect, $name, 'cassette filename must carry the canonical fingerprint');
        }
    }

    public function testRecordByteIdenticalAcrossRuns(): void
    {
        // Placeholders must be stable, not derived from a counter or the
        // secret's hash, or committed cassettes would churn on re-record.
        $adapter = new ExecAdapter();
        $req     = [
            'argv' => ['gh', 'auth', 'status'],
            'env'  => [
                'GITHUB_TOKEN'          => self::SECRET_TOKEN,
                'AWS_SECRET_ACCESS_KEY' => 'abc123',
            ],
        ];

        $run = function () use ($adapter, $req): string {
            $dir = self::tmpDir();
            self::record($dir, $req, "ok\n");
            $fp   = $adapter->fingerprint($req);
            $data = (string) file_get_contents("{$dir}/exec-{$fp}.req.yaml");

            // recorded_at is a timestamp and legitimately varies; drop it.
            return self::stripRecordedAt($data);
        };

        $this->assertSame($run(), $run(), 'redacted cassette bytes must be stable across runs');
    }

    public function testRedactedCassetteStillReplays(): void
    {
        // Redaction must not break the record→replay round trip. The
        // response payload replays intact; redacted request fields are
        // not part of matching.
        $dir = self::tmpDir();
        $req = [
            'argv' => ['gh', 'pr', 'view', '7'],
            'env'  => ['GITHUB_TOKEN' => self::SECRET_TOKEN],
        ];
        self::record($dir, $req, "title: hello\n");

        $rep = new Session(Mode::Replay, new FileCassette($dir));
        $got = $rep->record(new ExecAdapter(), $req, function (): never {
            $this->fail('do() must not be called in replay mode');
        });
        $this->assertIsArray($got);
        $this->assertSame("title: hello\n", $got['stdout']);
    }

    public function testRecordDisabledViaEnv(): void
    {
        putenv(Redactor::ENV_REDACT_DISABLE . '=1');
        $dir = self::tmpDir();
        self::record($dir, [
            'argv' => ['gh'],
            'env'  => ['GITHUB_TOKEN' => self::SECRET_TOKEN],
        ], "ok\n");
        $this->assertStringContainsString(
            self::SECRET_TOKEN,
            self::readAll($dir),
            'disable flag must restore verbatim recording'
        );
    }

    public function testRecordAllowListViaEnv(): void
    {
        putenv(Redactor::ENV_REDACT_ALLOW . '=GITHUB_TOKEN');
        $dir = self::tmpDir();
        self::record($dir, [
            'argv' => ['gh'],
            'env'  => [
                'GITHUB_TOKEN'          => self::SECRET_TOKEN,
                'AWS_SECRET_ACCESS_KEY' => 'leakme',
            ],
        ], "ok\n");

        $onDisk = self::readAll($dir);
        $this->assertStringContainsString(self::SECRET_TOKEN, $onDisk, 'allow-listed key must be preserved');
        $this->assertStringNotContainsString('leakme', $onDisk, 'non-allow-listed secret must still be redacted');
    }

    public function testRecordNestedStructuresPreserved(): void
    {
        $dir = self::tmpDir();
        $c   = new FileCassette($dir);
        $req = [
            'argv'   => ['svc'],
            'config' => [
                'retries'    => 3,
                'verbose'    => true,
                'api_key'    => self::SECRET_TOKEN,
                'endpoint'   => 'https://example.com',
                'nested_map' => ['password' => 'hunter2'],
            ],
        ];
        $c->save('exec', 'aabbccdd', $req, ['stdout' => 'ok']);

        $onDisk = self::readAll($dir);
        $this->assertStringNotContainsString(self::SECRET_TOKEN, $onDisk);
        $this->assertStringNotContainsString('hunter2', $onDisk);
        $this->assertStringContainsString('<redacted:API_KEY>', $onDisk);
        $this->assertStringContainsString('<redacted:PASSWORD>', $onDisk);
        // Non-string scalars keep their YAML type (not quoted into strings).
        $this->assertStringContainsString('retries: 3', $onDisk);
        $this->assertStringContainsString('verbose: true', $onDisk);
        $this->assertStringContainsString('https://example.com', $onDisk);
    }

    public function testExplicitRedactorBypassesEnv(): void
    {
        putenv(Redactor::ENV_REDACT_DISABLE . '=1');
        $dir = self::tmpDir();
        $c   = new FileCassette($dir, new Redactor());
        $c->save('exec', 'aabbccdd', ['env' => ['GITHUB_TOKEN' => self::SECRET_TOKEN]], ['stdout' => 'ok']);

        $onDisk = self::readAll($dir);
        $this->assertStringNotContainsString(self::SECRET_TOKEN, $onDisk);
        $this->assertStringContainsString('<redacted:GITHUB_TOKEN>', $onDisk);
    }
}
