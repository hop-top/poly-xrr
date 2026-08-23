<?php

declare(strict_types=1);

namespace HopTop\Xrr;

/**
 * Record-side secret redaction — see spec/cassette-format-v1.md.
 *
 * Redaction happens before serialization, so a secret never reaches
 * disk. Placeholders derive only from the field name, keeping
 * re-recording byte-identical. Fingerprints are computed from the live
 * request before writing, so redaction can never shift a cassette key.
 */
final class Redactor
{
    /**
     * Env vars controlling redaction. Redaction is ON by default; these
     * only exist to widen, narrow, or switch it off.
     */
    public const ENV_REDACT_DISABLE = 'XRR_REDACT_DISABLE';
    public const ENV_REDACT_ALLOW   = 'XRR_REDACT_ALLOW';
    public const ENV_REDACT_DENY    = 'XRR_REDACT_DENY';

    private const REDACTED_PREFIX = '<redacted:';
    private const REDACTED_SUFFIX = '>';

    /**
     * Matched against the *normalized* field name (uppercased, dashes →
     * underscores) as underscore-delimited words, so MONKEY_BUSINESS
     * does not trip on "KEY" and TOKENIZER_MODE does not trip on "TOKEN".
     */
    private const SECRET_KEY_WORDS = [
        'TOKEN', 'SECRET', 'PASSWORD', 'PASSWD', 'PASSPHRASE', 'CREDENTIAL',
        'CREDENTIALS', 'APIKEY', 'KEY', 'AUTH', 'AUTHORIZATION', 'COOKIE',
        'SESSION', 'SIGNATURE', 'PRIVATE', 'ACCESS', 'BEARER', 'OTP',
    ];

    /**
     * Names that are credential-bearing as a whole but whose words are
     * too generic to blanket-match.
     */
    private const SECRET_KEY_EXACT = [
        'AUTHORIZATION', 'PROXY_AUTHORIZATION', 'COOKIE', 'SET_COOKIE',
        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
    ];

    /** Whole namespaces credential-adjacent enough to redact wholesale. */
    private const SECRET_KEY_PREFIXES = ['AWS_'];

    /**
     * Never redacted by name: well-known, non-credential variables whose
     * values carry real debugging signal.
     */
    private const BENIGN_KEYS = [
        'XRR_MODE', 'XRR_CASSETTE_DIR', 'XRR_REDACT_ALLOW', 'XRR_REDACT_DENY',
        'XRR_REDACT_DISABLE', 'AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_PROFILE',
        'SSH_AUTH_SOCK', 'KEYBOARD_LAYOUT', 'ACCESS_LOG', 'ACCESS_LOG_FORMAT',
        'PRIVATE_NETWORK', 'SESSION_MANAGER', 'GPG_TTY', 'AUTHORIZED_KEYS_FILE',
    ];

    /**
     * High-confidence, vendor-prefixed credential shapes. Deliberately
     * narrow: a false positive silently corrupts a cassette, so generic
     * "long random-looking string" heuristics are NOT used — they would
     * redact commit SHAs, UUIDs, and base64 payloads.
     */
    private const SECRET_VALUE_PATTERNS = [
        // GitHub: ghp_/gho_/ghu_/ghs_/ghr_ + 36+ chars, and fine-grained PATs.
        '#\bgh[pousr]_[A-Za-z0-9]{20,}\b#',
        '#\bgithub_pat_[A-Za-z0-9_]{20,}\b#',
        // AWS access key IDs.
        '#\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b#',
        // OpenAI / Anthropic style.
        '#\bsk-[A-Za-z0-9_-]{20,}\b#',
        // Slack.
        '#\bxox[abposr]-[A-Za-z0-9-]{10,}\b#',
        // Google API keys.
        '#\bAIza[A-Za-z0-9_-]{35}\b#',
        // Stripe.
        '#\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b#',
        // PEM private key blocks.
        '#-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----#',
        // JWTs: header starts with the standard `{"alg"` prefix as eyJhbGci.
        '#\beyJhbGci[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+#',
        // Bearer/Basic credentials embedded in a header value.
        '#\b(?:bearer|basic)\s+[A-Za-z0-9+/._~-]{16,}={0,2}#i',
    ];

    /** @var array<string, true> */
    private array $allow = [];

    /** @var array<string, true> */
    private array $deny = [];

    /**
     * No-arg construction yields the secure defaults: name-based
     * matching over the built-in word list plus value-pattern matching
     * over the built-in vendor patterns.
     *
     * @param list<string> $allow field names preserved verbatim (wins over everything)
     * @param list<string> $deny  extra field names to always redact
     */
    public function __construct(
        private bool $disabled = false,
        array $allow = [],
        array $deny = []
    ) {
        foreach ($allow as $k) {
            $this->allow[self::normalizeKey($k)] = true;
        }
        foreach ($deny as $k) {
            $this->deny[self::normalizeKey($k)] = true;
        }
    }

    /**
     * Build a Redactor from the XRR_REDACT_* env vars. Unset vars leave
     * the secure defaults in place.
     */
    public static function fromEnv(): self
    {
        return new self(
            self::truthy(self::env(self::ENV_REDACT_DISABLE)),
            self::splitList(self::env(self::ENV_REDACT_ALLOW)),
            self::splitList(self::env(self::ENV_REDACT_DENY))
        );
    }

    /** Reports whether a field name looks credential-bearing. */
    public function isSecretKey(string $name): bool
    {
        if ($this->disabled) {
            return false;
        }

        $n = self::normalizeKey($name);

        if (isset($this->allow[$n])) {
            return false;
        }
        if (isset($this->deny[$n])) {
            return true;
        }
        if (in_array($n, self::BENIGN_KEYS, true)) {
            return false;
        }
        if (in_array($n, self::SECRET_KEY_EXACT, true)) {
            return true;
        }
        foreach (self::SECRET_KEY_PREFIXES as $prefix) {
            if (str_starts_with($n, $prefix)) {
                return true;
            }
        }
        // Word-boundary match over underscore-delimited segments.
        foreach (explode('_', $n) as $word) {
            if (in_array($word, self::SECRET_KEY_WORDS, true)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Reports whether a value matches a known credential pattern. Used
     * to catch secrets in fields whose names give no hint.
     */
    public function isSecretValue(string $value): bool
    {
        if ($this->disabled || $value === '') {
            return false;
        }
        foreach (self::SECRET_VALUE_PATTERNS as $pattern) {
            if (preg_match($pattern, $value) === 1) {
                return true;
            }
        }

        return false;
    }

    /**
     * Deterministic replacement for a field. Depends only on the field
     * name — never on the secret value, a counter, or a hash — so
     * re-recording produces byte-identical cassettes.
     */
    public function placeholder(string $name): string
    {
        return self::REDACTED_PREFIX . self::normalizeDisplayKey($name) . self::REDACTED_SUFFIX;
    }

    /**
     * Returns the value to serialize for (name, value). A field is
     * redacted when its name looks credential-bearing OR its value
     * matches a known credential pattern. Empty values are left alone —
     * there is nothing to leak, and a placeholder would misleadingly
     * imply a secret was present.
     */
    public function redactField(string $name, string $value): string
    {
        if ($this->disabled || $value === '') {
            return $value;
        }
        if (isset($this->allow[self::normalizeKey($name)])) {
            return $value;
        }
        if ($this->isSecretKey($name) || $this->isSecretValue($value)) {
            return $this->placeholder($name);
        }

        return $value;
    }

    /**
     * Returns a scrubbed copy of an envelope payload. Only string values
     * are rewritten, so the payload's shape and non-string types survive
     * redaction intact.
     */
    public function redactPayload(mixed $payload): mixed
    {
        if ($this->disabled) {
            return $payload;
        }

        return $this->redactValue($payload, 'payload');
    }

    /**
     * $key is the mapping key the value was reached under. List elements
     * inherit the key of the sequence itself, so
     * `args: [--token, ghp_...]` still gets value-pattern coverage.
     */
    private function redactValue(mixed $value, string $key): mixed
    {
        if (is_string($value)) {
            return $this->redactField($key, $value);
        }
        if (is_array($value)) {
            $out = [];
            foreach ($value as $k => $v) {
                $out[$k] = $this->redactValue($v, is_int($k) ? $key : $k);
            }

            return $out;
        }

        // Non-string scalars (ints, bools, null) carry no credentials
        // and rewriting them would change the payload's types.
        return $value;
    }

    private static function env(string $name): string
    {
        $v = getenv($name);

        return $v === false ? '' : $v;
    }

    private static function truthy(string $s): bool
    {
        return !in_array(strtolower(trim($s)), ['', '0', 'false', 'no'], true);
    }

    /** @return list<string> */
    private static function splitList(string $s): array
    {
        if (trim($s) === '') {
            return [];
        }

        return array_values(array_filter(
            array_map(trim(...), explode(',', $s)),
            static fn (string $p): bool => $p !== ''
        ));
    }

    /**
     * Uppercase and fold dashes to underscores so header names
     * ("X-Api-Key") and env names ("X_API_KEY") classify identically.
     */
    private static function normalizeKey(string $k): string
    {
        return strtoupper(str_replace('-', '_', trim($k)));
    }

    /**
     * Uppercase but preserve dashes, so an HTTP header renders as
     * <redacted:X-API-KEY> and an env var as <redacted:API_KEY>.
     */
    private static function normalizeDisplayKey(string $k): string
    {
        return strtoupper(trim($k));
    }
}
