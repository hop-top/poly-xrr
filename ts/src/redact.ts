/**
 * Record-side secret redaction — see spec/cassette-format-v1.md.
 *
 * Redaction happens before serialization, so a secret never reaches
 * disk. Placeholders derive only from the field name, keeping
 * re-recording byte-identical. Fingerprints are computed from the live
 * request before writing, so redaction can never shift a cassette key.
 */

/** Env vars controlling redaction. Redaction is ON by default; these
 * only exist to widen, narrow, or switch it off. */
export const ENV_REDACT_DISABLE = "XRR_REDACT_DISABLE";
export const ENV_REDACT_ALLOW = "XRR_REDACT_ALLOW";
export const ENV_REDACT_DENY = "XRR_REDACT_DENY";

const REDACTED_PREFIX = "<redacted:";
const REDACTED_SUFFIX = ">";

// Matched against the *normalized* field name (uppercased, dashes →
// underscores) as underscore-delimited words, so MONKEY_BUSINESS does
// not trip on "KEY" and TOKENIZER_MODE does not trip on "TOKEN".
const SECRET_KEY_WORDS = new Set([
  "TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASSPHRASE", "CREDENTIAL",
  "CREDENTIALS", "APIKEY", "KEY", "AUTH", "AUTHORIZATION", "COOKIE",
  "SESSION", "SIGNATURE", "PRIVATE", "ACCESS", "BEARER", "OTP",
]);

// Names that are credential-bearing as a whole but whose words are too
// generic to blanket-match.
const SECRET_KEY_EXACT = new Set([
  "AUTHORIZATION", "PROXY_AUTHORIZATION", "COOKIE", "SET_COOKIE",
  "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
]);

// Whole namespaces that are credential-adjacent enough to redact wholesale.
const SECRET_KEY_PREFIXES = ["AWS_"];

// Never redacted by name: well-known, non-credential variables whose
// values carry real debugging signal.
const BENIGN_KEYS = new Set([
  "XRR_MODE", "XRR_CASSETTE_DIR", "XRR_REDACT_ALLOW", "XRR_REDACT_DENY",
  "XRR_REDACT_DISABLE", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
  "SSH_AUTH_SOCK", "KEYBOARD_LAYOUT", "ACCESS_LOG", "ACCESS_LOG_FORMAT",
  "PRIVATE_NETWORK", "SESSION_MANAGER", "GPG_TTY", "AUTHORIZED_KEYS_FILE",
]);

// High-confidence, vendor-prefixed credential shapes. Deliberately
// narrow: a false positive silently corrupts a cassette, so generic
// "long random-looking string" heuristics are NOT used — they would
// redact commit SHAs, UUIDs, and base64 payloads.
const SECRET_VALUE_PATTERNS: RegExp[] = [
  // GitHub: ghp_/gho_/ghu_/ghs_/ghr_ + 36+ chars, and fine-grained PATs.
  /\bgh[pousr]_[A-Za-z0-9]{20,}\b/,
  /\bgithub_pat_[A-Za-z0-9_]{20,}\b/,
  // AWS access key IDs.
  /\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b/,
  // OpenAI / Anthropic style.
  /\bsk-[A-Za-z0-9_-]{20,}\b/,
  // Slack.
  /\bxox[abposr]-[A-Za-z0-9-]{10,}\b/,
  // Google API keys.
  /\bAIza[A-Za-z0-9_-]{35}\b/,
  // Stripe.
  /\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b/,
  // PEM private key blocks.
  /-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----/,
  // JWTs: header starts with the standard `{"alg"` prefix as eyJhbGci.
  /\beyJhbGci[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/,
  // Bearer/Basic credentials embedded in a header value.
  /\b(?:bearer|basic)\s+[A-Za-z0-9+/._~-]{16,}={0,2}/i,
];

/** Zero value = defaults on: name-based matching over the built-in word
 * list plus value-pattern matching over the built-in vendor patterns. */
export interface RedactConfig {
  /** Turns redaction off entirely. */
  disabled?: boolean;
  /** Field names preserved verbatim (wins over everything). */
  allow?: string[];
  /** Extra field names to always redact. */
  deny?: string[];
}

/** Builds a RedactConfig from the XRR_REDACT_* env vars. Unset vars
 * leave the secure defaults in place. */
export function redactConfigFromEnv(): RedactConfig {
  return {
    disabled: truthy(process.env[ENV_REDACT_DISABLE] ?? ""),
    allow: splitList(process.env[ENV_REDACT_ALLOW] ?? ""),
    deny: splitList(process.env[ENV_REDACT_DENY] ?? ""),
  };
}

function truthy(s: string): boolean {
  switch (s.trim().toLowerCase()) {
    case "":
    case "0":
    case "false":
    case "no":
      return false;
    default:
      return true;
  }
}

function splitList(s: string): string[] {
  if (s.trim() === "") return [];
  return s
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p !== "");
}

/** Uppercases and folds dashes to underscores so header names
 * ("X-Api-Key") and env names ("X_API_KEY") classify identically. */
function normalizeKey(k: string): string {
  return k.trim().replaceAll("-", "_").toUpperCase();
}

/** Uppercases but preserves dashes, so an HTTP header renders as
 * <redacted:X-API-KEY> and an env var as <redacted:API_KEY>. */
function normalizeDisplayKey(k: string): string {
  return k.trim().toUpperCase();
}

/**
 * Classifies field names and values as secret-bearing and produces
 * deterministic placeholders for them. Applied at record time by
 * FileCassette.write, before any bytes reach disk.
 */
export class Redactor {
  private readonly disabled: boolean;
  private readonly allow: Set<string>;
  private readonly deny: Set<string>;

  constructor(cfg: RedactConfig = {}) {
    this.disabled = cfg.disabled ?? false;
    this.allow = new Set((cfg.allow ?? []).map(normalizeKey));
    this.deny = new Set((cfg.deny ?? []).map(normalizeKey));
  }

  /** Reports whether a field name looks credential-bearing. */
  isSecretKey(name: string): boolean {
    if (this.disabled) return false;
    const n = normalizeKey(name);
    if (this.allow.has(n)) return false;
    if (this.deny.has(n)) return true;
    if (BENIGN_KEYS.has(n)) return false;
    if (SECRET_KEY_EXACT.has(n)) return true;
    if (SECRET_KEY_PREFIXES.some((p) => n.startsWith(p))) return true;
    return n.split("_").some((word) => SECRET_KEY_WORDS.has(word));
  }

  /** Reports whether a value matches a known credential pattern. Used
   * to catch secrets in fields whose names give no hint. */
  isSecretValue(value: string): boolean {
    if (this.disabled || value === "") return false;
    return SECRET_VALUE_PATTERNS.some((re) => re.test(value));
  }

  /** Deterministic replacement for a field. Depends only on the field
   * name — never on the secret value, a counter, or a hash — so
   * re-recording produces byte-identical cassettes. */
  placeholder(name: string): string {
    return REDACTED_PREFIX + normalizeDisplayKey(name) + REDACTED_SUFFIX;
  }

  /**
   * Returns the value to serialize for (name, value) and whether it was
   * redacted. A field is redacted when its name looks credential-bearing
   * OR its value matches a known credential pattern. Empty values are
   * left alone — there is nothing to leak, and a placeholder would
   * misleadingly imply a secret was present.
   */
  redactField(name: string, value: string): { value: string; redacted: boolean } {
    if (this.disabled || value === "") return { value, redacted: false };
    if (this.allow.has(normalizeKey(name))) return { value, redacted: false };
    if (this.isSecretKey(name) || this.isSecretValue(value)) {
      return { value: this.placeholder(name), redacted: true };
    }
    return { value, redacted: false };
  }

  /** Returns a scrubbed copy of an envelope payload. The input is never
   * mutated; only string values are rewritten, so the payload's shape
   * and non-string types survive redaction intact. */
  redactPayload(payload: unknown): unknown {
    if (this.disabled) return payload;
    return this.redactValue(payload, "payload");
  }

  // key is the mapping key the value was reached under. Sequence
  // elements inherit the key of the sequence itself, so
  // `args: [--token, ghp_...]` still gets value-pattern coverage.
  private redactValue(v: unknown, key: string): unknown {
    if (typeof v === "string") return this.redactField(key, v).value;
    if (Array.isArray(v)) return v.map((el) => this.redactValue(el, key));
    if (v !== null && typeof v === "object") {
      const out: Record<string, unknown> = {};
      for (const [k, val] of Object.entries(v)) out[k] = this.redactValue(val, k);
      return out;
    }
    // Non-string scalars (numbers, booleans, null) carry no credentials
    // and rewriting them would change the payload's types.
    return v;
  }
}
