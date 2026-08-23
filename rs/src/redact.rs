//! Record-side secret redaction — see spec/cassette-format-v1.md.
//!
//! Redaction happens before serialization, so a secret never reaches
//! disk. Placeholders derive only from the field name, keeping
//! re-recording byte-identical. Fingerprints are computed from the live
//! request before writing, so redaction can never shift a cassette key.

use std::collections::HashSet;
use std::sync::LazyLock;

use regex::Regex;

/// Env var turning redaction off entirely when set to a non-empty value
/// other than "0"/"false"/"no". Redaction is ON by default.
pub const ENV_REDACT_DISABLE: &str = "XRR_REDACT_DISABLE";
/// Env var: comma-separated field names to preserve verbatim.
pub const ENV_REDACT_ALLOW: &str = "XRR_REDACT_ALLOW";
/// Env var: comma-separated extra field names to always redact.
pub const ENV_REDACT_DENY: &str = "XRR_REDACT_DENY";

const REDACTED_PREFIX: &str = "<redacted:";
const REDACTED_SUFFIX: &str = ">";

/// Matched against the *normalized* field name (uppercased, dashes →
/// underscores) as underscore-delimited words, so MONKEY_BUSINESS does
/// not trip on "KEY" and TOKENIZER_MODE does not trip on "TOKEN".
const SECRET_KEY_WORDS: &[&str] = &[
    "TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASSPHRASE", "CREDENTIAL",
    "CREDENTIALS", "APIKEY", "KEY", "AUTH", "AUTHORIZATION", "COOKIE",
    "SESSION", "SIGNATURE", "PRIVATE", "ACCESS", "BEARER", "OTP",
];

/// Names that are credential-bearing as a whole but whose words are too
/// generic to blanket-match.
const SECRET_KEY_EXACT: &[&str] = &[
    "AUTHORIZATION", "PROXY_AUTHORIZATION", "COOKIE", "SET_COOKIE",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
];

/// Whole namespaces credential-adjacent enough to redact wholesale.
const SECRET_KEY_PREFIXES: &[&str] = &["AWS_"];

/// Never redacted by name: well-known, non-credential variables whose
/// values carry real debugging signal.
const BENIGN_KEYS: &[&str] = &[
    "XRR_MODE", "XRR_CASSETTE_DIR", "XRR_REDACT_ALLOW", "XRR_REDACT_DENY",
    "XRR_REDACT_DISABLE", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
    "SSH_AUTH_SOCK", "KEYBOARD_LAYOUT", "ACCESS_LOG", "ACCESS_LOG_FORMAT",
    "PRIVATE_NETWORK", "SESSION_MANAGER", "GPG_TTY", "AUTHORIZED_KEYS_FILE",
];

/// High-confidence, vendor-prefixed credential shapes. Deliberately
/// narrow: a false positive silently corrupts a cassette, so generic
/// "long random-looking string" heuristics are NOT used — they would
/// redact commit SHAs, UUIDs, and base64 payloads.
static SECRET_VALUE_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        // GitHub: ghp_/gho_/ghu_/ghs_/ghr_ + 36+ chars, and fine-grained PATs.
        r"\bgh[pousr]_[A-Za-z0-9]{20,}\b",
        r"\bgithub_pat_[A-Za-z0-9_]{20,}\b",
        // AWS access key IDs.
        r"\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b",
        // OpenAI / Anthropic style.
        r"\bsk-[A-Za-z0-9_-]{20,}\b",
        // Slack.
        r"\bxox[abposr]-[A-Za-z0-9-]{10,}\b",
        // Google API keys.
        r"\bAIza[A-Za-z0-9_-]{35}\b",
        // Stripe.
        r"\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b",
        // PEM private key blocks.
        r"-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----",
        // JWTs: header starts with the standard `{"alg"` prefix as eyJhbGci.
        r"\beyJhbGci[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
        // Bearer/Basic credentials embedded in a header value.
        r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/._~-]{16,}={0,2}",
    ]
    .iter()
    .map(|p| Regex::new(p).expect("static redaction pattern must compile"))
    .collect()
});

/// Default = defaults on: name-based matching over the built-in word
/// list plus value-pattern matching over the built-in vendor patterns.
#[derive(Debug, Clone, Default)]
pub struct RedactConfig {
    /// Turns redaction off entirely.
    pub disabled: bool,
    /// Field names preserved verbatim (wins over everything).
    pub allow: Vec<String>,
    /// Extra field names to always redact.
    pub deny: Vec<String>,
}

impl RedactConfig {
    /// Builds a config from the XRR_REDACT_* env vars. Unset vars leave
    /// the secure defaults in place.
    pub fn from_env() -> Self {
        Self {
            disabled: truthy(&env_var(ENV_REDACT_DISABLE)),
            allow: split_list(&env_var(ENV_REDACT_ALLOW)),
            deny: split_list(&env_var(ENV_REDACT_DENY)),
        }
    }
}

fn env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

fn truthy(s: &str) -> bool {
    !matches!(s.trim().to_lowercase().as_str(), "" | "0" | "false" | "no")
}

fn split_list(s: &str) -> Vec<String> {
    if s.trim().is_empty() {
        return Vec::new();
    }
    s.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(String::from)
        .collect()
}

/// Uppercases and folds dashes to underscores so header names
/// ("X-Api-Key") and env names ("X_API_KEY") classify identically.
fn normalize_key(k: &str) -> String {
    k.trim().replace('-', "_").to_uppercase()
}

/// Uppercases but preserves dashes, so an HTTP header renders as
/// `<redacted:X-API-KEY>` and an env var as `<redacted:API_KEY>`.
fn normalize_display_key(k: &str) -> String {
    k.trim().to_uppercase()
}

/// Top-level envelope fields that must never be rewritten. Scoped to
/// the document root so a payload field genuinely named "error" or
/// "adapter" is still eligible for redaction.
pub(crate) const ENVELOPE_META_KEYS: &[&str] =
    &["xrr", "adapter", "fingerprint", "recorded_at", "error"];

/// Classifies field names and values as secret-bearing and produces
/// deterministic placeholders for them. Applied at record time by
/// `FileCassette::write`, before any bytes reach disk.
#[derive(Debug, Clone)]
pub struct Redactor {
    disabled: bool,
    allow: HashSet<String>,
    deny: HashSet<String>,
}

impl Default for Redactor {
    fn default() -> Self {
        Self::new(RedactConfig::default())
    }
}

impl Redactor {
    /// Returns a Redactor for cfg. The default config yields the secure
    /// defaults.
    pub fn new(cfg: RedactConfig) -> Self {
        Self {
            disabled: cfg.disabled,
            allow: cfg.allow.iter().map(|k| normalize_key(k)).collect(),
            deny: cfg.deny.iter().map(|k| normalize_key(k)).collect(),
        }
    }

    /// Shorthand for `Redactor::new(RedactConfig::from_env())`.
    pub fn from_env() -> Self {
        Self::new(RedactConfig::from_env())
    }

    /// Reports whether a field name looks credential-bearing.
    pub fn is_secret_key(&self, name: &str) -> bool {
        if self.disabled {
            return false;
        }
        let n = normalize_key(name);
        if self.allow.contains(&n) {
            return false;
        }
        if self.deny.contains(&n) {
            return true;
        }
        if BENIGN_KEYS.contains(&n.as_str()) {
            return false;
        }
        if SECRET_KEY_EXACT.contains(&n.as_str()) {
            return true;
        }
        if SECRET_KEY_PREFIXES.iter().any(|p| n.starts_with(p)) {
            return true;
        }
        // Word-boundary match over underscore-delimited segments.
        n.split('_').any(|word| SECRET_KEY_WORDS.contains(&word))
    }

    /// Reports whether a value matches a known credential pattern. Used
    /// to catch secrets in fields whose names give no hint.
    pub fn is_secret_value(&self, value: &str) -> bool {
        if self.disabled || value.is_empty() {
            return false;
        }
        SECRET_VALUE_PATTERNS.iter().any(|re| re.is_match(value))
    }

    /// Deterministic replacement for a field. Depends only on the field
    /// name — never on the secret value, a counter, or a hash — so
    /// re-recording produces byte-identical cassettes.
    pub fn placeholder(&self, name: &str) -> String {
        format!("{REDACTED_PREFIX}{}{REDACTED_SUFFIX}", normalize_display_key(name))
    }

    /// Returns `Some(placeholder)` when (name, value) must be redacted,
    /// `None` to keep the value verbatim. A field is redacted when its
    /// name looks credential-bearing OR its value matches a known
    /// credential pattern. Empty values are left alone — there is
    /// nothing to leak, and a placeholder would misleadingly imply a
    /// secret was present.
    pub fn redact_field(&self, name: &str, value: &str) -> Option<String> {
        if self.disabled || value.is_empty() {
            return None;
        }
        if self.allow.contains(&normalize_key(name)) {
            return None;
        }
        if self.is_secret_key(name) || self.is_secret_value(value) {
            return Some(self.placeholder(name));
        }
        None
    }

    /// Walks a decoded YAML tree in place, replacing credential-bearing
    /// string scalars with deterministic placeholders. `key` is the
    /// mapping key the node was reached under. Sequence elements inherit
    /// the key of the sequence itself, so `args: [--token, ghp_...]`
    /// still gets value-pattern coverage. Only string scalars are
    /// rewritten; mapping keys and non-string scalars (ints, bools,
    /// null) are left untouched so the YAML shape and types survive
    /// redaction intact.
    pub(crate) fn redact_node(&self, node: &mut serde_yaml::Value, key: &str) {
        if self.disabled {
            return;
        }
        match node {
            serde_yaml::Value::Mapping(map) => {
                for (k, v) in map.iter_mut() {
                    let child_key = k.as_str().unwrap_or("").to_owned();
                    self.redact_node(v, &child_key);
                }
            }
            serde_yaml::Value::Sequence(seq) => {
                for el in seq.iter_mut() {
                    self.redact_node(el, key);
                }
            }
            serde_yaml::Value::String(s) => {
                if let Some(placeholder) = self.redact_field(key, s) {
                    *s = placeholder;
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- key-name classification ---------------------------------------

    #[test]
    fn default_redactor_secret_env_names() {
        let r = Redactor::default();
        for k in [
            "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID",
            "API_KEY", "DB_PASSWORD", "GOOGLE_CREDENTIALS", "MY_SECRET",
            "npm_token", "Stripe_Api_Key", "SESSION_COOKIE", "PRIVATE_KEY",
            "AUTH_TOKEN", "PASSPHRASE", "SOME_AUTH", "CLIENT_SECRET",
        ] {
            assert!(r.is_secret_key(k), "expected {k:?} to be classified secret");
        }
    }

    #[test]
    fn default_redactor_benign_env_names() {
        let r = Redactor::default();
        for k in [
            "PATH", "HOME", "LANG", "PWD", "SHELL", "TERM", "GOPATH",
            "CI", "NODE_ENV", "XRR_MODE", "XRR_CASSETTE_DIR",
            // "key"/"token" as a substring of a non-credential word must not trip.
            "MONKEY_BUSINESS", "TOKENIZER_MODE", "KEYBOARD_LAYOUT",
        ] {
            assert!(!r.is_secret_key(k), "expected {k:?} to be benign");
        }
    }

    #[test]
    fn default_redactor_secret_header_names() {
        let r = Redactor::default();
        // Header matching must be case-insensitive and dash/underscore agnostic.
        for k in [
            "Authorization", "authorization", "Proxy-Authorization",
            "Cookie", "Set-Cookie", "X-Api-Key", "x-api-key", "X_API_KEY",
            "X-Auth-Token", "X-Amz-Security-Token", "X-CSRF-Token",
        ] {
            assert!(r.is_secret_key(k), "expected header {k:?} to be classified secret");
        }
        for k in ["Content-Type", "Accept", "User-Agent", "Content-Length"] {
            assert!(!r.is_secret_key(k), "expected header {k:?} to be benign");
        }
    }

    // --- placeholder shape ----------------------------------------------

    #[test]
    fn placeholder_is_deterministic_and_named() {
        let r = Redactor::default();
        let got = r.placeholder("Authorization");
        assert_eq!(got, "<redacted:AUTHORIZATION>");
        // Stable across calls — no counters, no randomness, no hashing of value.
        assert_eq!(r.placeholder("Authorization"), got);
        assert_eq!(r.placeholder("X-Api-Key"), "<redacted:X-API-KEY>");
        assert_eq!(r.placeholder("GITHUB_TOKEN"), "<redacted:GITHUB_TOKEN>");
    }

    // --- value-pattern matching -----------------------------------------

    #[test]
    fn default_redactor_secret_value_patterns() {
        let r = Redactor::default();
        // High-confidence, vendor-prefixed tokens. Name gives no hint here —
        // only the value shape does.
        for v in [
            "ghp_0123456789abcdefghijklmnopqrstuvwxyz",
            "github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR",
            "AKIAIOSFODNN7EXAMPLE",
            "sk-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij0123456789",
            "xoxb-123456789012-1234567890123-abcdefghijklmnopqrstuvwx",
            "-----BEGIN RSA PRIVATE KEY-----",
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefghijklmnop",
        ] {
            assert!(r.is_secret_value(v), "expected value {v:?} to match a secret pattern");
        }
    }

    #[test]
    fn default_redactor_benign_values_not_matched() {
        let r = Redactor::default();
        for v in [
            "", "/usr/local/bin:/usr/bin", "en_US.UTF-8", "true", "1",
            "https://api.example.com/v1/things?page=2",
            "application/json", "Mozilla/5.0 (Macintosh)",
            "a3f9c1b2", "hello world",
        ] {
            assert!(!r.is_secret_value(v), "expected value {v:?} NOT to match a secret pattern");
        }
    }

    // --- escape hatch + custom config -----------------------------------

    #[test]
    fn allow_list_preserves_value() {
        let r = Redactor::new(RedactConfig {
            allow: vec!["GITHUB_TOKEN".into()],
            ..Default::default()
        });
        assert!(!r.is_secret_key("GITHUB_TOKEN"), "allow-list must win over default deny");
        // Sibling secrets still redacted.
        assert!(r.is_secret_key("AWS_SECRET_ACCESS_KEY"));
    }

    #[test]
    fn custom_deny_keys() {
        let r = Redactor::new(RedactConfig {
            deny: vec!["MY_CUSTOM_FIELD".into()],
            ..Default::default()
        });
        assert!(r.is_secret_key("MY_CUSTOM_FIELD"));
        assert!(r.is_secret_key("my_custom_field"), "custom deny must be case-insensitive");
    }

    #[test]
    fn disabled_redactor() {
        let r = Redactor::new(RedactConfig {
            disabled: true,
            ..Default::default()
        });
        assert!(!r.is_secret_key("GITHUB_TOKEN"));
        assert!(!r.is_secret_value("ghp_0123456789abcdefghijklmnopqrstuvwxyz"));
    }

    #[test]
    fn allow_list_suppresses_value_pattern_match() {
        // Allow-listing a key must also suppress value-pattern redaction
        // for that key, otherwise the escape hatch is useless for a var
        // whose value happens to look like a token.
        let r = Redactor::new(RedactConfig {
            allow: vec!["FIXTURE_TOKEN".into()],
            ..Default::default()
        });
        assert_eq!(
            r.redact_field("FIXTURE_TOKEN", "ghp_0123456789abcdefghijklmnopqrstuvwxyz"),
            None
        );
    }

    // --- redact_field composition ---------------------------------------

    #[test]
    fn redact_field_composition() {
        let r = Redactor::default();

        // Secret by key name.
        assert_eq!(
            r.redact_field("GITHUB_TOKEN", "anything"),
            Some("<redacted:GITHUB_TOKEN>".into())
        );

        // Secret by value shape, benign key.
        assert_eq!(
            r.redact_field("MY_VAR", "ghp_0123456789abcdefghijklmnopqrstuvwxyz"),
            Some("<redacted:MY_VAR>".into())
        );

        // Benign key + benign value passes through untouched.
        assert_eq!(r.redact_field("PATH", "/usr/bin"), None);

        // Empty value on a secret key: nothing to leak, leave it alone.
        assert_eq!(r.redact_field("GITHUB_TOKEN", ""), None);
    }

    #[test]
    fn split_list_and_truthy() {
        assert!(truthy("1"));
        assert!(truthy("yes"));
        assert!(!truthy(""));
        assert!(!truthy("0"));
        assert!(!truthy("false"));
        assert!(!truthy("No"));

        assert_eq!(split_list(""), Vec::<String>::new());
        assert_eq!(split_list("FOO_TOKEN, BAR_KEY"), vec!["FOO_TOKEN", "BAR_KEY"]);
        assert_eq!(split_list("A,,B ,"), vec!["A", "B"]);
    }
}
