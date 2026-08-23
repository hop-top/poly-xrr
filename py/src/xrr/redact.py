"""Record-side secret redaction — see spec/cassette-format-v1.md.

Redaction happens before serialization, so a secret never reaches disk.
Placeholders derive only from the field name, keeping re-recording
byte-identical. Fingerprints are computed from the live request before
writing, so redaction can never shift a cassette key.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

# Env vars controlling redaction. Redaction is ON by default; these
# only exist to widen, narrow, or switch it off.
ENV_REDACT_DISABLE = "XRR_REDACT_DISABLE"
ENV_REDACT_ALLOW = "XRR_REDACT_ALLOW"
ENV_REDACT_DENY = "XRR_REDACT_DENY"

_REDACTED_PREFIX = "<redacted:"
_REDACTED_SUFFIX = ">"

# Matched against the *normalized* field name (uppercased, dashes ->
# underscores) as underscore-delimited words, so MONKEY_BUSINESS does
# not trip on "KEY" and TOKENIZER_MODE does not trip on "TOKEN".
_SECRET_KEY_WORDS = frozenset({
    "TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASSPHRASE", "CREDENTIAL",
    "CREDENTIALS", "APIKEY", "KEY", "AUTH", "AUTHORIZATION", "COOKIE",
    "SESSION", "SIGNATURE", "PRIVATE", "ACCESS", "BEARER", "OTP",
})

# Names that are credential-bearing as a whole but whose words are too
# generic to blanket-match.
_SECRET_KEY_EXACT = frozenset({
    "AUTHORIZATION", "PROXY_AUTHORIZATION", "COOKIE", "SET_COOKIE",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
})

# Whole namespaces that are credential-adjacent enough to redact wholesale.
_SECRET_KEY_PREFIXES = ("AWS_",)

# Never redacted by name: well-known, non-credential variables whose
# values carry real debugging signal.
_BENIGN_KEYS = frozenset({
    "XRR_MODE", "XRR_CASSETTE_DIR", "XRR_REDACT_ALLOW", "XRR_REDACT_DENY",
    "XRR_REDACT_DISABLE", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE",
    "SSH_AUTH_SOCK", "KEYBOARD_LAYOUT", "ACCESS_LOG", "ACCESS_LOG_FORMAT",
    "PRIVATE_NETWORK", "SESSION_MANAGER", "GPG_TTY", "AUTHORIZED_KEYS_FILE",
})

# High-confidence, vendor-prefixed credential shapes. Deliberately
# narrow: a false positive silently corrupts a cassette, so generic
# "long random-looking string" heuristics are NOT used — they would
# redact commit SHAs, UUIDs, and base64 payloads.
_SECRET_VALUE_PATTERNS = tuple(re.compile(p) for p in (
    # GitHub: ghp_/gho_/ghu_/ghs_/ghr_ + 36+ chars, and fine-grained PATs.
    r"\bgh[pousr]_[A-Za-z0-9]{20,}\b",
    r"\bgithub_pat_[A-Za-z0-9_]{20,}\b",
    # AWS access key IDs.
    r"\b(?:AKIA|ASIA|ABIA|ACCA)[A-Z0-9]{16}\b",
    # OpenAI / Anthropic style.
    r"\bsk-[A-Za-z0-9_-]{20,}\b",
    # Slack.
    r"\bxox[abposr]-[A-Za-z0-9-]{10,}\b",
    # Google API keys.
    r"\bAIza[A-Za-z0-9_-]{35}\b",
    # Stripe.
    r"\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b",
    # PEM private key blocks.
    r"-----BEGIN (?:[A-Z ]+ )?PRIVATE KEY-----",
    # JWTs: header starts with the standard `{"alg"` prefix as eyJhbGci.
    r"\beyJhbGci[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
    # Bearer/Basic credentials embedded in a header value.
    r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/._~-]{16,}={0,2}",
))


@dataclass
class RedactConfig:
    """Zero value = defaults on: name-based matching over the built-in
    word list plus value-pattern matching over the built-in patterns."""

    disabled: bool = False
    allow: list[str] = field(default_factory=list)
    deny: list[str] = field(default_factory=list)


def redact_config_from_env() -> RedactConfig:
    """Build a RedactConfig from the XRR_REDACT_* env vars. Unset vars
    leave the secure defaults in place."""
    return RedactConfig(
        disabled=_truthy(os.environ.get(ENV_REDACT_DISABLE, "")),
        allow=_split_list(os.environ.get(ENV_REDACT_ALLOW, "")),
        deny=_split_list(os.environ.get(ENV_REDACT_DENY, "")),
    )


def _truthy(s: str) -> bool:
    return s.strip().lower() not in ("", "0", "false", "no")


def _split_list(s: str) -> list[str]:
    if not s.strip():
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _normalize_key(k: str) -> str:
    """Uppercase and fold dashes to underscores so header names
    ("X-Api-Key") and env names ("X_API_KEY") classify identically."""
    return k.strip().replace("-", "_").upper()


def _normalize_display_key(k: str) -> str:
    """Uppercase but preserve dashes, so an HTTP header renders as
    <redacted:X-API-KEY> and an env var as <redacted:API_KEY>."""
    return k.strip().upper()


class Redactor:
    """Classifies field names and values as secret-bearing and produces
    deterministic placeholders for them. Applied at record time by
    FileCassette._write, before any bytes reach disk."""

    def __init__(self, config: RedactConfig | None = None) -> None:
        cfg = config or RedactConfig()
        self._disabled = cfg.disabled
        self._allow = {_normalize_key(k) for k in cfg.allow}
        self._deny = {_normalize_key(k) for k in cfg.deny}

    def is_secret_key(self, name: str) -> bool:
        """Report whether a field name looks credential-bearing."""
        if self._disabled:
            return False
        n = _normalize_key(name)
        if n in self._allow:
            return False
        if n in self._deny:
            return True
        if n in _BENIGN_KEYS:
            return False
        if n in _SECRET_KEY_EXACT:
            return True
        if n.startswith(_SECRET_KEY_PREFIXES):
            return True
        return any(word in _SECRET_KEY_WORDS for word in n.split("_"))

    def is_secret_value(self, value: str) -> bool:
        """Report whether a value matches a known credential pattern.
        Used to catch secrets in fields whose names give no hint."""
        if self._disabled or not value:
            return False
        return any(p.search(value) for p in _SECRET_VALUE_PATTERNS)

    def placeholder(self, name: str) -> str:
        """Deterministic replacement for a field. Depends only on the
        field name — never on the secret value, a counter, or a hash —
        so re-recording produces byte-identical cassettes."""
        return _REDACTED_PREFIX + _normalize_display_key(name) + _REDACTED_SUFFIX

    def redact_field(self, name: str, value: str) -> tuple[str, bool]:
        """Return the value to serialize for (name, value) and whether
        it was redacted. A field is redacted when its name looks
        credential-bearing OR its value matches a known credential
        pattern. Empty values are left alone — there is nothing to leak,
        and a placeholder would misleadingly imply a secret was present.
        """
        if self._disabled or not value:
            return value, False
        if _normalize_key(name) in self._allow:
            return value, False
        if self.is_secret_key(name) or self.is_secret_value(value):
            return self.placeholder(name), True
        return value, False

    def redact_payload(self, payload: Any) -> Any:
        """Return a scrubbed copy of an envelope payload. The input is
        never mutated; only string values are rewritten, so the
        payload's shape and non-string types survive redaction intact.
        """
        if self._disabled:
            return payload
        return self._redact_value(payload, "payload")

    def _redact_value(self, value: Any, key: str) -> Any:
        # key is the mapping key the value was reached under. Sequence
        # elements inherit the key of the sequence itself, so
        # `args: [--token, ghp_...]` still gets value-pattern coverage.
        if isinstance(value, str):
            return self.redact_field(key, value)[0]
        if isinstance(value, dict):
            return {k: self._redact_value(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [self._redact_value(v, key) for v in value]
        # Non-string scalars (ints, bools, None) carry no credentials
        # and rewriting them would change the payload's types.
        return value
