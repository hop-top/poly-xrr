"""Tests for record-side secret redaction — mirrors the Go coverage."""
from __future__ import annotations

import pytest
from xrr.adapters.exec import ExecAdapter, ExecRequest, ExecResponse
from xrr.cassette import FileCassette
from xrr.redact import (
    ENV_REDACT_ALLOW,
    ENV_REDACT_DENY,
    ENV_REDACT_DISABLE,
    RedactConfig,
    Redactor,
    redact_config_from_env,
)
from xrr.session import RECORD, REPLAY, Session

SECRET_TOKEN = "ghp_supersecrettokenvalue0123456789abcd"


def read_all(directory) -> str:
    """Concatenate every file under directory so a test can assert on
    'nothing anywhere in the cassette dir contains this string'."""
    return "\n".join(
        p.read_text() for p in sorted(directory.iterdir()) if p.is_file()
    )


def strip_recorded_at(s: str) -> str:
    return "\n".join(
        line for line in s.splitlines() if not line.startswith("recorded_at:")
    )


# --- key-name classification -------------------------------------------


def test_default_redactor_secret_env_names():
    r = Redactor()
    secret = [
        "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID",
        "API_KEY", "DB_PASSWORD", "GOOGLE_CREDENTIALS", "MY_SECRET",
        "npm_token", "Stripe_Api_Key", "SESSION_COOKIE", "PRIVATE_KEY",
        "AUTH_TOKEN", "PASSPHRASE", "SOME_AUTH", "CLIENT_SECRET",
    ]
    for k in secret:
        assert r.is_secret_key(k), f"expected {k!r} to be classified secret"


def test_default_redactor_benign_env_names():
    r = Redactor()
    benign = [
        "PATH", "HOME", "LANG", "PWD", "SHELL", "TERM", "GOPATH",
        "CI", "NODE_ENV", "XRR_MODE", "XRR_CASSETTE_DIR",
        # "key"/"token" as a substring of a non-credential word must not trip.
        "MONKEY_BUSINESS", "TOKENIZER_MODE", "KEYBOARD_LAYOUT",
    ]
    for k in benign:
        assert not r.is_secret_key(k), f"expected {k!r} to be benign"


def test_default_redactor_secret_header_names():
    r = Redactor()
    # Header matching must be case-insensitive and dash/underscore agnostic.
    secret = [
        "Authorization", "authorization", "Proxy-Authorization",
        "Cookie", "Set-Cookie", "X-Api-Key", "x-api-key", "X_API_KEY",
        "X-Auth-Token", "X-Amz-Security-Token", "X-CSRF-Token",
    ]
    for k in secret:
        assert r.is_secret_key(k), f"expected header {k!r} to be classified secret"
    for k in ["Content-Type", "Accept", "User-Agent", "Content-Length"]:
        assert not r.is_secret_key(k), f"expected header {k!r} to be benign"


# --- placeholder shape --------------------------------------------------


def test_placeholder_is_deterministic_and_named():
    r = Redactor()
    got = r.placeholder("Authorization")
    assert got == "<redacted:AUTHORIZATION>"
    # Stable across calls — no counters, no randomness, no hashing of value.
    assert r.placeholder("Authorization") == got
    assert r.placeholder("X-Api-Key") == "<redacted:X-API-KEY>"
    assert r.placeholder("GITHUB_TOKEN") == "<redacted:GITHUB_TOKEN>"


# --- value-pattern matching ---------------------------------------------


def test_default_redactor_secret_value_patterns():
    r = Redactor()
    # High-confidence, vendor-prefixed tokens. Name gives no hint here —
    # only the value shape does.
    secret = [
        "ghp_0123456789abcdefghijklmnopqrstuvwxyz",
        "github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR",
        "AKIAIOSFODNN7EXAMPLE",
        "sk-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij0123456789",
        "xoxb-123456789012-1234567890123-abcdefghijklmnopqrstuvwx",
        "-----BEGIN RSA PRIVATE KEY-----",
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefghijklmnop",
    ]
    for v in secret:
        assert r.is_secret_value(v), f"expected value {v!r} to match a secret pattern"


def test_default_redactor_benign_values_not_matched():
    r = Redactor()
    benign = [
        "", "/usr/local/bin:/usr/bin", "en_US.UTF-8", "true", "1",
        "https://api.example.com/v1/things?page=2",
        "application/json", "Mozilla/5.0 (Macintosh)",
        "a3f9c1b2", "hello world",
    ]
    for v in benign:
        assert not r.is_secret_value(v), f"expected value {v!r} NOT to match"


# --- escape hatch + custom config ---------------------------------------


def test_allow_list_preserves_value():
    r = Redactor(RedactConfig(allow=["GITHUB_TOKEN"]))
    assert not r.is_secret_key("GITHUB_TOKEN"), "allow-list must win over default deny"
    # Sibling secrets still redacted.
    assert r.is_secret_key("AWS_SECRET_ACCESS_KEY")


def test_custom_deny_keys():
    r = Redactor(RedactConfig(deny=["MY_CUSTOM_FIELD"]))
    assert r.is_secret_key("MY_CUSTOM_FIELD")
    assert r.is_secret_key("my_custom_field"), "custom deny must be case-insensitive"


def test_disabled():
    r = Redactor(RedactConfig(disabled=True))
    assert not r.is_secret_key("GITHUB_TOKEN")
    assert not r.is_secret_value("ghp_0123456789abcdefghijklmnopqrstuvwxyz")


def test_allow_list_suppresses_value_pattern_match():
    # Allow-listing a key must also suppress value-pattern redaction for
    # that key, otherwise the escape hatch is useless for a var whose
    # value happens to look like a token.
    r = Redactor(RedactConfig(allow=["FIXTURE_TOKEN"]))
    v, redacted = r.redact_field(
        "FIXTURE_TOKEN", "ghp_0123456789abcdefghijklmnopqrstuvwxyz"
    )
    assert not redacted
    assert v == "ghp_0123456789abcdefghijklmnopqrstuvwxyz"


def test_config_from_env(monkeypatch):
    monkeypatch.setenv(ENV_REDACT_DISABLE, "1")
    assert redact_config_from_env().disabled

    monkeypatch.setenv(ENV_REDACT_DISABLE, "")
    monkeypatch.setenv(ENV_REDACT_ALLOW, "FOO_TOKEN, BAR_KEY")
    monkeypatch.setenv(ENV_REDACT_DENY, "CUSTOM_A,CUSTOM_B")
    cfg = redact_config_from_env()
    assert not cfg.disabled
    assert cfg.allow == ["FOO_TOKEN", "BAR_KEY"]
    assert cfg.deny == ["CUSTOM_A", "CUSTOM_B"]


# --- redact_field composition -------------------------------------------


def test_redact_field():
    r = Redactor()

    # Secret by key name.
    v, redacted = r.redact_field("GITHUB_TOKEN", "anything")
    assert redacted
    assert v == "<redacted:GITHUB_TOKEN>"

    # Secret by value shape, benign key.
    v, redacted = r.redact_field(
        "MY_VAR", "ghp_0123456789abcdefghijklmnopqrstuvwxyz"
    )
    assert redacted
    assert v == "<redacted:MY_VAR>"

    # Benign key + benign value passes through untouched.
    v, redacted = r.redact_field("PATH", "/usr/bin")
    assert not redacted
    assert v == "/usr/bin"

    # Empty value on a secret key: nothing to leak, leave it alone.
    v, redacted = r.redact_field("GITHUB_TOKEN", "")
    assert not redacted
    assert v == ""


# --- record-side integration --------------------------------------------


def record(directory, req: ExecRequest, resp: ExecResponse) -> None:
    sess = Session(RECORD, FileCassette(str(directory)))
    sess.record(ExecAdapter(), req, lambda: resp)


def test_record_secret_env_never_hits_disk(tmp_path):
    record(
        tmp_path,
        ExecRequest(
            argv=["gh", "pr", "view", "1"],
            env={"GITHUB_TOKEN": SECRET_TOKEN, "PATH": "/usr/local/bin:/usr/bin"},
        ),
        ExecResponse(stdout="ok\n"),
    )

    on_disk = read_all(tmp_path)
    assert SECRET_TOKEN not in on_disk, "secret env value leaked into cassette"
    assert "<redacted:GITHUB_TOKEN>" in on_disk
    # Benign env survives — redaction must not nuke useful debugging context.
    assert "/usr/local/bin:/usr/bin" in on_disk


def test_record_value_pattern_secret_never_hits_disk(tmp_path):
    # The env var name gives no hint, only the value shape does.
    record(
        tmp_path,
        ExecRequest(argv=["deploy"], env={"DEPLOY_HANDLE": SECRET_TOKEN}),
        ExecResponse(stdout="done\n"),
    )

    on_disk = read_all(tmp_path)
    assert SECRET_TOKEN not in on_disk
    assert "<redacted:DEPLOY_HANDLE>" in on_disk


def test_record_redaction_is_fingerprint_stable(tmp_path):
    # Fingerprints are computed from {argv, stdin} only, so redacting
    # env cannot shift them. If this fails, ports would disagree on
    # cassette filenames.
    adapter = ExecAdapter()

    def new_req() -> ExecRequest:
        return ExecRequest(
            argv=["gh", "pr", "view", "1"], env={"GITHUB_TOKEN": SECRET_TOKEN}
        )

    dirs = [tmp_path / "a", tmp_path / "b"]
    for d in dirs:
        d.mkdir()
        record(d, new_req(), ExecResponse(stdout="ok\n"))

    first, second = (sorted(p.name for p in d.iterdir()) for d in dirs)
    assert first == second, "re-recording must produce identical cassette filenames"

    fp_direct = adapter.fingerprint(new_req())
    for name in first:
        assert fp_direct in name, "cassette filename must carry the canonical fingerprint"


def test_record_byte_identical_across_runs(tmp_path):
    # Placeholders must be stable, not derived from a counter or the
    # secret's hash, or committed cassettes would churn on re-record.
    adapter = ExecAdapter()

    def run(d) -> str:
        d.mkdir()
        req = ExecRequest(
            argv=["gh", "auth", "status"],
            env={"GITHUB_TOKEN": SECRET_TOKEN, "AWS_SECRET_ACCESS_KEY": "abc123"},
        )
        record(d, req, ExecResponse(stdout="ok\n"))
        fp = adapter.fingerprint(req)
        data = (d / f"exec-{fp}.req.yaml").read_text()
        # recorded_at is a timestamp and legitimately varies; drop it.
        return strip_recorded_at(data)

    assert run(tmp_path / "a") == run(tmp_path / "b")


def test_record_replay_round_trip(tmp_path):
    # Redaction must not break the record->replay round trip. The
    # response payload replays intact; redacted request fields are not
    # part of matching.
    adapter = ExecAdapter()
    req = ExecRequest(
        argv=["gh", "pr", "view", "7"], env={"GITHUB_TOKEN": SECRET_TOKEN}
    )
    record(tmp_path, req, ExecResponse(stdout="title: hello\n"))

    rep = Session(REPLAY, FileCassette(str(tmp_path)))

    def must_not_call():
        pytest.fail("do() must not be called in replay mode")

    got = rep.record(adapter, req, must_not_call)
    assert got.stdout == "title: hello\n"


def test_record_disabled_via_env(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_REDACT_DISABLE, "1")
    record(
        tmp_path,
        ExecRequest(argv=["gh"], env={"GITHUB_TOKEN": SECRET_TOKEN}),
        ExecResponse(stdout="ok\n"),
    )
    assert SECRET_TOKEN in read_all(tmp_path), "disable flag must restore verbatim recording"


def test_record_allow_list_via_env(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_REDACT_ALLOW, "GITHUB_TOKEN")
    record(
        tmp_path,
        ExecRequest(
            argv=["gh"],
            env={"GITHUB_TOKEN": SECRET_TOKEN, "AWS_SECRET_ACCESS_KEY": "leakme"},
        ),
        ExecResponse(stdout="ok\n"),
    )
    on_disk = read_all(tmp_path)
    assert SECRET_TOKEN in on_disk, "allow-listed key must be preserved"
    assert "leakme" not in on_disk, "non-allow-listed secret must still be redacted"


def test_record_nested_structures_preserved(tmp_path):
    c = FileCassette(str(tmp_path))
    req = {
        "argv": ["svc"],
        "config": {
            "retries": 3,
            "verbose": True,
            "api_key": SECRET_TOKEN,
            "endpoint": "https://example.com",
            "nested_map": {"password": "hunter2"},
        },
    }
    c.save("exec", "aabbccdd", req, {"stdout": "ok"})

    on_disk = read_all(tmp_path)
    assert SECRET_TOKEN not in on_disk
    assert "hunter2" not in on_disk
    assert "<redacted:API_KEY>" in on_disk
    assert "<redacted:PASSWORD>" in on_disk
    # Non-string scalars keep their YAML type (not quoted into strings).
    assert "retries: 3" in on_disk
    assert "verbose: true" in on_disk
    assert "https://example.com" in on_disk
    # Redaction must not mutate the caller's request.
    assert req["config"]["api_key"] == SECRET_TOKEN


def test_explicit_redactor_bypasses_env(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_REDACT_DISABLE, "1")
    c = FileCassette(str(tmp_path), redactor=Redactor())
    c.save("exec", "aabbccdd", {"env": {"GITHUB_TOKEN": SECRET_TOKEN}}, {"stdout": "ok"})
    on_disk = read_all(tmp_path)
    assert SECRET_TOKEN not in on_disk
    assert "<redacted:GITHUB_TOKEN>" in on_disk
