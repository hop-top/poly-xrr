import fs from "node:fs";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { ExecAdapter, type ExecRequest, type ExecResponse } from "../src/adapters/exec.js";
import { FileCassette } from "../src/cassette.js";
import {
  ENV_REDACT_ALLOW,
  ENV_REDACT_DENY,
  ENV_REDACT_DISABLE,
  Redactor,
  redactConfigFromEnv,
} from "../src/redact.js";
import { FileSession } from "../src/session.js";

const secretToken = "ghp_supersecrettokenvalue0123456789abcd";

const ENV_VARS = [ENV_REDACT_DISABLE, ENV_REDACT_ALLOW, ENV_REDACT_DENY];
const savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
  for (const k of ENV_VARS) {
    savedEnv[k] = process.env[k];
    delete process.env[k];
  }
});

afterEach(() => {
  for (const k of ENV_VARS) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
});

function tmpDir(): string {
  return fs.mkdtempSync(fs.realpathSync("/tmp") + "/xrr-redact-");
}

// Concatenates every file written under dir so a test can assert on
// "nothing anywhere in the cassette dir contains this string".
function readAll(dir: string): string {
  return fs
    .readdirSync(dir)
    .map((name) => fs.readFileSync(path.join(dir, name), "utf8"))
    .join("\n");
}

function stripRecordedAt(s: string): string {
  return s
    .split("\n")
    .filter((line) => !line.startsWith("recorded_at:"))
    .join("\n");
}

describe("Redactor key classification", () => {
  test("secret env names", () => {
    const r = new Redactor();
    const secret = [
      "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID",
      "API_KEY", "DB_PASSWORD", "GOOGLE_CREDENTIALS", "MY_SECRET",
      "npm_token", "Stripe_Api_Key", "SESSION_COOKIE", "PRIVATE_KEY",
      "AUTH_TOKEN", "PASSPHRASE", "SOME_AUTH", "CLIENT_SECRET",
    ];
    for (const k of secret) {
      expect(r.isSecretKey(k), `expected ${k} to be classified secret`).toBe(true);
    }
  });

  test("benign env names", () => {
    const r = new Redactor();
    const benign = [
      "PATH", "HOME", "LANG", "PWD", "SHELL", "TERM", "GOPATH",
      "CI", "NODE_ENV", "XRR_MODE", "XRR_CASSETTE_DIR",
      // "key"/"token" as a substring of a non-credential word must not trip.
      "MONKEY_BUSINESS", "TOKENIZER_MODE", "KEYBOARD_LAYOUT",
    ];
    for (const k of benign) {
      expect(r.isSecretKey(k), `expected ${k} to be benign`).toBe(false);
    }
  });

  test("secret header names, case- and separator-insensitive", () => {
    const r = new Redactor();
    const secret = [
      "Authorization", "authorization", "Proxy-Authorization",
      "Cookie", "Set-Cookie", "X-Api-Key", "x-api-key", "X_API_KEY",
      "X-Auth-Token", "X-Amz-Security-Token", "X-CSRF-Token",
    ];
    for (const k of secret) {
      expect(r.isSecretKey(k), `expected header ${k} to be classified secret`).toBe(true);
    }
    for (const k of ["Content-Type", "Accept", "User-Agent", "Content-Length"]) {
      expect(r.isSecretKey(k), `expected header ${k} to be benign`).toBe(false);
    }
  });
});

describe("Redactor placeholder", () => {
  test("deterministic and name-derived", () => {
    const r = new Redactor();
    const got = r.placeholder("Authorization");
    expect(got).toBe("<redacted:AUTHORIZATION>");
    // Stable across calls — no counters, no randomness, no hashing of value.
    expect(r.placeholder("Authorization")).toBe(got);
    expect(r.placeholder("X-Api-Key")).toBe("<redacted:X-API-KEY>");
    expect(r.placeholder("GITHUB_TOKEN")).toBe("<redacted:GITHUB_TOKEN>");
  });
});

describe("Redactor value patterns", () => {
  test("high-confidence vendor shapes match", () => {
    const r = new Redactor();
    const secret = [
      "ghp_0123456789abcdefghijklmnopqrstuvwxyz",
      "github_pat_11ABCDEFG0123456789_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQR",
      "AKIAIOSFODNN7EXAMPLE",
      "sk-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij0123456789",
      "xoxb-EXAMPLE-NOT-A-REAL-TOKEN-000",
      "-----BEGIN RSA PRIVATE KEY-----",
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abcdefghijklmnop",
    ];
    for (const v of secret) {
      expect(r.isSecretValue(v), `expected value ${v} to match a secret pattern`).toBe(true);
    }
  });

  test("benign values do not match", () => {
    const r = new Redactor();
    const benign = [
      "", "/usr/local/bin:/usr/bin", "en_US.UTF-8", "true", "1",
      "https://api.example.com/v1/things?page=2",
      "application/json", "Mozilla/5.0 (Macintosh)",
      "a3f9c1b2", "hello world",
    ];
    for (const v of benign) {
      expect(r.isSecretValue(v), `expected value ${v} NOT to match a secret pattern`).toBe(false);
    }
  });
});

describe("Redactor config", () => {
  test("allow list preserves value", () => {
    const r = new Redactor({ allow: ["GITHUB_TOKEN"] });
    expect(r.isSecretKey("GITHUB_TOKEN"), "allow-list must win over default deny").toBe(false);
    // Sibling secrets still redacted.
    expect(r.isSecretKey("AWS_SECRET_ACCESS_KEY")).toBe(true);
  });

  test("custom deny keys, case-insensitive", () => {
    const r = new Redactor({ deny: ["MY_CUSTOM_FIELD"] });
    expect(r.isSecretKey("MY_CUSTOM_FIELD")).toBe(true);
    expect(r.isSecretKey("my_custom_field")).toBe(true);
  });

  test("disabled turns everything off", () => {
    const r = new Redactor({ disabled: true });
    expect(r.isSecretKey("GITHUB_TOKEN")).toBe(false);
    expect(r.isSecretValue("ghp_0123456789abcdefghijklmnopqrstuvwxyz")).toBe(false);
  });

  test("allow list suppresses value-pattern match", () => {
    // Allow-listing a key must also suppress value-pattern redaction for
    // that key, otherwise the escape hatch is useless for a var whose
    // value happens to look like a token.
    const r = new Redactor({ allow: ["FIXTURE_TOKEN"] });
    const { value, redacted } = r.redactField(
      "FIXTURE_TOKEN",
      "ghp_0123456789abcdefghijklmnopqrstuvwxyz"
    );
    expect(redacted).toBe(false);
    expect(value).toBe("ghp_0123456789abcdefghijklmnopqrstuvwxyz");
  });

  test("config from env", () => {
    process.env[ENV_REDACT_DISABLE] = "1";
    expect(redactConfigFromEnv().disabled).toBe(true);

    process.env[ENV_REDACT_DISABLE] = "";
    process.env[ENV_REDACT_ALLOW] = "FOO_TOKEN, BAR_KEY";
    process.env[ENV_REDACT_DENY] = "CUSTOM_A,CUSTOM_B";
    const cfg = redactConfigFromEnv();
    expect(cfg.disabled).toBe(false);
    expect(cfg.allow).toEqual(["FOO_TOKEN", "BAR_KEY"]);
    expect(cfg.deny).toEqual(["CUSTOM_A", "CUSTOM_B"]);
  });
});

describe("Redactor redactField", () => {
  test("composition of name and value matching", () => {
    const r = new Redactor();

    // Secret by key name.
    let res = r.redactField("GITHUB_TOKEN", "anything");
    expect(res.redacted).toBe(true);
    expect(res.value).toBe("<redacted:GITHUB_TOKEN>");

    // Secret by value shape, benign key.
    res = r.redactField("MY_VAR", "ghp_0123456789abcdefghijklmnopqrstuvwxyz");
    expect(res.redacted).toBe(true);
    expect(res.value).toBe("<redacted:MY_VAR>");

    // Benign key + benign value passes through untouched.
    res = r.redactField("PATH", "/usr/bin");
    expect(res.redacted).toBe(false);
    expect(res.value).toBe("/usr/bin");

    // Empty value on a secret key: nothing to leak, leave it alone.
    res = r.redactField("GITHUB_TOKEN", "");
    expect(res.redacted).toBe(false);
    expect(res.value).toBe("");
  });
});

describe("record-side redaction", () => {
  const adapter = new ExecAdapter();

  async function record(dir: string, req: ExecRequest, resp: ExecResponse): Promise<void> {
    const sess = new FileSession("record", new FileCassette(dir));
    await sess.record(adapter, req, () => Promise.resolve(resp));
  }

  test("secret env value never hits disk", async () => {
    const dir = tmpDir();
    await record(
      dir,
      {
        argv: ["gh", "pr", "view", "1"],
        env: { GITHUB_TOKEN: secretToken, PATH: "/usr/local/bin:/usr/bin" },
      },
      { stdout: "ok\n", exit_code: 0 }
    );

    const onDisk = readAll(dir);
    expect(onDisk).not.toContain(secretToken);
    expect(onDisk).toContain("<redacted:GITHUB_TOKEN>");
    // Benign env survives — redaction must not nuke useful debugging context.
    expect(onDisk).toContain("/usr/local/bin:/usr/bin");
  });

  test("value-pattern secret never hits disk", async () => {
    // The env var name gives no hint, only the value shape does.
    const dir = tmpDir();
    await record(
      dir,
      { argv: ["deploy"], env: { DEPLOY_HANDLE: secretToken } },
      { stdout: "done\n", exit_code: 0 }
    );

    const onDisk = readAll(dir);
    expect(onDisk).not.toContain(secretToken);
    expect(onDisk).toContain("<redacted:DEPLOY_HANDLE>");
  });

  test("redaction is fingerprint-stable", async () => {
    // Fingerprints are computed from {argv, stdin} only, so redacting
    // env cannot shift them. If this fails, ports would disagree on
    // cassette filenames.
    const req = (): ExecRequest => ({
      argv: ["gh", "pr", "view", "1"],
      env: { GITHUB_TOKEN: secretToken },
    });

    const dirs = [tmpDir(), tmpDir()];
    for (const dir of dirs) {
      await record(dir, req(), { stdout: "ok\n", exit_code: 0 });
    }
    const [first, second] = dirs.map((d) => fs.readdirSync(d).sort());
    expect(first).toEqual(second);

    const fpDirect = await adapter.fingerprint(req());
    for (const name of first) {
      expect(name).toContain(fpDirect);
    }
  });

  test("redacted cassette bytes are identical across runs", async () => {
    const run = async (): Promise<string> => {
      const dir = tmpDir();
      const req: ExecRequest = {
        argv: ["gh", "auth", "status"],
        env: { GITHUB_TOKEN: secretToken, AWS_SECRET_ACCESS_KEY: "abc123" },
      };
      await record(dir, req, { stdout: "ok\n", exit_code: 0 });
      const fp = await adapter.fingerprint(req);
      const data = fs.readFileSync(path.join(dir, `exec-${fp}.req.yaml`), "utf8");
      // recorded_at is a timestamp and legitimately varies; drop it.
      return stripRecordedAt(data);
    };
    expect(await run()).toEqual(await run());
  });

  test("redacted cassette still replays", async () => {
    const dir = tmpDir();
    const req: ExecRequest = {
      argv: ["gh", "pr", "view", "7"],
      env: { GITHUB_TOKEN: secretToken },
    };
    await record(dir, req, { stdout: "title: hello\n", exit_code: 0 });

    const rep = new FileSession("replay", new FileCassette(dir));
    const got = await rep.record(adapter, req, () => {
      throw new Error("do() must not be called in replay mode");
    });
    expect(got.stdout).toBe("title: hello\n");
  });

  test("disable via env restores verbatim recording", async () => {
    process.env[ENV_REDACT_DISABLE] = "1";
    const dir = tmpDir();
    await record(
      dir,
      { argv: ["gh"], env: { GITHUB_TOKEN: secretToken } },
      { stdout: "ok\n", exit_code: 0 }
    );
    expect(readAll(dir)).toContain(secretToken);
  });

  test("allow list via env preserves named field only", async () => {
    process.env[ENV_REDACT_ALLOW] = "GITHUB_TOKEN";
    const dir = tmpDir();
    await record(
      dir,
      { argv: ["gh"], env: { GITHUB_TOKEN: secretToken, AWS_SECRET_ACCESS_KEY: "leakme" } },
      { stdout: "ok\n", exit_code: 0 }
    );
    const onDisk = readAll(dir);
    expect(onDisk).toContain(secretToken);
    expect(onDisk).not.toContain("leakme");
  });

  test("nested structures and non-string scalars preserved", async () => {
    const dir = tmpDir();
    const c = new FileCassette(dir);
    await c.save(
      "exec",
      "aabbccdd",
      {
        argv: ["svc"],
        config: {
          retries: 3,
          verbose: true,
          api_key: secretToken,
          endpoint: "https://example.com",
          nested_map: { password: "hunter2" },
        },
      },
      { stdout: "ok" }
    );

    const onDisk = readAll(dir);
    expect(onDisk).not.toContain(secretToken);
    expect(onDisk).not.toContain("hunter2");
    expect(onDisk).toContain("<redacted:API_KEY>");
    expect(onDisk).toContain("<redacted:PASSWORD>");
    // Non-string scalars keep their YAML type (not quoted into strings).
    expect(onDisk).toContain("retries: 3");
    expect(onDisk).toContain("verbose: true");
    expect(onDisk).toContain("https://example.com");
  });

  test("explicit redactor bypasses the environment", async () => {
    process.env[ENV_REDACT_DISABLE] = "1";
    const dir = tmpDir();
    const c = new FileCassette(dir, new Redactor());
    await c.save("exec", "aabbccdd", { env: { GITHUB_TOKEN: secretToken } }, { stdout: "ok" });
    const onDisk = readAll(dir);
    expect(onDisk).not.toContain(secretToken);
    expect(onDisk).toContain("<redacted:GITHUB_TOKEN>");
  });
});
