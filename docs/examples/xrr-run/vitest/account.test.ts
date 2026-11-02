import { spawnSync } from "node:child_process";
import { expect, test } from "vitest";

// Plain child_process: `xrr run` answers `az` from cassettes via PATH.
const az = (...args: string[]) => spawnSync("az", args, { encoding: "utf8" });

test("account show", () => {
  const p = az("account", "show");
  expect(p.status).toBe(0);
  expect(JSON.parse(p.stdout).name).toBe("contoso");
});

test("group list", () => {
  const names = JSON.parse(az("group", "list").stdout).map((g: { name: string }) => g.name);
  expect(names).toEqual(["rg-app", "rg-data"]);
});

test("unknown subcommand exit code", () => {
  expect(az("frobnicate").status).toBe(2);
});
