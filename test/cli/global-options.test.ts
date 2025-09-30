import { describe, it, expect } from "vitest";
import { spawnSync } from "node:child_process";

const cliEntry = "dist/esm/cli.js";

describe("CLI global options", () => {
  it("documents the --driver option", () => {
    const result = spawnSync("node", [cliEntry, "--help"], {
      encoding: "utf8"
    });

    if (result.status !== 0) {
      throw new Error(`CLI exited with ${result.status}\nstdout: ${result.stdout}\nstderr: ${result.stderr}`);
    }

    expect(result.stdout.toLowerCase()).toContain("--driver");
  });

  it("fails fast when driver does not match URL", () => {
    const result = spawnSync(
      "node",
      [cliEntry, "status", "--driver", "mysql", "--url", "postgres://example"],
      { encoding: "utf8" }
    );

    expect(result.status).not.toBe(0);
    expect(result.stderr.toLowerCase()).toContain("driver");
  });
});
