import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

const cliEntry = "dist/esm/cli.js";

function runCli(args: string[], env: NodeJS.ProcessEnv = process.env) {
  const result = spawnSync("node", [cliEntry, ...args], {
    encoding: "utf8",
    env: { ...env }
  });

  return {
    status: result.status ?? -1,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? ""
  };
}

describe("CLI: nomad create --block", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "nomad-create-block-"));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("creates a migration template with block markers", () => {
    const result = runCli(["create", "block_test", "--dir", tempDir, "--block"]);

    if (result.status !== 0) {
      throw new Error(`CLI exited with ${result.status}\nstdout: ${result.stdout}\nstderr: ${result.stderr}`);
    }

    const files = readdirSync(tempDir).filter(name => name.endsWith(".sql"));
    expect(files.length).toBe(1);

    const content = readFileSync(join(tempDir, files[0]), "utf8");
    expect(content).toContain("-- +nomad block\n");
    expect(content).toContain("-- +nomad endblock");
    expect(content).toContain("-- +nomad Down\n");
    expect(content.includes("\r")).toBe(false);
  });

  it("documents the --block option in help output", () => {
    const result = runCli(["create", "--help"]);
    expect(result.status).toBe(0);
    expect(result.stdout.toLowerCase()).toContain("--block");
  });
});
