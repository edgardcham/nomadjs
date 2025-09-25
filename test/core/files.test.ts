import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeSqlTemplate } from "../../src/core/files.js";

interface TestContext {
  tempDir: string;
}

describe("writeSqlTemplate", () => {
  let ctx: TestContext;

  beforeEach(() => {
    ctx = {
      tempDir: mkdtempSync(join(tmpdir(), "nomad-files-test-"))
    };
  });

  afterEach(() => {
    rmSync(ctx.tempDir, { recursive: true, force: true });
  });

  it("writes the default template by default", () => {
    const filePath = join(ctx.tempDir, "migration.sql");

    writeSqlTemplate(filePath);

    const content = readFileSync(filePath, "utf8");
    expect(content).toBe(`-- +nomad Up\n-- write your up migration here\n\n-- +nomad Down\n-- write your down migration here\n`);
    expect(content.includes("\r")).toBe(false);
  });

  it("writes a block template when requested", () => {
    const filePath = join(ctx.tempDir, "block_migration.sql");

    writeSqlTemplate(filePath, { block: true });

    const content = readFileSync(filePath, "utf8");
    expect(content.startsWith("-- +nomad Up\n")).toBe(true);
    expect(content).toContain("-- +nomad block\n");
    expect(content).toContain("-- Place multi-line statements here");
    expect(content).toContain("-- \\\.");
    expect(content).toContain("-- +nomad endblock\n\n-- +nomad Down\n");
    expect(content).toContain("-- write your down migration here");
    expect(content.includes("\r")).toBe(false);
  });

  it("throws when the destination file already exists", () => {
    const filePath = join(ctx.tempDir, "existing.sql");

    writeSqlTemplate(filePath);
    expect(() => writeSqlTemplate(filePath)).toThrow(/EEXIST/);
  });
});
