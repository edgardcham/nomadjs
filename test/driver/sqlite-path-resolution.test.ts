import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { resolve as resolvePath, join } from "node:path";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolveSqliteFilename } from "../../src/driver/sqlite.js";

const normalize = (value: string): string => value.replace(/\\/g, "/");

describe("resolveSqliteFilename", () => {
  let cwd: string;
  let originalCwd: string;

  beforeEach(() => {
    originalCwd = process.cwd();
    cwd = mkdtempSync(join(tmpdir(), "nomad-sqlite-path-"));
    process.chdir(cwd);
  });

  afterEach(() => {
    process.chdir(originalCwd);
    rmSync(cwd, { recursive: true, force: true });
  });

  it("handles :memory: variants", () => {
    const variants = [":memory:", "sqlite::memory:", "sqlite:memory", "  :memory:  "];
    for (const value of variants) {
      const result = resolveSqliteFilename(value);
      expect(result).toEqual({ filename: ":memory:", isMemory: true });
    }
  });

  it("handles sqlite:/// absolute URLs", () => {
    const result = resolveSqliteFilename("sqlite:///absolute/path/db.sqlite");
    expect(normalize(result.filename)).toBe("/absolute/path/db.sqlite");
    expect(result.isMemory).toBe(false);
  });

  it("handles file:// URLs", () => {
    const result = resolveSqliteFilename("file:///tmp/nomad.db");
    expect(normalize(result.filename)).toBe("/tmp/nomad.db");
    expect(result.isMemory).toBe(false);
  });

  it("handles sqlite: relative paths", () => {
    const result = resolveSqliteFilename("sqlite:data/nomad.sqlite");
    expect(normalize(result.filename)).toBe(normalize(resolvePath(process.cwd(), "data/nomad.sqlite")));
    expect(result.isMemory).toBe(false);
  });

  it("handles plain filenames with extensions", () => {
    const result = resolveSqliteFilename("nomad.sqlite");
    expect(normalize(result.filename)).toBe(normalize(resolvePath(process.cwd(), "nomad.sqlite")));
    expect(result.isMemory).toBe(false);
  });

  it("handles absolute paths", () => {
    const result = resolveSqliteFilename("/var/lib/nomad.db");
    expect(normalize(result.filename)).toBe("/var/lib/nomad.db");
    expect(result.isMemory).toBe(false);
  });

  it("handles relative paths without scheme", () => {
    const result = resolveSqliteFilename("./data/db.sqlite");
    expect(normalize(result.filename)).toBe(normalize(resolvePath(process.cwd(), "./data/db.sqlite")));
    expect(result.isMemory).toBe(false);
  });

  it("handles sqlite:// URLs without triple slash", () => {
    const result = resolveSqliteFilename("sqlite://data/db.sqlite");
    expect(normalize(result.filename)).toBe("/data/db.sqlite");
    expect(result.isMemory).toBe(false);
  });
});
