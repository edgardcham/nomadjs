import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { writeDefaultConfig } from "../../src/core/files.js";

describe("writeDefaultConfig", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "nomad-test-"));
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("creates a TOML config file with default content", () => {
    const configPath = join(tempDir, "nomad.toml");
    writeDefaultConfig(configPath, "toml");

    expect(existsSync(configPath)).toBe(true);
    const content = readFileSync(configPath, "utf8");

    expect(content).toContain("# NomadJS configuration file");
    expect(content).toContain("[database]");
    expect(content).toContain("[migrations]");
    expect(content).toContain('dir = "migrations"');
    expect(content).toContain("# url = ");
    expect(content).toContain("# table = ");
  });

  it("creates a JSON config file with default content", () => {
    const configPath = join(tempDir, "nomad.json");
    writeDefaultConfig(configPath, "json");

    expect(existsSync(configPath)).toBe(true);
    const content = readFileSync(configPath, "utf8");

    expect(content).toContain("// NomadJS configuration file");
    expect(content).toContain('"database"');
    expect(content).toContain('"migrations"');
    expect(content).toContain('"dir": "migrations"');
    expect(content).toContain('// "url"');
    expect(content).toContain('// "table"');
  });

  it("throws error if file already exists", () => {
    const configPath = join(tempDir, "nomad.toml");
    writeDefaultConfig(configPath, "toml");

    expect(() => writeDefaultConfig(configPath, "toml")).toThrow();
  });

  it("defaults to TOML format when not specified", () => {
    const configPath = join(tempDir, "nomad.toml");
    writeDefaultConfig(configPath);

    expect(existsSync(configPath)).toBe(true);
    const content = readFileSync(configPath, "utf8");
    expect(content).toContain("[database]");
  });
});