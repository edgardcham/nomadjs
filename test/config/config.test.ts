import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveRuntimeConfig, resetConfigCache } from "../../src/config.js";

describe("resolveRuntimeConfig", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    // Reset config cache
    resetConfigCache();

    // reset env mutations between tests
    for (const key of Object.keys(process.env)) {
      if (!(key in originalEnv)) {
        delete process.env[key];
      }
    }
    for (const [key, value] of Object.entries(originalEnv)) {
      process.env[key] = value as string;
    }
  });

  afterEach(() => {
    for (const key of Object.keys(process.env)) {
      if (!(key in originalEnv)) {
        delete process.env[key];
      }
    }
    for (const [key, value] of Object.entries(originalEnv)) {
      process.env[key] = value as string;
    }
  });

  it("falls back to defaults when nothing configured", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-default-"));
    try {
      // Clear any DATABASE_URL that might be set
      delete process.env.DATABASE_URL;
      delete process.env.NOMAD_DATABASE_URL;

      const config = resolveRuntimeConfig({ cli: {}, cwd });
      expect(config.dir).toBe("migrations");
      expect(config.url).toBeUndefined();
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("reads config from JSON file", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-json-"));
    try {
      // Clear any DATABASE_URL that might be set
      delete process.env.DATABASE_URL;
      delete process.env.NOMAD_DATABASE_URL;

      writeFileSync(
        join(cwd, "nomad.json"),
        JSON.stringify({
          database: { url: "postgres://user@example/db", table: "nomad_custom" },
          migrations: { dir: "./db/migrations" }
        })
      );

      const config = resolveRuntimeConfig({ cli: {}, cwd });
      expect(config.url).toBe("postgres://user@example/db");
      expect(config.table).toBe("nomad_custom");
      expect(config.dir).toBe("./db/migrations");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("reads config from TOML and merges env + CLI overrides", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-toml-"));
    try {
      writeFileSync(
        join(cwd, "nomad.toml"),
        `# comment\n[database]\nurl = "postgres://from-config"\ntable = "table_from_config"\n\n[migrations]\ndir = "./migs"\n`
      );
      writeFileSync(
        join(cwd, ".env"),
        "DATABASE_URL=postgres://from-env\nNOMAD_MIGRATIONS_DIR=./from-env-dir\n"
      );
      const config = resolveRuntimeConfig({
        cli: { url: "postgres://from-cli", table: "table_cli" },
        cwd
      });
      expect(config.url).toBe("postgres://from-cli");
      expect(config.table).toBe("table_cli");
      expect(config.dir).toBe("./from-env-dir");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });
});
