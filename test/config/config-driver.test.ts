import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveRuntimeConfig, resetConfigCache } from "../../src/config.js";
import { ParseConfigError } from "../../src/core/errors.js";

describe("resolveRuntimeConfig driver handling", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    resetConfigCache();
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

  it("prefers CLI driver over env and config", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-driver-cli-"));
    try {
      writeFileSync(
        join(cwd, "nomad.toml"),
        `# comment\n[database]\nurl = "mysql://from-config"\ndriver = "postgres"\n`
      );
      process.env.NOMAD_DRIVER = "postgres";
      const config = resolveRuntimeConfig({
        cli: { url: "mysql://cli", driver: "mysql" } as any,
        cwd
      });
      expect((config as any).driver).toBe("mysql");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("falls back to environment driver", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-driver-env-"));
    try {
      process.env.NOMAD_DRIVER = "mysql";
      const config = resolveRuntimeConfig({
        cli: { url: "mysql://env" },
        cwd
      });
      expect((config as any).driver).toBe("mysql");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("infers mysql driver from URL when unspecified", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-driver-infer-"));
    try {
      const config = resolveRuntimeConfig({
        cli: { url: "mysql://example" },
        cwd
      });
      expect((config as any).driver).toBe("mysql");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("infers sqlite driver from URL when unspecified", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-driver-sqlite-infer-"));
    try {
      const config = resolveRuntimeConfig({
        cli: { url: "sqlite:///tmp/nomad.sqlite" },
        cwd
      });
      expect((config as any).driver).toBe("sqlite");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("falls back to environment sqlite driver", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-driver-env-sqlite-"));
    try {
      process.env.NOMAD_DRIVER = "sqlite";
      const config = resolveRuntimeConfig({
        cli: { url: "sqlite:///tmp/env.sqlite" },
        cwd
      });
      expect((config as any).driver).toBe("sqlite");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });

  it("throws when driver does not match URL scheme", () => {
    const cwd = mkdtempSync(join(tmpdir(), "nomad-config-driver-invalid-"));
    try {
      process.env.NOMAD_DRIVER = "mysql";
      expect(() =>
        resolveRuntimeConfig({
          cli: { url: "postgres://example" },
          cwd
        })
      ).toThrow(ParseConfigError);
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });
});
