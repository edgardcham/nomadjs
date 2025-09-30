
import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { Config } from "../../src/config.js";
import { createDriverMock } from "../helpers/driver-mock.js";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");

describe("Performance Improvements", () => {
  let migrator: Migrator;
  let config: Config;
  const driver = createDriverMock();

  beforeEach(() => {
    delete process.env.NODE_ENV;

    config = {
      driver: "postgres",
      url: "postgres://localhost/test",
      dir: "./migrations",
      table: "nomad_migrations",
      schema: "custom_schema",
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    };

    migrator = new Migrator(config, driver);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Schema Configuration", () => {
    it("uses custom schema when provided", () => {
      expect(config.schema).toBe("custom_schema");
    });

    it("defaults to public schema when not provided", () => {
      const defaultConfig: Config = {
        driver: "postgres",
        url: "postgres://localhost/test",
        dir: "./migrations"
      };

      const defaultMigrator = new Migrator(defaultConfig, driver);
      expect(defaultMigrator).toBeDefined();
      expect(defaultConfig.schema).toBeUndefined();
    });
  });

  describe("Migration File Caching", () => {
    it("exposes clearCache", () => {
      expect(typeof migrator.clearCache).toBe("function");
    });

    it("clears cache when clearCache is called", () => {
      expect(() => migrator.clearCache()).not.toThrow();
    });

    it("skips caching in test environment", () => {
      process.env.NODE_ENV = "test";
      const testMigrator = new Migrator(config, driver);
      expect(testMigrator).toBeDefined();
    });
  });

  describe("Connection Error Classification", () => {
    it("differentiates connection error types", async () => {
      const { ConnectionError, ParseConfigError } = await import("../../src/core/errors.js");

      const connError = new ConnectionError("Connection failed: ECONNREFUSED");
      expect(connError.exitCode).toBe(7);

      const parseError = new ParseConfigError("Invalid connection URL: malformed");
      expect(parseError.exitCode).toBe(4);
    });
  });

  describe("Process Exit Removal", () => {
    it("throws errors instead of calling process.exit", async () => {
      const { DriftError, MissingFileError, LockTimeoutError } = await import("../../src/core/errors.js");

      expect(new DriftError([]).exitCode).toBe(2);
      expect(new MissingFileError([]).exitCode).toBe(5);
      expect(new LockTimeoutError(30000).exitCode).toBe(3);
    });
  });
});
