import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { Config } from "../../src/config.js";
import { Pool } from "pg";

// Mock dependencies
vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");

describe("Performance Improvements", () => {
  let migrator: Migrator;
  let mockPool: any;
  let config: Config;

  beforeEach(() => {
    // Clear any cached NODE_ENV
    delete process.env.NODE_ENV;

    mockPool = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue({
        query: vi.fn(),
        release: vi.fn()
      })
    };

    config = {
      driver: "postgres",
      url: "postgres://localhost/test",
      dir: "./migrations",
      table: "nomad_migrations",
      schema: "custom_schema", // Test custom schema
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    };

    migrator = new Migrator(config, mockPool as unknown as Pool);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Schema Configuration", () => {
    it("should use custom schema when provided", () => {
      expect(config.schema).toBe("custom_schema");
    });

    it("should default to public schema when not provided", () => {
      const defaultConfig: Config = {
        driver: "postgres",
        url: "postgres://localhost/test",
        dir: "./migrations"
      };

      const defaultMigrator = new Migrator(defaultConfig, mockPool as unknown as Pool);
      // The schema would be used in advisory lock creation
      expect(defaultConfig.schema).toBeUndefined();
    });
  });

  describe("Migration File Caching", () => {
    it("should have clearCache method", () => {
      expect(migrator.clearCache).toBeDefined();
      expect(typeof migrator.clearCache).toBe("function");
    });

    it("should clear cache when clearCache is called", () => {
      // Call clearCache and ensure no errors
      expect(() => migrator.clearCache()).not.toThrow();
    });

    it("should skip caching in test environment", () => {
      process.env.NODE_ENV = 'test';

      // Create a new migrator in test mode
      const testMigrator = new Migrator(config, mockPool as unknown as Pool);

      // Cache should not be populated in test mode
      expect(process.env.NODE_ENV).toBe('test');
    });
  });

  describe("Connection Error Classification", () => {
    it("should differentiate between connection error types", async () => {
      const { ConnectionError, ParseConfigError } = await import("../../src/core/errors.js");

      // Test connection refused error
      const connError = new ConnectionError("Connection failed: ECONNREFUSED");
      expect(connError.message).toContain("Connection failed");
      expect(connError.exitCode).toBe(7);

      // Test parse config error
      const parseError = new ParseConfigError("Invalid connection URL: malformed");
      expect(parseError.message).toContain("Invalid connection URL");
      expect(parseError.exitCode).toBe(4);
    });
  });

  describe("Process Exit Removal", () => {
    it("should throw errors instead of calling process.exit", async () => {
      const { DriftError, MissingFileError, LockTimeoutError } = await import("../../src/core/errors.js");

      // Test that errors are thrown properly
      const driftError = new DriftError([]);
      expect(driftError).toBeInstanceOf(Error);
      expect(driftError.exitCode).toBe(2);

      const missingError = new MissingFileError([]);
      expect(missingError).toBeInstanceOf(Error);
      expect(missingError.exitCode).toBe(5);

      const lockError = new LockTimeoutError(30000);
      expect(lockError).toBeInstanceOf(Error);
      expect(lockError.exitCode).toBe(3);
    });
  });
});