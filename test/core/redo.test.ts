import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { Pool } from "pg";
import type { Config } from "../../src/config.js";
import { readFileSync } from "node:fs";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { detectHazards, validateHazards } from "../../src/core/hazards.js";

// Mock dependencies
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");
vi.mock("../../src/core/hazards.js");

describe("Migrator.redo()", () => {
  let migrator: Migrator;
  let dropShouldFail: boolean;
  let mockPool: any;
  let mockClient: any;
  let config: Config;

  beforeEach(() => {
    vi.clearAllMocks();
    dropShouldFail = false;

    const queryMock = vi.fn(async (sql: string) => {
      if (typeof sql === "string") {
        const trimmed = sql.trim();
        if (dropShouldFail && trimmed === "DROP TABLE users;") {
          throw new Error("Table does not exist");
        }
        if (trimmed.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: true }] };
        }
        if (trimmed.includes("pg_advisory_unlock")) {
          return { rows: [{ pg_advisory_unlock: true }] };
        }
      }
      return { rows: [] };
    });

    // Mock client
    mockClient = {
      query: queryMock,
      release: vi.fn()
    };

    // Mock pool
    mockPool = {
      query: queryMock, // Shared with client for connection-based queries
      connect: vi.fn().mockResolvedValue(mockClient)
    };

    // Config
    config = {
      driver: "postgres",
      url: "postgres://localhost/test",
      dir: "./migrations",
      table: "nomad_migrations",
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    };

    // Mock hazard detection
    vi.mocked(detectHazards).mockReturnValue([]);
    vi.mocked(validateHazards).mockImplementation((hazards, hasNotx) => ({
      shouldSkipTransaction: Boolean(hasNotx),
      hazardsDetected: hazards || []
    }) as any);

    // Mock filenameToVersion to extract version from filepath
    vi.mocked(filenameToVersion).mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : "0";
    });

    migrator = new Migrator(config, mockPool as unknown as Pool);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("Basic Functionality", () => {
    it("should redo the last applied migration", async () => {
      // Setup: One applied migration
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock pool queries in order
      mockPool.query.mockClear();
      mockPool.query
        .mockResolvedValueOnce({ rows: [] }) // ensureTable CREATE TABLE
        .mockResolvedValueOnce({ rows: appliedMigrations }); // getAppliedMigrations

      // Mock file system
      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      const migrationContent = `-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("abc123");
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: ["CREATE TABLE users (id INT);"],
          notx: false
        },
        down: {
          statements: ["DROP TABLE users;"],
          notx: false
        },
        tags: []
      });

      // Execute redo
      await migrator.redo();

      // Verify down was executed
      expect(mockClient.query).toHaveBeenCalledWith("DROP TABLE users;");

      // Verify up was executed after down
      expect(mockClient.query).toHaveBeenCalledWith("CREATE TABLE users (id INT);");

      // Verify version tracking updates
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("UPDATE"),
        expect.arrayContaining(["20240101120000"])
      );
    });

    it.skip("should redo a specific migration by version (removed feature)", async () => {
      // Setup: Multiple applied migrations
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        },
        {
          version: 20240102130000n,
          name: "create_posts",
          checksum: "def456",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      // Mock file system for first migration only
      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql",
        "/migrations/20240102130000_create_posts.sql"
      ]);

      const usersMigration = `-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockImplementation((path) => {
        if (path.toString().includes("20240101120000")) {
          return usersMigration;
        }
        return `-- +nomad Up\n-- +nomad Down`;
      });

      vi.mocked(calculateChecksum).mockImplementation((content) => {
        if (content === usersMigration) return "abc123";
        return "other";
      });

      vi.mocked(parseNomadSqlFile).mockImplementation((path) => {
        if (path.includes("20240101120000")) {
          return {
            up: {
              statements: ["CREATE TABLE users (id INT);"],
              notx: false
            },
            down: {
              statements: ["DROP TABLE users;"],
              notx: false
            },
            tags: []
          };
        }
        return { up: { statements: [], notx: false }, down: { statements: [], notx: false }, tags: [] };
      });

      // Execute redo for specific version
      await migrator.redo(20240101120000n);

      // Verify only the specified migration was redone
      expect(mockClient.query).toHaveBeenCalledWith("DROP TABLE users;");
      expect(mockClient.query).toHaveBeenCalledWith("CREATE TABLE users (id INT);");
      expect(mockClient.query).not.toHaveBeenCalledWith(expect.stringContaining("posts"));
    });

    it("should handle transaction wrapping correctly", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      const migrationContent = `-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("abc123");
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: ["CREATE TABLE users (id INT);"],
          notx: false
        },
        down: {
          statements: ["DROP TABLE users;"],
          notx: false
        },
        tags: []
      });

      await migrator.redo();

      // Verify transaction commands were used
      expect(mockClient.query).toHaveBeenCalledWith("BEGIN");
      expect(mockClient.query).toHaveBeenCalledWith("COMMIT");
    });

    it("should handle notx migrations correctly", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_index",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_index.sql"
      ]);

      const migrationContent = `-- +nomad Up
-- +nomad notx
CREATE INDEX CONCURRENTLY idx_users ON users(email);
-- +nomad Down
-- +nomad notx
DROP INDEX CONCURRENTLY idx_users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("abc123");
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: ["CREATE INDEX CONCURRENTLY idx_users ON users(email);"],
          notx: true
        },
        down: {
          statements: ["DROP INDEX CONCURRENTLY idx_users;"],
          notx: true
        },
        tags: []
      });

      await migrator.redo();

      // Verify no transaction commands were used
      expect(mockClient.query).not.toHaveBeenCalledWith("BEGIN");
      expect(mockClient.query).not.toHaveBeenCalledWith("COMMIT");

      // Verify the migrations were executed
      expect(mockClient.query).toHaveBeenCalledWith("DROP INDEX CONCURRENTLY idx_users;");
      expect(mockClient.query).toHaveBeenCalledWith("CREATE INDEX CONCURRENTLY idx_users ON users(email);");
    });
  });

  describe("Edge Cases", () => {
    it("should throw error when no migrations are applied", async () => {
      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call) - empty
      mockPool.query.mockResolvedValueOnce({
        rows: []
      });

      await expect(migrator.redo()).rejects.toThrow("No migrations to redo");
    });

    it.skip("should throw error when specified version is not found (feature removed)", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      await expect(migrator.redo(99999999999999n)).rejects.toThrow(
        "Migration 99999999999999 not found or not applied"
      );
    });

    it("should throw error when migration file is missing", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([]); // No files

      await expect(migrator.redo()).rejects.toThrow(/missing/i);
    });

    it("should handle checksum mismatch with allowDrift", async () => {
      config.allowDrift = true;
      migrator = new Migrator(config, mockPool as unknown as Pool);

      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      const migrationContent = `-- +nomad Up
CREATE TABLE users (id INT, name TEXT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("different123"); // Different checksum
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: ["CREATE TABLE users (id INT, name TEXT);"],
          notx: false
        },
        down: {
          statements: ["DROP TABLE users;"],
          notx: false
        },
        tags: []
      });

      await migrator.redo();

      // Should execute despite checksum mismatch
      expect(mockClient.query).toHaveBeenCalledWith("DROP TABLE users;");
      expect(mockClient.query).toHaveBeenCalledWith("CREATE TABLE users (id INT, name TEXT);");
    });

    it("should rollback on down failure", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      const migrationContent = `-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("abc123");
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: ["CREATE TABLE users (id INT);"],
          notx: false
        },
        down: {
          statements: ["DROP TABLE users;"],
          notx: false
        },
        tags: []
      });

      dropShouldFail = true;

      await expect(migrator.redo()).rejects.toThrow("Table does not exist");

      // Verify rollback was called
      expect(mockClient.query).toHaveBeenCalledWith("ROLLBACK");
    });

    it("should handle empty up/down sections", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "empty_migration",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_empty_migration.sql"
      ]);

      const migrationContent = `-- +nomad Up
-- Nothing to do
-- +nomad Down
-- Nothing to undo`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("abc123");
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: [],
          notx: false
        },
        down: {
          statements: [],
          notx: false
        },
        tags: []
      });

      await migrator.redo();

      // Should handle gracefully with no SQL executed - only updates to the migration table
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("UPDATE"),
        expect.arrayContaining(["20240101120000"])
      );
      // No actual migration SQL statements or transactions executed (since statements are empty)
      expect(mockClient.query).not.toHaveBeenCalledWith("BEGIN");
      expect(mockClient.query).not.toHaveBeenCalledWith("COMMIT");
      expect(mockClient.query).not.toHaveBeenCalledWith(expect.stringMatching(/CREATE|DROP|ALTER/));
    });

    it("should acquire and release advisory lock", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      const migrationContent = `-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("abc123");
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: {
          statements: ["CREATE TABLE users (id INT);"],
          notx: false
        },
        down: {
          statements: ["DROP TABLE users;"],
          notx: false
        },
        tags: []
      });

      await migrator.redo();

      const lockCalls = mockPool.query.mock.calls
        .map(call => call[0])
        .filter(sql => typeof sql === "string");
      expect(lockCalls.some(sql => (sql as string).includes("pg_try_advisory_lock"))).toBe(true);
      expect(lockCalls.some(sql => (sql as string).includes("pg_advisory_unlock"))).toBe(true);
    });
  });

  describe("Error Handling", () => {
    it("should provide clear error message for checksum mismatch without allowDrift", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      const migrationContent = `-- +nomad Up
CREATE TABLE users (id INT, name TEXT);
-- +nomad Down
DROP TABLE users;`;

      vi.mocked(readFileSync).mockReturnValue(migrationContent);
      vi.mocked(calculateChecksum).mockReturnValue("different123");

      await expect(migrator.redo()).rejects.toThrow(/checksum/i);
    });

    it("should handle concurrent redo operations", async () => {
      const appliedMigrations = [
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock ensureTable (first call)
      mockPool.query.mockResolvedValueOnce({ rows: [] });

      // Mock getAppliedMigrations (second call)
      mockPool.query.mockResolvedValueOnce({
        rows: appliedMigrations
      });

      // Mock file system so migration is found
      vi.mocked(listMigrationFiles).mockReturnValue([
        "/migrations/20240101120000_create_users.sql"
      ]);

      vi.mocked(readFileSync).mockReturnValue(`-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;`);

      vi.mocked(calculateChecksum).mockReturnValue("abc123"); // Match the checksum
      vi.mocked(parseNomadSqlFile).mockReturnValue({
        up: { statements: ["CREATE TABLE users (id INT);"], notx: false },
        down: { statements: ["DROP TABLE users;"], notx: false },
        tags: []
      });

      const { LockTimeoutError } = await import("../../src/core/errors.js");
      config.lockTimeout = 5;

      const originalImpl = mockPool.query.getMockImplementation();
      mockPool.query.mockImplementation(async (...args: any[]) => {
        const [sql] = args;
        if (typeof sql === "string") {
          if (sql.includes("pg_try_advisory_lock")) {
            return { rows: [{ pg_try_advisory_lock: false }] };
          }
          if (sql.includes("pg_advisory_unlock")) {
            return { rows: [{ pg_advisory_unlock: true }] };
          }
          if (sql.includes("SELECT version") || sql.includes("FROM nomad_migrations")) {
            return { rows: appliedMigrations };
          }
        }
        return originalImpl ? await originalImpl(...args) : { rows: [] };
      });

      await expect(migrator.redo()).rejects.toThrow(LockTimeoutError);

      if (originalImpl) {
        mockPool.query.mockImplementation(originalImpl);
      }
    });
  });
});
