import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import { Migrator } from "../../src/core/migrator.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { listMigrationFiles } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import type { Config } from "../../src/config.js";

// Mock the pg module
vi.mock("pg", () => ({
  Pool: vi.fn(() => ({
    query: vi.fn(),
    end: vi.fn(),
    connect: vi.fn()
  }))
}));

// Mock file system
vi.mock("node:fs", () => ({
  readFileSync: vi.fn(),
  existsSync: vi.fn(),
  readdirSync: vi.fn()
}));

// Mock the files module
vi.mock("../../src/core/files.js", () => ({
  listMigrationFiles: vi.fn(() => []),
  filenameToVersion: vi.fn((path: string) => {
    const match = path.match(/(\d+)/);
    return match ? match[1] : "0";
  })
}));

// Mock the parser
vi.mock("../../src/parser/enhanced-parser.js", () => ({
  parseNomadSqlFile: vi.fn(() => ({
    up: { statements: ["CREATE TABLE test (id INT);"], notx: false },
    down: { statements: ["DROP TABLE test;"], notx: false },
    noTransaction: false,
    tags: []
  }))
}));

describe("Migrator with Checksum Support", () => {
  let migrator: Migrator;
  let mockPool: any;
  let queryMock: ReturnType<typeof vi.fn>;
  let listMigrationFilesMock: ReturnType<typeof vi.fn>;
  let readFileSyncMock: ReturnType<typeof vi.fn>;
  let parseNomadSqlFileMock: ReturnType<typeof vi.fn>;

  const config: Config = {
    driver: "postgres",
    url: "postgresql://test:test@localhost:5432/testdb",
    dir: "/test/migrations",
    table: "nomad_migrations",
    allowDrift: false // Default to strict mode
  };

  // Helper function to create a query mock that handles advisory locks
  const createQueryMockWithLocking = () => {
    return vi.fn(async (sql: string, params?: any[]) => {
      // Always handle advisory lock queries
      if (typeof sql === 'string') {
        if (sql.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: true }] };
        }
        if (sql.includes("pg_advisory_unlock")) {
          return { rows: [{ pg_advisory_unlock: true }] };
        }
      }
      // Default response for other queries
      return { rows: [] };
    });
  };

  beforeEach(() => {
    vi.clearAllMocks();

    // Mock console.log to prevent output during tests
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});

    // Mock process.on/off to prevent signal handler issues
    vi.spyOn(process, 'on').mockImplementation(() => process);
    vi.spyOn(process, 'off').mockImplementation(() => process);
    vi.spyOn(process, 'exit').mockImplementation((() => {}) as any);

    // Create a unified query mock that handles all query types
    queryMock = createQueryMockWithLocking();

    // Create a mock client that uses the same query mock
    const mockClient = {
      query: queryMock,
      release: vi.fn()
    };

    const connectMock = vi.fn();
    connectMock.mockResolvedValue(mockClient);

    mockPool = {
      query: queryMock,
      end: vi.fn(),
      connect: connectMock
    };

    // Update mocked functions
    (Pool as any).mockImplementation(() => mockPool);
    listMigrationFilesMock = listMigrationFiles as any;
    readFileSyncMock = readFileSync as any;
    parseNomadSqlFileMock = parseNomadSqlFile as any;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("Migration table schema", () => {
    it("creates table with checksum column", async () => {
      migrator = new Migrator(config, mockPool);

      // Mock table creation
      queryMock.mockResolvedValueOnce({ rows: [] }); // Create table

      await migrator.ensureTable();

      expect(queryMock).toHaveBeenCalledTimes(1);
      const createTableCall = queryMock.mock.calls[0][0];
      expect(createTableCall).toContain("CREATE TABLE IF NOT EXISTS nomad_migrations");
      expect(createTableCall).toContain("checksum");
      expect(createTableCall).toContain("TEXT NOT NULL");
    });

    it("stores checksum when applying migration", async () => {
      migrator = new Migrator(config, mockPool);

      const content = "CREATE TABLE users (id INT);";
      const checksum = calculateChecksum(content);

      // Set up mocks
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql"
      ]);
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["DROP TABLE users;"], notx: false },
        noTransaction: false
      });

      // Override the default queryMock to handle specific queries
      let callCount = 0;
      queryMock.mockImplementation(async (sql: string, params?: any[]) => {
        // Handle advisory lock queries (always succeed)
        if (sql.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: true }] };
        }
        if (sql.includes("pg_advisory_unlock")) {
          return { rows: [{ pg_advisory_unlock: true }] };
        }

        // Handle other queries in sequence
        callCount++;
        return { rows: [] };
      });

      await migrator.up(1);

      // Find the INSERT query
      const insertCall = queryMock.mock.calls.find((call: any[]) =>
        call[0]?.includes("INSERT INTO nomad_migrations")
      );

      expect(insertCall).toBeDefined();
      expect(insertCall[0]).toContain("checksum");
      expect(insertCall[1]).toContain(checksum);
    });

    it("retrieves checksum when fetching applied migrations", async () => {
      migrator = new Migrator(config, mockPool);

      const mockRows = [
        {
          version: "20250921112233",
          name: "create_users",
          checksum: "abc123def456",
          applied_at: new Date(),
          rolled_back_at: null
        }
      ];

      // Mock the SELECT query for getAppliedMigrations
      queryMock.mockResolvedValueOnce({ rows: mockRows });

      const applied = await migrator.getAppliedMigrations();

      expect(applied).toHaveLength(1);
      expect(applied[0].checksum).toBe("abc123def456");
    });
  });

  describe("Drift detection", () => {
    it("throws error when applied migration has different checksum", async () => {
      migrator = new Migrator(config, mockPool);

      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      // Mock applied migration with original checksum
      queryMock.mockResolvedValueOnce({
        rows: [{
          version: "20250921112233",
          name: "create_users",
          checksum: calculateChecksum(originalContent),
          applied_at: new Date(),
          rolled_back_at: null
        }]
      });

      // Mock current file with modified content
      const currentMigration = {
        version: 20250921112233n,
        name: "create_users",
        filepath: "/test/migrations/20250921112233_create_users.sql",
        content: modifiedContent,
        checksum: calculateChecksum(modifiedContent),
        parsed: {
          up: { statements: [] },
          down: { statements: [] },
          noTransaction: false
        }
      };

      await expect(
        migrator.verifyChecksum(currentMigration)
      ).rejects.toThrow("Checksum mismatch");
    });

    it("allows drift when allowDrift flag is set", async () => {
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      // Mock applied migration
      queryMock.mockResolvedValueOnce({
        rows: [{
          version: "20250921112233",
          name: "create_users",
          checksum: calculateChecksum(originalContent),
          applied_at: new Date(),
          rolled_back_at: null
        }]
      });

      const currentMigration = {
        version: 20250921112233n,
        name: "create_users",
        filepath: "/test/migrations/20250921112233_create_users.sql",
        content: modifiedContent,
        checksum: calculateChecksum(modifiedContent),
        parsed: {
          up: { statements: [] },
          down: { statements: [] },
          noTransaction: false
        }
      };

      // Should not throw with allowDrift
      await expect(
        migrator.verifyChecksum(currentMigration)
      ).resolves.toBeUndefined();
    });

    it("logs warning when drift is allowed", async () => {
      const consoleSpy = vi.spyOn(console, "warn");
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      queryMock.mockResolvedValueOnce({
        rows: [{
          version: "20250921112233",
          name: "create_users",
          checksum: calculateChecksum(originalContent),
          applied_at: new Date()
        }]
      });

      const currentMigration = {
        version: 20250921112233n,
        name: "create_users",
        filepath: "/test/migrations/20250921112233_create_users.sql",
        content: modifiedContent
      };

      await migrator.verifyChecksum(currentMigration);

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining("WARNING: Checksum mismatch")
      );
    });
  });

  describe("Status command with checksums", () => {
    it("shows drift status for each migration", async () => {
      // Use config with allowDrift so status returns instead of throwing
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      // Set up file mocks

      const content1 = "CREATE TABLE users (id INT);";
      const content2 = "ALTER TABLE users ADD email TEXT;";

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql",
        "/test/migrations/20250921112234_add_column.sql"
      ]);

      readFileSyncMock
        .mockReturnValueOnce(content1)
        .mockReturnValueOnce(content2);

      parseNomadSqlFileMock
        .mockReturnValueOnce({ up: { statements: [] }, down: { statements: [] }, noTransaction: false })
        .mockReturnValueOnce({ up: { statements: [] }, down: { statements: [] }, noTransaction: false });

      // Mock queries
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20250921112233",
              name: "create_users",
              checksum: calculateChecksum(content1), // Matches
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20250921112234",
              name: "add_column",
              checksum: "different_checksum", // Drift!
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(2);
      expect(status[0].hasDrift).toBeFalsy();
      expect(status[1].hasDrift).toBe(true);
    });

    it("detects missing migration files", async () => {
      // Use config with allowDrift so status returns instead of throwing
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      // Mock no files found
      listMigrationFilesMock.mockReturnValue([]);

      // Mock queries
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [{
            version: "20250921112233",
            name: "create_users",
            checksum: "abc123",
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].isMissing).toBe(true);
    });

    it("returns exit code 2 when drift detected", async () => {
      migrator = new Migrator(config, mockPool);

      // Mock file with different checksum
      const content = "CREATE TABLE users (id INT);";
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql"
      ]);
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [] },
        down: { statements: [] },
        noTransaction: false
      });

      // Mock queries
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [{
            version: "20250921112233",
            name: "create_users",
            checksum: "wrong_checksum", // Different from actual file
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      try {
        await migrator.status();
        expect(true).toBe(false); // Should have thrown
      } catch (error: any) {
        expect(error.exitCode).toBe(2);
        expect(error.message).toContain("Drift detected");
      }
    });

    it("returns exit code 5 when file missing", async () => {
      migrator = new Migrator(config, mockPool);

      // Mock no files found
      listMigrationFilesMock.mockReturnValue([]);

      // Mock queries
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [{
            version: "20250921112233",
            name: "create_users",
            checksum: "abc123",
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      try {
        await migrator.status();
        expect(true).toBe(false); // Should have thrown
      } catch (error: any) {
        expect(error.exitCode).toBe(5);
        expect(error.message).toContain("Missing");
      }
    });
  });

  describe("Verify command", () => {
    it("checks all migration checksums without applying", async () => {
      migrator = new Migrator(config, mockPool);

      const content1 = "CREATE TABLE users (id INT);";
      const content2 = "ALTER TABLE users ADD email TEXT;";

      // Mock files
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql",
        "/test/migrations/20250921112234_add_column.sql"
      ]);
      readFileSyncMock
        .mockReturnValueOnce(content1)
        .mockReturnValueOnce(content2);
      parseNomadSqlFileMock
        .mockReturnValueOnce({ up: { statements: [] }, down: { statements: [] }, noTransaction: false })
        .mockReturnValueOnce({ up: { statements: [] }, down: { statements: [] }, noTransaction: false });

      // Mock queries
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20250921112233",
              name: "create_users",
              checksum: calculateChecksum(content1),
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20250921112234",
              name: "add_column",
              checksum: calculateChecksum(content2),
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const result = await migrator.verify();

      expect(result.valid).toBe(true);
      expect(result.driftCount).toBe(0);
    });

    it("reports drift count and details", async () => {
      migrator = new Migrator(config, mockPool);

      const content1 = "CREATE TABLE users (id INT);";
      const content2 = "ALTER TABLE users ADD email TEXT;";

      // Mock files
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql",
        "/test/migrations/20250921112234_add_column.sql"
      ]);
      readFileSyncMock
        .mockReturnValueOnce(content1)
        .mockReturnValueOnce(content2);
      parseNomadSqlFileMock
        .mockReturnValueOnce({ up: { statements: [] }, down: { statements: [] }, noTransaction: false })
        .mockReturnValueOnce({ up: { statements: [] }, down: { statements: [] }, noTransaction: false });

      // Mock queries with one wrong checksum
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20250921112233",
              name: "create_users",
              checksum: calculateChecksum(content1), // Correct
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20250921112234",
              name: "add_column",
              checksum: "wrong_checksum", // Wrong!
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const result = await migrator.verify();

      expect(result.valid).toBe(false);
      expect(result.driftCount).toBe(1);
      expect(result.driftedMigrations).toHaveLength(1);
      expect(result.driftedMigrations[0].version).toBe(20250921112234n);
    });
  });

  describe("Rollback with checksum verification", () => {
    it("verifies checksum before rolling back", async () => {
      migrator = new Migrator(config, mockPool);

      const content = "CREATE TABLE users (id INT);";

      // Mock files
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql"
      ]);
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["DROP TABLE users;"], notx: false },
        noTransaction: false
      });

      // The connection is already mocked in beforeEach with proper advisory lock handling

      // Set up query responses
      let queryIndex = 0;
      const responses = [
        { rows: [] }, // ensureTable check
        { rows: [{    // getAppliedMigrations
          version: BigInt(20250921112233),
          name: "create_users",
          checksum: calculateChecksum(content),
          applied_at: new Date(),
          rolled_back_at: null
        }]},
        { rows: [] }, // BEGIN transaction
        { rows: [] }, // DROP TABLE users;
        { rows: [] }, // UPDATE nomad_migrations
        { rows: [] }  // COMMIT
      ];

      queryMock.mockImplementation(async (sql: string, params?: any[]) => {
        // Always handle advisory lock queries
        if (sql.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: true }] };
        }
        if (sql.includes("pg_advisory_unlock")) {
          return { rows: [{ pg_advisory_unlock: true }] };
        }

        // Return the appropriate response for other queries
        if (queryIndex < responses.length) {
          return responses[queryIndex++];
        }
        return { rows: [] };
      });

      await migrator.down(1);

      // Should have executed rollback
      const dropTableCall = queryMock.mock.calls.find(call =>
        call[0] && call[0].includes("DROP TABLE")
      );
      expect(dropTableCall).toBeDefined();
      expect(dropTableCall[0]).toContain("DROP TABLE users;");
    });

    it("prevents rollback if checksum doesn't match", async () => {
      migrator = new Migrator(config, mockPool);

      const content = "CREATE TABLE users (id INT);";

      // Mock files
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20250921112233_create_users.sql"
      ]);
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [] },
        down: { statements: [] },
        noTransaction: false
      });

      // Mock queries with wrong checksum
      queryMock.mockImplementation(async (sql: string) => {
        // Always handle advisory lock queries
        if (sql.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: true }] };
        }
        if (sql.includes("pg_advisory_unlock")) {
          return { rows: [{ pg_advisory_unlock: true }] };
        }

        // Handle ensureTable
        if (sql.includes("CREATE TABLE IF NOT EXISTS")) {
          return { rows: [] };
        }

        // Handle getAppliedMigrations - return wrong checksum
        if (sql.includes("SELECT version, name, checksum")) {
          return {
            rows: [{
              version: "20250921112233",  // Note: returned as string from DB
              name: "create_users",
              checksum: "wrong_checksum", // Different from file!
              applied_at: new Date(),
              rolled_back_at: null
            }]
          };
        }

        return { rows: [] };
      });

      await expect(migrator.down(1)).rejects.toThrow("Checksum mismatch");
    });
  });

  describe("Edge cases", () => {
    it("handles migration with no checksum (legacy)", async () => {
      // Use config with allowDrift for legacy migrations
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      // Mock no files (checking legacy applied migrations)
      listMigrationFilesMock.mockReturnValue([]);

      // Mock queries
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [{
            version: "20250921112233",
            name: "create_users",
            checksum: null, // Legacy migration without checksum
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].hasLegacyChecksum).toBe(true);
      expect(status[0].hasDrift).toBeUndefined(); // Can't determine drift for legacy
    });

    it("calculates checksum lazily for large migrations", async () => {
      migrator = new Migrator(config, mockPool);

      // Large migration content
      const largeContent = "SELECT 1;\n".repeat(100_000);
      const migration = {
        version: 20250921112233n,
        name: "large_migration",
        content: largeContent
      };

      const start = Date.now();
      const checksum = calculateChecksum(migration.content);
      const elapsed = Date.now() - start;

      expect(checksum).toHaveLength(64);
      expect(elapsed).toBeLessThan(500); // Should be fast even for large files
    });
  });
});