import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import type { Config } from "../../src/config.js";

// Mock dependencies
vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Status Command", () => {
  let migrator: Migrator;
  let mockPool: any;
  let queryMock: ReturnType<typeof vi.fn>;
  let listMigrationFilesMock: ReturnType<typeof vi.fn>;
  let readFileSyncMock: ReturnType<typeof vi.fn>;
  let parseNomadSqlFileMock: ReturnType<typeof vi.fn>;
  let filenameToVersionMock: ReturnType<typeof vi.fn>;

  const config: Config = {
    driver: "postgres",
    url: "postgresql://test:test@localhost:5432/testdb",
    dir: "/test/migrations",
    table: "nomad_migrations",
    allowDrift: false
  };

  beforeEach(() => {
    vi.clearAllMocks();

    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(process, 'exit').mockImplementation((() => {}) as any);

    queryMock = vi.fn();
    mockPool = {
      query: queryMock,
      end: vi.fn(),
      connect: vi.fn().mockResolvedValue({
        query: queryMock,
        release: vi.fn()
      })
    };

    (Pool as any).mockImplementation(() => mockPool);
    listMigrationFilesMock = listMigrationFiles as any;
    readFileSyncMock = readFileSync as any;
    parseNomadSqlFileMock = parseNomadSqlFile as any;
    filenameToVersionMock = filenameToVersion as any;

    filenameToVersionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    migrator = new Migrator(config, mockPool);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("Basic Status", () => {
    it("should show pending migrations", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql",
        "/test/migrations/20240102130000_add_email.sql"
      ]);

      const content1 = "CREATE TABLE users (id INT);";
      const content2 = "ALTER TABLE users ADD email TEXT;";

      readFileSyncMock
        .mockReturnValueOnce(content1)
        .mockReturnValueOnce(content2);

      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({ rows: [] }); // no applied migrations

      const status = await migrator.status();

      expect(status).toHaveLength(2);
      expect(status[0].version).toBe(20240101120000n);
      expect(status[0].applied).toBe(false);
      expect(status[1].version).toBe(20240102130000n);
      expect(status[1].applied).toBe(false);
    });

    it("should show applied migrations", async () => {
      const content = "CREATE TABLE users (id INT);";

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql"
      ]);

      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      const appliedAt = new Date("2024-01-01T12:00:00Z");

      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "create_users",
            checksum: calculateChecksum(content),
            applied_at: appliedAt,
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].version).toBe(20240101120000n);
      expect(status[0].applied).toBe(true);
      expect(status[0].appliedAt).toEqual(appliedAt);
      expect(status[0].hasDrift).toBeUndefined();
    });

    it("should show mixed applied and pending", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql",
        "/test/migrations/20240103140000_three.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "one",
            checksum: calculateChecksum(content),
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(3);
      expect(status[0].applied).toBe(true);
      expect(status[1].applied).toBe(false);
      expect(status[2].applied).toBe(false);
    });
  });

  describe("Tag filtering in status", () => {
    it("should include tags and filter by tags", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_seed.sql",
        "/test/migrations/20240102130000_users.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      // First file tagged seed, second untagged
      parseNomadSqlFileMock.mockImplementation((filepath: string) => {
        const isSeed = String(filepath).includes("20240101120000_seed");
        return {
          up: { statements: [content], notx: false },
          down: { statements: [], notx: false },
          noTransaction: false,
          tags: isSeed ? ["seed"] : undefined
        };
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({ rows: [] }) // no applied
        .mockResolvedValue({ rows: [] }); // subsequent calls

      const all = await migrator.status();
      expect(all).toHaveLength(2);
      expect(all[0].tags).toEqual(["seed"]);

      const filtered = await migrator.status({ tags: ["seed"] } as any);
      expect(filtered).toHaveLength(1);
      expect(filtered[0].name).toContain("seed");
      expect(filtered[0].tags).toEqual(["seed"]);
    });

    it("should exclude DB-only missing entries when a filter is active", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_seed.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false,
        tags: ["seed"]
      });

      // Applied includes an extra version that has no file (missing)
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [
            { version: "20240101120000", name: "seed", checksum: calculateChecksum(content), applied_at: new Date(), rolled_back_at: null },
            { version: "20240102130000", name: "ghost", checksum: calculateChecksum(content), applied_at: new Date(), rolled_back_at: null }
          ]
        });

      const filtered = await migrator.status({ tags: ["seed"] } as any);
      // Only the file-backed seed migration should appear
      expect(filtered.every(r => r.name !== "ghost")).toBe(true);
    });
  });

  describe("Drift Detection", () => {
    it("should detect drift when file checksum doesn't match", async () => {
      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      // Use allowDrift to get results without throwing
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql"
      ]);

      readFileSyncMock.mockReturnValue(modifiedContent);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "create_users",
            checksum: calculateChecksum(originalContent),
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status[0].hasDrift).toBe(true);
    });

    it("should throw with exit code 2 when drift detected without allowDrift", async () => {
      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql"
      ]);

      readFileSyncMock.mockReturnValue(modifiedContent);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "create_users",
            checksum: calculateChecksum(originalContent),
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      try {
        await migrator.status();
        expect(true).toBe(false); // Should have thrown
      } catch (error: any) {
        expect(error.message).toContain("Drift detected");
        expect(error.exitCode).toBe(2);
      }
    });

    it("should not throw when drift detected with allowDrift", async () => {
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql"
      ]);

      readFileSyncMock.mockReturnValue(modifiedContent);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "create_users",
            checksum: calculateChecksum(originalContent),
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status[0].hasDrift).toBe(true);
      expect(status).toHaveLength(1); // Should not throw
    });
  });

  describe("Missing Files", () => {
    it("should detect missing migration files", async () => {
      // Use allowDrift to get results without throwing
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      // No files on disk
      listMigrationFilesMock.mockReturnValue([]);

      // But migration was applied
      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "missing_migration",
            checksum: "abc123",
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].isMissing).toBe(true);
      expect(status[0].applied).toBe(true);
    });

    it("should throw with exit code 5 when missing files without allowDrift", async () => {
      listMigrationFilesMock.mockReturnValue([]);

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "missing_migration",
            checksum: "abc123",
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      try {
        await migrator.status();
        expect(true).toBe(false); // Should have thrown
      } catch (error: any) {
        expect(error.message).toContain("Missing");
        expect(error.exitCode).toBe(5);
      }
    });

    it("should not count rolled back migrations as missing", async () => {
      listMigrationFilesMock.mockReturnValue([]);

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "rolled_back",
            checksum: "abc123",
            applied_at: new Date("2024-01-01"),
            rolled_back_at: new Date("2024-01-02") // Rolled back
          }]
        });

      const status = await migrator.status();

      expect(status).toHaveLength(0); // Should not show rolled back migration
    });
  });

  describe("Legacy Migrations", () => {
    it("should handle migrations without checksums", async () => {
      listMigrationFilesMock.mockReturnValue([]);

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "legacy_migration",
            checksum: null, // No checksum (legacy)
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].hasLegacyChecksum).toBe(true);
      expect(status[0].hasDrift).toBeUndefined(); // Can't detect drift without checksum
    });
  });

  describe("JSON Output Format", () => {
    it("should format status as JSON", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql",
        "/test/migrations/20240102130000_add_email.sql"
      ]);

      const content = "CREATE TABLE users (id INT);";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false,
        tags: ["core", "users"]
      });

      const appliedAt = new Date("2024-01-01T12:00:00Z");

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "create_users",
            checksum: calculateChecksum(content),
            applied_at: appliedAt,
            rolled_back_at: null
          }]
        });

      const status = await migrator.status();

      // Format as JSON (convert BigInt to string)
      const jsonStr = JSON.stringify(status, (_, v) =>
        typeof v === 'bigint' ? v.toString() : v
      );
      const parsed = JSON.parse(jsonStr);

      expect(parsed).toHaveLength(2);
      expect(parsed[0].version).toBe("20240101120000");
      expect(parsed[0].applied).toBe(true);
      expect(parsed[0].appliedAt).toBe(appliedAt.toISOString());
      expect(parsed[1].version).toBe("20240102130000");
      expect(parsed[1].applied).toBe(false);
    });

    it("should include drift and missing flags in JSON", async () => {
      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql"
      ]);

      readFileSyncMock.mockReturnValue(modifiedContent);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "create_users",
              checksum: calculateChecksum(originalContent),
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20240102130000",
              name: "missing_migration",
              checksum: "abc123",
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      const status = await migrator.status();

      const jsonStr = JSON.stringify(status, (_, v) =>
        typeof v === 'bigint' ? v.toString() : v
      );
      const parsed = JSON.parse(jsonStr);

      expect(parsed[0].hasDrift).toBe(true);
      expect(parsed[1].isMissing).toBe(true);
    });
  });

  describe("Sorting", () => {
    it("should sort migrations by version", async () => {
      // Return files in wrong order
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240103140000_three.sql",
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const status = await migrator.status();

      expect(status[0].version).toBe(20240101120000n);
      expect(status[1].version).toBe(20240102130000n);
      expect(status[2].version).toBe(20240103140000n);
    });
  });

  describe("Edge Cases", () => {
    it("should handle empty migrations directory", async () => {
      listMigrationFilesMock.mockReturnValue([]);

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] }); // No applied migrations

      const status = await migrator.status();

      expect(status).toHaveLength(0);
    });

    it("should handle rolled back migrations that need re-application", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql",
        "/test/migrations/20240102130000_add_email.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "create_users",
              checksum: calculateChecksum("SELECT 1;"),
              applied_at: new Date("2024-01-01"),
              rolled_back_at: new Date("2024-01-02") // Rolled back
            }
          ]
        });

      const status = await migrator.status();

      // Should show both migrations as pending (rolled back is not counted as applied)
      expect(status).toHaveLength(2);
      expect(status[0].applied).toBe(false);
      expect(status[1].applied).toBe(false);
    });

    it("should handle multiple migrations with drift", async () => {
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql",
        "/test/migrations/20240103140000_three.sql"
      ]);

      readFileSyncMock.mockReturnValue("MODIFIED CONTENT");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "one",
              checksum: "original_checksum_1",
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20240102130000",
              name: "two",
              checksum: "original_checksum_2",
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const status = await migrator.status();

      expect(status[0].hasDrift).toBe(true);
      expect(status[1].hasDrift).toBe(true);
      expect(status[2].applied).toBe(false);
    });

    it("should handle multiple missing migration files", async () => {
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240103140000_three.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "missing_one",
              checksum: "abc123",
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20240102130000",
              name: "missing_two",
              checksum: "def456",
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const status = await migrator.status();

      expect(status.filter(s => s.isMissing)).toHaveLength(2);
      expect(status[0].isMissing).toBe(true);
      expect(status[1].isMissing).toBe(true);
      expect(status[2].applied).toBe(false);
    });

    it("should handle migration files with special characters in names", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create-users_table.sql",
        "/test/migrations/20240102130000_add#email@field.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const status = await migrator.status();

      expect(status).toHaveLength(2);
      expect(status[0].name).toBe("create-users_table");
      expect(status[1].name).toBe("add#email@field");
    });

    it("should handle database connection errors gracefully", async () => {
      queryMock.mockRejectedValueOnce(new Error("Connection refused"));

      try {
        await migrator.status();
        expect(true).toBe(false); // Should have thrown
      } catch (error: any) {
        expect(error.message).toContain("Connection refused");
      }
    });

    it("should handle mixed legacy and checksum migrations", async () => {
      const driftConfig = { ...config, allowDrift: true };
      migrator = new Migrator(driftConfig, mockPool);

      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "one",
              checksum: null, // Legacy, no checksum
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20240102130000",
              name: "two",
              checksum: calculateChecksum("SELECT 1;"), // Has checksum
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const status = await migrator.status();

      expect(status[0].hasLegacyChecksum).toBe(true);
      expect(status[0].hasDrift).toBeUndefined(); // Can't detect drift without checksum
      expect(status[1].hasLegacyChecksum).toBeUndefined();
      expect(status[1].hasDrift).toBeUndefined(); // Checksums match
    });

    it("should handle very long migration names", async () => {
      const longName = "a".repeat(200);
      listMigrationFilesMock.mockReturnValue([
        `/test/migrations/20240101120000_${longName}.sql`
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const status = await migrator.status();

      expect(status[0].name).toBe(longName);
      expect(status[0].name.length).toBe(200);
    });

    it("should handle database table not existing yet", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_initial.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      // First query creates table, second returns no rows
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable creates it
        .mockResolvedValueOnce({ rows: [] }); // No applied migrations

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].applied).toBe(false);
    });
  });
});
