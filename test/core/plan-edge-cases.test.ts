import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { Planner } from "../../src/core/planner.js";
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

describe("Plan Command Edge Cases", () => {
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
    allowDrift: false,
    autoNotx: false
  };

  beforeEach(() => {
    vi.clearAllMocks();

    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});

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

  describe("Empty and Edge Cases", () => {
    it("should handle no migrations directory gracefully", async () => {
      // Simulate ENOENT error from listMigrationFiles
      listMigrationFilesMock.mockImplementation(() => {
        const error: any = new Error("ENOENT");
        error.code = "ENOENT";
        throw error;
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({ rows: [] }); // getAppliedMigrations

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(0);
      expect(plan.summary.total).toBe(0);
    });

    it("should handle empty migration files", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_empty.sql"
      ]);

      readFileSyncMock.mockReturnValue(""); // Empty file
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].statements).toHaveLength(0);
    });

    it("should handle migrations with only comments", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_comments_only.sql"
      ]);

      const content = "-- This is just a comment\n-- Another comment";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].statements).toHaveLength(0);
    });
  });

  describe("Version Edge Cases", () => {
    it("should handle invalid target version", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql"
      ]);

      readFileSyncMock.mockReturnValue("SELECT 1;");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: ["SELECT 1;"], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      // Try to plan to a version that doesn't exist
      const plan = await migrator.planTo({ version: 99999999999999n });

      expect(plan.migrations).toHaveLength(1);
      expect(plan.direction).toBe("up");
    });

    it("should handle planning to version 0 (rollback all)", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["SELECT 2;"], notx: false },
        noTransaction: false
      });

      // Both migrations are applied
      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "one",
              checksum: calculateChecksum(content),
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20240102130000",
              name: "two",
              checksum: calculateChecksum(content),
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const plan = await migrator.planTo({ version: 0n });

      expect(plan.direction).toBe("down");
      expect(plan.migrations).toHaveLength(2);
    });
  });

  describe("Multiple Hazards and Complex SQL", () => {
    it("should handle nested hazards in transactions", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_complex.sql"
      ]);

      const content = `
        BEGIN;
        CREATE INDEX CONCURRENTLY idx1 ON users(email);
        COMMIT;
        VACUUM ANALYZE users;
      `;

      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      expect(plan.migrations[0].hazards).toHaveLength(2);
      expect(plan.migrations[0].transaction).toBe(false);
    });

    it("should handle SQL with dollar quotes containing hazard keywords", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_dollar_quotes.sql"
      ]);

      const content = `
        CREATE FUNCTION test() RETURNS void AS $$
        BEGIN
          -- This comment mentions VACUUM but shouldn't trigger hazard
          RAISE NOTICE 'REINDEX is mentioned here';
        END;
        $$ LANGUAGE plpgsql;
      `;

      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      // Should not detect hazards inside dollar quotes
      expect(plan.migrations[0].hazards).toHaveLength(0);
      expect(plan.migrations[0].transaction).toBe(true);
    });
  });

  describe("Concurrent Migrations", () => {
    it("should handle partially applied out-of-order migrations", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql",
        "/test/migrations/20240103140000_three.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      // Migration 1 and 3 are applied, but not 2
      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "one",
              checksum: calculateChecksum(content),
              applied_at: new Date(),
              rolled_back_at: null
            },
            {
              version: "20240103140000",
              name: "three",
              checksum: calculateChecksum(content),
              applied_at: new Date(),
              rolled_back_at: null
            }
          ]
        });

      const plan = await migrator.planUp();

      // Should only plan to apply migration 2
      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].version).toBe(20240102130000n);
    });
  });

  describe("Output Formatting", () => {
    it("should handle very long SQL statements in output", async () => {
      const planner = new Planner();

      const longStatement = "CREATE TABLE " + "x".repeat(100) + " (id INT)";

      const plan = {
        direction: "up" as const,
        migrations: [{
          version: 20240101120000n,
          name: "long_statement",
          filepath: "/test.sql",
          transaction: true,
          hazards: [],
          statements: [longStatement],
        }],
        summary: {
          total: 1,
          transactional: 1,
          nonTransactional: 0,
          hazardCount: 0
        }
      };

      const output = planner.formatPlanOutput(plan);

      // Should truncate long statements
      expect(output).toContain("...");
      expect(output.split("\n").some(line => line.length <= 80)).toBe(true);
    });

    it("should handle special characters in migration names", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_add-user's-table!.sql"
      ]);

      readFileSyncMock.mockReturnValue("CREATE TABLE users (id INT);");
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: ["CREATE TABLE users (id INT);"], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      expect(plan.migrations[0].name).toBe("add-user's-table!");
    });
  });

  describe("Error Recovery", () => {
    it("should handle database connection errors gracefully", async () => {
      queryMock.mockRejectedValue(new Error("Connection refused"));

      await expect(migrator.planUp()).rejects.toThrow("Connection refused");
    });

    it("should handle malformed migration files", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_malformed.sql"
      ]);

      readFileSyncMock.mockImplementation(() => {
        throw new Error("EACCES: permission denied");
      });

      await expect(migrator.planUp()).rejects.toThrow("permission denied");
    });
  });
});