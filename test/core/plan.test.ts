import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import type { Config } from "../../src/config.js";
import type { PlanOptions, MigrationPlan, PlannedMigration } from "../../src/core/planner.js";

// Mock dependencies
vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Plan Command", () => {
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

    // Mock console to prevent output during tests
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});

    // Create query mock
    queryMock = vi.fn();
    mockPool = {
      query: queryMock,
      end: vi.fn(),
      connect: vi.fn().mockResolvedValue({
        query: queryMock,
        release: vi.fn()
      })
    };

    // Setup mocks
    (Pool as any).mockImplementation(() => mockPool);
    listMigrationFilesMock = listMigrationFiles as any;
    readFileSyncMock = readFileSync as any;
    parseNomadSqlFileMock = parseNomadSqlFile as any;
    filenameToVersionMock = filenameToVersion as any;

    // Mock filenameToVersion to extract version from path
    filenameToVersionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    migrator = new Migrator(config, mockPool);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("Plan UP migrations", () => {
    it("should show pending migrations in order", async () => {
      // Mock files on disk
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql",
        "/test/migrations/20240102130000_add_email.sql",
        "/test/migrations/20240103140000_add_index.sql"
      ]);

      const content1 = "CREATE TABLE users (id INT);";
      const content2 = "ALTER TABLE users ADD email TEXT;";
      const content3 = "CREATE INDEX idx_email ON users(email);";

      readFileSyncMock
        .mockReturnValueOnce(content1)
        .mockReturnValueOnce(content2)
        .mockReturnValueOnce(content3);

      parseNomadSqlFileMock
        .mockReturnValueOnce({
          up: { statements: [content1], notx: false },
          down: { statements: ["DROP TABLE users;"], notx: false },
          noTransaction: false
        })
        .mockReturnValueOnce({
          up: { statements: [content2], notx: false },
          down: { statements: ["ALTER TABLE users DROP COLUMN email;"], notx: false },
          noTransaction: false
        })
        .mockReturnValueOnce({
          up: { statements: [content3], notx: false },
          down: { statements: ["DROP INDEX idx_email;"], notx: false },
          noTransaction: false
        });

      // Mock applied migrations (first one already applied)
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "create_users",
            checksum: calculateChecksum(content1),
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const plan = await migrator.planUp();

      expect(plan.direction).toBe("up");
      expect(plan.migrations).toHaveLength(2);
      expect(plan.migrations[0].version).toBe(20240102130000n);
      expect(plan.migrations[1].version).toBe(20240103140000n);
      expect(plan.migrations[0].transaction).toBe(true);
      expect(plan.migrations[1].transaction).toBe(true);
    });

    it("should detect hazardous operations", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_index_concurrently.sql"
      ]);

      const hazardousSQL = "CREATE INDEX CONCURRENTLY idx_email ON users(email);";

      readFileSyncMock.mockReturnValue(hazardousSQL);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [hazardousSQL], notx: false },
        down: { statements: ["DROP INDEX idx_email;"], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({ rows: [] }); // no applied migrations

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].hazards).toHaveLength(1);
      expect(plan.migrations[0].hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
      expect(plan.migrations[0].transaction).toBe(false); // Should auto-detect no transaction needed
      expect(plan.summary.hazardCount).toBe(1);
    });

    it("should respect notx directive", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_vacuum.sql"
      ]);

      const vacuumSQL = "VACUUM ANALYZE users;";

      readFileSyncMock.mockReturnValue(vacuumSQL);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [vacuumSQL], notx: true }, // notx directive set
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      expect(plan.migrations[0].transaction).toBe(false);
      expect(plan.migrations[0].reason).toBe("notx directive");
    });

    it("should limit migrations when specified", async () => {
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

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp({ limit: 2 });

      expect(plan.migrations).toHaveLength(2);
      expect(plan.migrations[0].version).toBe(20240101120000n);
      expect(plan.migrations[1].version).toBe(20240102130000n);
    });
  });

  describe("Plan DOWN migrations", () => {
    it("should show rollback plan in reverse order", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql",
        "/test/migrations/20240102130000_add_email.sql"
      ]);

      const content1 = "CREATE TABLE users (id INT);";
      const content2 = "ALTER TABLE users ADD email TEXT;";

      readFileSyncMock
        .mockReturnValueOnce(content1)
        .mockReturnValueOnce(content2);

      parseNomadSqlFileMock
        .mockReturnValueOnce({
          up: { statements: [content1], notx: false },
          down: { statements: ["DROP TABLE users;"], notx: false },
          noTransaction: false
        })
        .mockReturnValueOnce({
          up: { statements: [content2], notx: false },
          down: { statements: ["ALTER TABLE users DROP COLUMN email;"], notx: false },
          noTransaction: false
        });

      // Both migrations are applied
      queryMock
        .mockResolvedValueOnce({ rows: [] }) // ensureTable
        .mockResolvedValueOnce({
          rows: [
            {
              version: "20240101120000",
              name: "create_users",
              checksum: calculateChecksum(content1),
              applied_at: new Date("2024-01-01"),
              rolled_back_at: null
            },
            {
              version: "20240102130000",
              name: "add_email",
              checksum: calculateChecksum(content2),
              applied_at: new Date("2024-01-02"),
              rolled_back_at: null
            }
          ]
        });

      const plan = await migrator.planDown({ count: 2 });

      expect(plan.direction).toBe("down");
      expect(plan.migrations).toHaveLength(2);
      // Should be in reverse order
      expect(plan.migrations[0].version).toBe(20240102130000n);
      expect(plan.migrations[1].version).toBe(20240101120000n);
      expect(plan.migrations[0].statements[0]).toContain("DROP COLUMN email");
      expect(plan.migrations[1].statements[0]).toContain("DROP TABLE users");
    });

    it("should detect hazards in down migrations", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_reindex.sql"
      ]);

      const content = "CREATE INDEX idx_test ON users(email);";
      readFileSyncMock.mockReturnValue(content);

      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["REINDEX INDEX idx_test;"], notx: false }, // Hazardous operation
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "reindex",
            checksum: calculateChecksum(content),
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const plan = await migrator.planDown({ count: 1 });

      expect(plan.migrations[0].hazards).toHaveLength(1);
      expect(plan.migrations[0].hazards[0].type).toBe("REINDEX");
      expect(plan.migrations[0].transaction).toBe(false);
    });

    it("should handle already rolled back migrations", async () => {
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

      // Second migration is already rolled back
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
              rolled_back_at: new Date() // Already rolled back
            }
          ]
        });

      const plan = await migrator.planDown({ count: 2 });

      // Should only plan to rollback the first one
      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].version).toBe(20240101120000n);
    });
  });

  describe("Plan TO specific version", () => {
    it("should plan forward migration to target version", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql",
        "/test/migrations/20240103140000_three.sql",
        "/test/migrations/20240104150000_four.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["SELECT 2;"], notx: false },
        noTransaction: false
      });

      // First migration is applied
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

      const plan = await migrator.planTo({ version: 20240103140000n });

      expect(plan.direction).toBe("up");
      expect(plan.migrations).toHaveLength(2);
      expect(plan.migrations[0].version).toBe(20240102130000n);
      expect(plan.migrations[1].version).toBe(20240103140000n);
      // Should NOT include version 4
    });

    it("should plan backward migration to target version", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql",
        "/test/migrations/20240103140000_three.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["SELECT 2;"], notx: false },
        noTransaction: false
      });

      // All three are applied
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

      const plan = await migrator.planTo({ version: 20240101120000n });

      expect(plan.direction).toBe("down");
      expect(plan.migrations).toHaveLength(2);
      // Should rollback in reverse order
      expect(plan.migrations[0].version).toBe(20240103140000n);
      expect(plan.migrations[1].version).toBe(20240102130000n);
      // Should NOT rollback version 1 (target)
    });

    it("should return empty plan when already at target version", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql"
      ]);

      const content = "SELECT 1;";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      // Version 1 is applied, version 2 is not
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

      const plan = await migrator.planTo({ version: 20240101120000n });

      expect(plan.migrations).toHaveLength(0);
      expect(plan.summary.total).toBe(0);
    });
  });

  describe("JSON output format", () => {
    it("should generate valid JSON for up migrations", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_create_users.sql"
      ]);

      const content = "CREATE TABLE users (id INT);";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["DROP TABLE users;"], notx: false },
        noTransaction: false,
        tags: ["core", "users"]
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp({ format: "json" });

      expect(plan).toMatchObject({
        direction: "up",
        migrations: [{
          version: 20240101120000n,
          name: "create_users",
          filepath: "/test/migrations/20240101120000_create_users.sql",
          transaction: true,
          hazards: [],
          statements: [content],
          tags: ["core", "users"]
        }],
        summary: {
          total: 1,
          transactional: 1,
          nonTransactional: 0,
          hazardCount: 0
        }
      });

      // Should be serializable to JSON
      const jsonStr = JSON.stringify(plan, (_, v) =>
        typeof v === 'bigint' ? v.toString() : v
      );
      expect(() => JSON.parse(jsonStr)).not.toThrow();
    });

    it("should include hazard details in JSON", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_multi_hazard.sql"
      ]);

      const content = `
        CREATE INDEX CONCURRENTLY idx1 ON users(email);
        VACUUM ANALYZE users;
        REINDEX TABLE users;
      `;

      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: true },
        down: { statements: [], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp({ format: "json" });

      expect(plan.migrations[0].hazards).toHaveLength(3);
      expect(plan.migrations[0].hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
      expect(plan.migrations[0].hazards[1].type).toBe("VACUUM");
      expect(plan.migrations[0].hazards[2].type).toBe("REINDEX");
      expect(plan.summary.hazardCount).toBe(3);
    });
  });

  describe("Dry run mode", () => {
    it("should support dry-run flag for testing", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_test.sql"
      ]);

      const content = "CREATE TABLE test (id INT);";
      readFileSyncMock.mockReturnValue(content);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [content], notx: false },
        down: { statements: ["DROP TABLE test;"], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp({ dryRun: true });

      expect(plan.dryRun).toBe(true);
      expect(plan.migrations).toHaveLength(1);
      // Dry run should not actually modify the database
      // This will be tested in integration tests
    });
  });

  describe("Error handling", () => {
    it("should detect checksum mismatches in plan", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_drifted.sql"
      ]);

      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      readFileSyncMock.mockReturnValue(modifiedContent);
      parseNomadSqlFileMock.mockReturnValue({
        up: { statements: [modifiedContent], notx: false },
        down: { statements: ["DROP TABLE users;"], notx: false },
        noTransaction: false
      });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "drifted",
            checksum: calculateChecksum(originalContent), // Different checksum
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const plan = await migrator.planDown({ count: 1 });

      // Warnings should be in the summary, not individual migrations in current implementation
      expect(plan.summary.warnings).toBeDefined();
      expect(plan.summary.warnings).toHaveLength(1);
      expect(plan.summary.warnings[0]).toContain("Checksum mismatch");
    });

    it("should detect missing migration files", async () => {
      listMigrationFilesMock.mockReturnValue([]); // No files on disk

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            version: "20240101120000",
            name: "missing",
            checksum: "abc123",
            applied_at: new Date(),
            rolled_back_at: null
          }]
        });

      const plan = await migrator.planDown({ count: 1 });

      expect(plan.errors).toHaveLength(1);
      expect(plan.errors[0]).toContain("Migration file not found");
      expect(plan.migrations).toHaveLength(0);
    });
  });

  describe("Summary statistics", () => {
    it("should calculate correct summary", async () => {
      listMigrationFilesMock.mockReturnValue([
        "/test/migrations/20240101120000_one.sql",
        "/test/migrations/20240102130000_two.sql",
        "/test/migrations/20240103140000_three.sql"
      ]);

      readFileSyncMock
        .mockReturnValueOnce("CREATE TABLE users (id INT);")
        .mockReturnValueOnce("CREATE INDEX CONCURRENTLY idx ON users(id);")
        .mockReturnValueOnce("ALTER TABLE users ADD name TEXT;");

      parseNomadSqlFileMock
        .mockReturnValueOnce({
          up: { statements: ["CREATE TABLE users (id INT);"], notx: false },
          down: { statements: [], notx: false },
          noTransaction: false
        })
        .mockReturnValueOnce({
          up: { statements: ["CREATE INDEX CONCURRENTLY idx ON users(id);"], notx: true },
          down: { statements: [], notx: false },
          noTransaction: false
        })
        .mockReturnValueOnce({
          up: { statements: ["ALTER TABLE users ADD name TEXT;"], notx: false },
          down: { statements: [], notx: false },
          noTransaction: false
        });

      queryMock
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const plan = await migrator.planUp();

      expect(plan.summary.total).toBe(3);
      expect(plan.summary.transactional).toBe(2);
      expect(plan.summary.nonTransactional).toBe(1);
      expect(plan.summary.hazardCount).toBe(1);
    });
  });
});