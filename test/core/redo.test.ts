import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import type { Config } from "../../src/config.js";
import { readFileSync } from "node:fs";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { detectHazards, validateHazards } from "../../src/core/hazards.js";
import { createDriverMock, type DriverConnectionMock, type DriverMock } from "../helpers/driver-mock.js";
import { ChecksumMismatchError, MissingFileError } from "../../src/core/errors.js";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");
vi.mock("../../src/core/hazards.js");

type AppliedRow = {
  version: bigint;
  name: string;
  checksum: string;
  appliedAt: Date;
  rolledBackAt: Date | null;
};

interface MigrationSection {
  statements: string[];
  notx?: boolean;
}

interface MockMigrationConfig {
  version: bigint;
  name: string;
  checksum: string;
  up: MigrationSection;
  down: MigrationSection;
}

interface EnqueueOptions {
  onStatement?: (sql: string) => Promise<void> | void;
  executionOverrides?: Partial<Omit<DriverConnectionMock, "runStatement">>;
}

describe.each(["postgres", "mysql"] as const)("Migrator.redo() (%s)", (flavor) => {
  let config: Config;
  let driver: DriverMock;
  let migrator: Migrator;

  beforeEach(() => {
    vi.clearAllMocks();

    driver = createDriverMock({ flavor });

    config = {
      driver: flavor,
      url: flavor === "mysql" ? "mysql://localhost/test" : "postgresql://localhost/test",
      dir: "./migrations",
      table: "nomad_migrations",
      schema: flavor === "postgres" ? "public" : undefined,
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    };

    migrator = new Migrator(config, driver);

    vi.mocked(detectHazards).mockReturnValue([]);
    vi.mocked(validateHazards).mockImplementation((hazards, hasNotx) => ({
      shouldSkipTransaction: Boolean(hasNotx),
      hazardsDetected: hazards || []
    }) as any);
    vi.mocked(filenameToVersion).mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : "0";
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function createAppliedRow(partial?: Partial<AppliedRow>): AppliedRow {
    return {
      version: 20240101120000n,
      name: "create_users",
      checksum: "abc123",
      appliedAt: new Date("2024-01-01T00:00:00Z"),
      rolledBackAt: null,
      ...partial
    };
  }

  function mockMigrationFile({ version, name, checksum, up, down }: MockMigrationConfig) {
    const filepath = `/migrations/${version}_${name}.sql`;
    vi.mocked(listMigrationFiles).mockReturnValue([filepath]);
    vi.mocked(readFileSync).mockReturnValue("-- mocked --");
    vi.mocked(calculateChecksum).mockReturnValue(checksum);
    vi.mocked(parseNomadSqlFile).mockReturnValue({
      up: { statements: up.statements, notx: up.notx ?? false },
      down: { statements: down.statements, notx: down.notx ?? false },
      tags: []
    } as any);
  }

  function enqueueConnections(appliedRows: AppliedRow[], options: EnqueueOptions = {}) {
    const { onStatement, executionOverrides = {} } = options;
    const ensureConn = driver.enqueueConnection({});
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(appliedRows)
    });

    const executionConn = driver.enqueueConnection({
      acquireLock: vi.fn().mockResolvedValue(true),
      releaseLock: vi.fn().mockResolvedValue(undefined),
      beginTransaction: vi.fn().mockResolvedValue(undefined),
      commitTransaction: vi.fn().mockResolvedValue(undefined),
      rollbackTransaction: vi.fn().mockResolvedValue(undefined),
      markMigrationRolledBack: vi.fn().mockResolvedValue(undefined),
      markMigrationApplied: vi.fn().mockResolvedValue(undefined),
      ...executionOverrides
    });

    const statements: string[] = [];
    executionConn.runStatement.mockImplementation(async (sql: string) => {
      statements.push(sql);
      if (onStatement) {
        await onStatement(sql);
      }
    });

    return {
      ensureConn,
      fetchConn,
      executionConn,
      statements
    };
  }

  describe("Basic Functionality", () => {
    it("redo executes down then up and updates version tracking", async () => {
      const appliedRow = createAppliedRow();
      const { executionConn, fetchConn, statements } = enqueueConnections([appliedRow]);

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: ["CREATE TABLE users (id INT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });

      await migrator.redo();

      expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalledTimes(1);
      if (flavor === "postgres") {
        expect(executionConn.beginTransaction).toHaveBeenCalledTimes(2);
        expect(executionConn.commitTransaction).toHaveBeenCalledTimes(2);
      } else {
        expect(executionConn.beginTransaction).not.toHaveBeenCalled();
        expect(executionConn.commitTransaction).not.toHaveBeenCalled();
      }
      expect(executionConn.rollbackTransaction).not.toHaveBeenCalled();
      expect(statements).toEqual([
        "DROP TABLE users;",
        "CREATE TABLE users (id INT);"
      ]);
      expect(executionConn.markMigrationRolledBack).toHaveBeenCalledWith(appliedRow.version);
      expect(executionConn.markMigrationApplied).toHaveBeenCalledWith({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum
      });
    });

    it("wraps statements in transactions when notx is absent", async () => {
      const appliedRow = createAppliedRow();
      const { executionConn } = enqueueConnections([appliedRow]);

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: ["CREATE TABLE users (id INT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });

      await migrator.redo();

      if (flavor === "postgres") {
        expect(executionConn.beginTransaction).toHaveBeenCalledTimes(2);
        expect(executionConn.commitTransaction).toHaveBeenCalledTimes(2);
      } else {
        expect(executionConn.beginTransaction).not.toHaveBeenCalled();
        expect(executionConn.commitTransaction).not.toHaveBeenCalled();
      }
    });

    it("honours notx sections", async () => {
      const appliedRow = createAppliedRow({ name: "create_index" });
      const { executionConn, statements } = enqueueConnections([appliedRow]);

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: ["CREATE INDEX CONCURRENTLY idx_users ON users(email);"], notx: true },
        down: { statements: ["DROP INDEX CONCURRENTLY idx_users;"], notx: true }
      });

      await migrator.redo();

      expect(executionConn.beginTransaction).not.toHaveBeenCalled();
      expect(executionConn.commitTransaction).not.toHaveBeenCalled();
      expect(statements).toEqual([
        "DROP INDEX CONCURRENTLY idx_users;",
        "CREATE INDEX CONCURRENTLY idx_users ON users(email);"
      ]);
    });
  });

  describe("Edge Cases", () => {
    it("throws when no migrations are applied", async () => {
      driver.enqueueConnection({});
      driver.enqueueConnection({ fetchAppliedMigrations: vi.fn().mockResolvedValue([]) });
      vi.mocked(listMigrationFiles).mockReturnValue([]);
      await expect(migrator.redo()).rejects.toThrow("No migrations to redo");
    });

    it("throws when migration file is missing", async () => {
      const appliedRow = createAppliedRow();
      driver.enqueueConnection({});
      driver.enqueueConnection({ fetchAppliedMigrations: vi.fn().mockResolvedValue([appliedRow]) });
      vi.mocked(listMigrationFiles).mockReturnValue([]);
      await expect(migrator.redo()).rejects.toThrow(MissingFileError);
    });

    it("allows checksum drift when allowDrift is set", async () => {
      config.allowDrift = true;
      migrator = new Migrator(config, driver);

      const appliedRow = createAppliedRow();
      const { executionConn } = enqueueConnections([appliedRow]);

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: "different", // parsed checksum
        up: { statements: ["CREATE TABLE users (id INT, name TEXT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });

      await migrator.redo();

      expect(executionConn.markMigrationApplied).toHaveBeenCalledWith({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: "different"
      });
    });

    it("rolls back when down direction fails", async () => {
      const appliedRow = createAppliedRow();
      const failure = new Error("Table does not exist");
      const { executionConn } = enqueueConnections([appliedRow], {
        onStatement: (sql) => {
          if (sql === "DROP TABLE users;") {
            throw failure;
          }
        }
      });

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: ["CREATE TABLE users (id INT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });

      await expect(migrator.redo()).rejects.toThrow("Table does not exist");
      if (flavor === "postgres") {
        expect(executionConn.rollbackTransaction).toHaveBeenCalledTimes(1);
      } else {
        expect(executionConn.rollbackTransaction).not.toHaveBeenCalled();
      }
    });

    it("handles empty up/down sections without executing SQL", async () => {
      const appliedRow = createAppliedRow({ name: "empty" });
      const { executionConn, statements } = enqueueConnections([appliedRow]);

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: [] },
        down: { statements: [] }
      });

      await migrator.redo();

      expect(statements).toEqual([]);
      expect(executionConn.markMigrationRolledBack).toHaveBeenCalledWith(appliedRow.version);
      expect(executionConn.markMigrationApplied).toHaveBeenCalledWith({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum
      });
    });

    it("acquires and releases the advisory lock", async () => {
      const appliedRow = createAppliedRow();
      const { executionConn } = enqueueConnections([appliedRow]);

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: ["CREATE TABLE users (id INT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });

      await migrator.redo();

      // acquireLock is called with 5000ms per-attempt timeout, not the full config.lockTimeout
      expect(executionConn.acquireLock).toHaveBeenCalledWith(expect.any(String), 5000);
      expect(executionConn.releaseLock).toHaveBeenCalledTimes(1);
    });
  });

  describe("Error Handling", () => {
    it("throws checksum mismatch without allowDrift", async () => {
      const appliedRow = createAppliedRow();
      driver.enqueueConnection({});
      driver.enqueueConnection({ fetchAppliedMigrations: vi.fn().mockResolvedValue([appliedRow]) });
      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: "different",
        up: { statements: ["CREATE TABLE users (id INT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });
      await expect(migrator.redo()).rejects.toThrow(ChecksumMismatchError);
    });

    it("fails fast when lock cannot be acquired", async () => {
      const appliedRow = createAppliedRow();
      config.lockTimeout = 5;
      migrator = new Migrator(config, driver);

      driver.enqueueConnection({});
      driver.enqueueConnection({ fetchAppliedMigrations: vi.fn().mockResolvedValue([appliedRow]) });

      const acquireLock = vi.fn().mockResolvedValue(false);
      const executionConn = driver.enqueueConnection({
        acquireLock,
        releaseLock: vi.fn().mockResolvedValue(undefined),
        beginTransaction: vi.fn().mockResolvedValue(undefined),
        commitTransaction: vi.fn().mockResolvedValue(undefined),
        rollbackTransaction: vi.fn().mockResolvedValue(undefined),
        markMigrationRolledBack: vi.fn().mockResolvedValue(undefined),
        markMigrationApplied: vi.fn().mockResolvedValue(undefined)
      });
      executionConn.runStatement.mockImplementation(async () => {});

      mockMigrationFile({
        version: appliedRow.version,
        name: appliedRow.name,
        checksum: appliedRow.checksum,
        up: { statements: ["CREATE TABLE users (id INT);"] },
        down: { statements: ["DROP TABLE users;"] }
      });

      await expect(migrator.redo()).rejects.toThrowError(/lock/);
      expect(acquireLock).toHaveBeenCalled();
    });
  });
});
