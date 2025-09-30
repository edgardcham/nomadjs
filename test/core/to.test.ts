import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { MockedFunction } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import type { Config } from "../../src/config.js";
import { ChecksumMismatchError, MissingFileError } from "../../src/core/errors.js";
import { readFileSync } from "node:fs";
import { createDriverMock, type DriverMock } from "../helpers/driver-mock.js";

type AppliedRow = {
  version: bigint;
  name: string;
  checksum: string;
  appliedAt: Date;
  rolledBackAt: Date | null;
};

type MigrationDef = {
  version: bigint;
  name: string;
  up: string[];
  down: string[];
  upNotx?: boolean;
  downNotx?: boolean;
  checksum: string;
  content: string;
};

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");

describe.each(["postgres", "mysql"] as const)("Migrator.to() (%s)", (flavor) => {
  let config: Config;
  let driver: DriverMock;
  let migrator: Migrator;
  let listMigrationFilesMock: MockedFunction<typeof listMigrationFiles>;
  let readFileSyncMock: MockedFunction<typeof readFileSync>;
  let parseNomadSqlFileMock: MockedFunction<typeof parseNomadSqlFile>;
  let filenameToVersionMock: MockedFunction<typeof filenameToVersion>;
  let migrationsByPath: Map<string, MigrationDef>;
  let checksumByContent: Map<string, string>;

  beforeEach(() => {
    driver = createDriverMock({ flavor });

    config = {
      driver: flavor,
      url: flavor === "mysql" ? "mysql://test:test@localhost:3306/testdb" : "postgresql://test:test@localhost:5432/testdb",
      dir: "/test/migrations",
      table: "nomad_migrations",
      schema: flavor === "postgres" ? "public" : undefined,
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    } as Config;

    migrator = new Migrator(config, driver);

    migrationsByPath = new Map();
    checksumByContent = new Map();

    listMigrationFilesMock = vi.mocked(listMigrationFiles);
    readFileSyncMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
    parseNomadSqlFileMock = vi.mocked(parseNomadSqlFile);
    filenameToVersionMock = vi.mocked(filenameToVersion);

    filenameToVersionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    vi.mocked(calculateChecksum).mockImplementation((content: string) => {
      return checksumByContent.get(content) ?? `chk:${content}`;
    });

    readFileSyncMock.mockImplementation((filepath: string) => {
      const entry = migrationsByPath.get(filepath);
      if (!entry) throw new Error(`Unexpected file read for ${filepath}`);
      return entry.content;
    });

    parseNomadSqlFileMock.mockImplementation((filepath: string) => {
      const entry = migrationsByPath.get(filepath);
      if (!entry) throw new Error(`Unexpected parse for ${filepath}`);
      return {
        up: { statements: entry.up, notx: entry.upNotx ?? false },
        down: { statements: entry.down, notx: entry.downNotx ?? false },
        tags: []
      } as any;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function createMigration(version: string, name: string, opts: {
    up: string[];
    down: string[];
    upNotx?: boolean;
    downNotx?: boolean;
    checksum?: string;
  }): MigrationDef {
    const versionBigInt = BigInt(version);
    const checksum = opts.checksum ?? `chk-${version}`;
    const content = ['-- up', ...opts.up, '-- down', ...opts.down].join('\n');
    checksumByContent.set(content, checksum);
    return {
      version: versionBigInt,
      name,
      up: opts.up,
      down: opts.down,
      upNotx: opts.upNotx,
      downNotx: opts.downNotx,
      checksum,
      content
    };
  }

  function installMigrations(defs: MigrationDef[]) {
    migrationsByPath.clear();
    const paths = defs.map(def => {
      const filepath = `${config.dir}/${def.version}_${def.name}.sql`;
      migrationsByPath.set(filepath, def);
      return filepath;
    });
    listMigrationFilesMock.mockReturnValue(paths);
  }

  function createAppliedRow(def: MigrationDef, overrides: Partial<AppliedRow> = {}): AppliedRow {
    return {
      version: def.version,
      name: def.name,
      checksum: def.checksum,
      appliedAt: new Date("2024-01-01T00:00:00Z"),
      rolledBackAt: null,
      ...overrides
    };
  }

  function enqueueConnections(initialRows: AppliedRow[], options: {
    finalRows?: AppliedRow[];
    lockAcquired?: boolean;
  } = {}) {
    const finalRows = options.finalRows ?? initialRows;
    const lockAcquired = options.lockAcquired ?? true;

    const executionConn = driver.enqueueConnection({
      ensureMigrationsTable: vi.fn().mockResolvedValue(undefined),
      acquireLock: vi.fn().mockResolvedValue(lockAcquired),
      releaseLock: vi.fn().mockResolvedValue(undefined),
      beginTransaction: vi.fn().mockResolvedValue(undefined),
      commitTransaction: vi.fn().mockResolvedValue(undefined),
      rollbackTransaction: vi.fn().mockResolvedValue(undefined),
      markMigrationApplied: vi.fn().mockResolvedValue(undefined),
      markMigrationRolledBack: vi.fn().mockResolvedValue(undefined)
    });

    const statements: string[] = [];
    executionConn.runStatement.mockImplementation(async (sql: string) => {
      statements.push(sql);
    });

    driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(initialRows)
    });

    driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(finalRows)
    });

    return { executionConn, statements };
  }

  it("applies forward to reach target version", async () => {
    const mig1 = createMigration("20250923052647", "initialize_db", {
      up: ["CREATE TABLE t(id int);"] ,
      down: ["DROP TABLE t;"]
    });
    const mig2 = createMigration("20250923052844", "init_user_values", {
      up: ["INSERT INTO t VALUES (1);"] ,
      down: ["DELETE FROM t WHERE id=1;"]
    });
    installMigrations([mig1, mig2]);

    const finalRows = [createAppliedRow(mig1), createAppliedRow(mig2)];
    const { executionConn, statements } = enqueueConnections([], { finalRows });

    await migrator.to(mig2.version);

    if (flavor === "postgres") {
      expect(executionConn.beginTransaction).toHaveBeenCalledTimes(2);
      expect(executionConn.commitTransaction).toHaveBeenCalledTimes(2);
    } else {
        expect(executionConn.commitTransaction).not.toHaveBeenCalled();
    }
    expect(statements).toEqual([
      "CREATE TABLE t(id int);",
      "INSERT INTO t VALUES (1);"
    ]);
    expect(executionConn.markMigrationApplied).toHaveBeenNthCalledWith(1, {
      version: mig1.version,
      name: mig1.name,
      checksum: mig1.checksum
    });
    expect(executionConn.markMigrationApplied).toHaveBeenNthCalledWith(2, {
      version: mig2.version,
      name: mig2.name,
      checksum: mig2.checksum
    });
  });

  it("rolls back down to target version", async () => {
    const mig1 = createMigration("20250923052647", "initialize_db", {
      up: ["CREATE TABLE t(id int);"] ,
      down: ["DROP TABLE t;"]
    });
    const mig2 = createMigration("20250923052844", "init_user_values", {
      up: ["INSERT INTO t VALUES (1);"] ,
      down: ["DELETE FROM t WHERE id=1;"]
    });
    installMigrations([mig1, mig2]);

    const initialRows = [createAppliedRow(mig1), createAppliedRow(mig2)];
    const finalRows = [createAppliedRow(mig1), createAppliedRow(mig2, { rolledBackAt: new Date() })];
    const { executionConn, statements } = enqueueConnections(initialRows, { finalRows });

    await migrator.to(mig1.version);

    expect(statements).toEqual(["DELETE FROM t WHERE id=1;"]);
    expect(executionConn.markMigrationRolledBack).toHaveBeenCalledWith(mig2.version);
    if (flavor === "postgres") {
      expect(executionConn.beginTransaction).toHaveBeenCalledTimes(1);
    } else {
      expect(executionConn.beginTransaction).not.toHaveBeenCalled();
    }
  });

  it("is a no-op when already at target version", async () => {
    const mig1 = createMigration("20250923052647", "initialize_db", {
      up: ["CREATE TABLE t(id int);"] ,
      down: ["DROP TABLE t;"]
    });
    installMigrations([mig1]);

    const rows = [createAppliedRow(mig1)];
    const { executionConn, statements } = enqueueConnections(rows, { finalRows: rows });

    await migrator.to(mig1.version);

    expect(statements).toEqual([]);
    expect(executionConn.markMigrationApplied).not.toHaveBeenCalled();
    expect(executionConn.markMigrationRolledBack).not.toHaveBeenCalled();
  });

  it("throws MissingFileError when file for rollback is missing", async () => {
    const mig1 = createMigration("20250923052647", "initialize_db", {
      up: ["CREATE TABLE t(id int);"] ,
      down: ["DROP TABLE t;"]
    });
    installMigrations([mig1]);

    const missingApplied: AppliedRow = {
      version: 20250923052844n,
      name: "init_user_values",
      checksum: "chk-missing",
      appliedAt: new Date("2024-01-01T00:00:00Z"),
      rolledBackAt: null
    };

    enqueueConnections([createAppliedRow(mig1), missingApplied]);

    await expect(migrator.to(mig1.version)).rejects.toBeInstanceOf(MissingFileError);
  });

  it("throws ChecksumMismatchError on rollback when drift detected without allowDrift", async () => {
    const mig1 = createMigration("20250923052647", "initialize_db", {
      up: ["CREATE TABLE t(id int);"] ,
      down: ["DROP TABLE t;"]
    });
    const mig2 = createMigration("20250923052844", "init_user_values", {
      up: ["INSERT INTO t VALUES (1);"] ,
      down: ["DELETE FROM t WHERE id=1;"]
    });
    installMigrations([mig1, mig2]);

    const appliedRows = [
      createAppliedRow(mig1),
      createAppliedRow(mig2, { checksum: "WRONG" })
    ];

    enqueueConnections(appliedRows);

    await expect(migrator.to(mig1.version)).rejects.toBeInstanceOf(ChecksumMismatchError);
  });

  it("allows drift when allowDrift=true", async () => {
    const mig1 = createMigration("20250923052647", "initialize_db", {
      up: ["CREATE TABLE t(id int);"] ,
      down: ["DROP TABLE t;"]
    });
    const mig2 = createMigration("20250923052844", "init_user_values", {
      up: ["INSERT INTO t VALUES (1);"] ,
      down: ["DELETE FROM t WHERE id=1;"]
    });
    installMigrations([mig1, mig2]);

    const appliedRows = [
      createAppliedRow(mig1),
      createAppliedRow(mig2, { checksum: "WRONG" })
    ];

    driver = createDriverMock();
    migrator = new Migrator({ ...config, allowDrift: true }, driver);

    const finalRows = [createAppliedRow(mig1), createAppliedRow(mig2, { rolledBackAt: new Date() })];
    const { executionConn } = enqueueConnections(appliedRows, { finalRows });

    await expect(migrator.to(mig1.version)).resolves.toBeUndefined();
    expect(executionConn.markMigrationRolledBack).toHaveBeenCalledWith(mig2.version);
  });
});
