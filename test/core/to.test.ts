import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import type { Config } from "../../src/config.js";
import { ChecksumMismatchError, MissingFileError } from "../../src/core/errors.js";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import { createPgQueryMock, type PgResponder } from "../helpers/mockPg.js";

// Mocks
vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Migrator.to()", () => {
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
    autoNotx: false,
    lockTimeout: 30000
  } as any;

  let responders: PgResponder[];
  let mockAppliedRows: any[] = [];
  let mockUpStatements: string[] = [];
  let mockDownStatements: string[] = [];

  beforeEach(() => {
    vi.clearAllMocks();

    // Silence logs
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});

    mockAppliedRows = [];
    mockUpStatements = [];
    mockDownStatements = [];

    responders = [
      {
        match: (sql: string) => /SELECT version, name, checksum/.test(sql),
        handler: () => ({ rows: mockAppliedRows })
      },
      {
        match: (sql: string) => /CREATE TABLE IF NOT EXISTS/.test(sql),
        handler: () => ({ rows: [] })
      },
      {
        match: (sql: string) => mockUpStatements.includes(sql.trim()),
        handler: () => ({ rows: [] })
      },
      {
        match: (sql: string) => mockDownStatements.includes(sql.trim()),
        handler: () => ({ rows: [] })
      }
    ];

    queryMock = createPgQueryMock(responders);

    // pg Pool mock
    mockPool = {
      query: queryMock,
      end: vi.fn(),
      connect: vi.fn().mockResolvedValue({ query: queryMock, release: vi.fn() })
    };
    (Pool as any).mockImplementation(() => mockPool);

    // File helpers
    listMigrationFilesMock = listMigrationFiles as any;
    readFileSyncMock = readFileSync as any;
    parseNomadSqlFileMock = parseNomadSqlFile as any;
    filenameToVersionMock = filenameToVersion as any;

    // filenameToVersion -> extract numeric timestamp from path
    filenameToVersionMock.mockImplementation((filepath: string) => {
      const m = filepath.match(/(\d{14})/);
      return m ? m[1] : undefined;
    });

    migrator = new Migrator(config, mockPool as unknown as Pool);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("applies forward to reach target version", async () => {
    // Files
    const f1 = "/test/migrations/20250923052647_initialize_db.sql";
    const f2 = "/test/migrations/20250923052844_init_user_values.sql";
    listMigrationFilesMock.mockReturnValue([f1, f2]);

    const c1 = "CREATE TABLE t(id int);";
    const c2 = "INSERT INTO t VALUES (1);";
    mockUpStatements = [c1.trim(), c2.trim()];
    mockUpStatements = [c1.trim(), c2.trim()];
    mockUpStatements = [c1.trim(), c2.trim()];
    readFileSyncMock
      .mockReturnValueOnce(c1)
      .mockReturnValueOnce(c2);

    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [c1], notx: false }, down: { statements: ["DROP TABLE t;"], notx: false }, noTransaction: false })
      .mockReturnValueOnce({ up: { statements: [c2], notx: false }, down: { statements: ["DELETE FROM t WHERE id=1;"], notx: false }, noTransaction: false });

    mockAppliedRows = [];

    await migrator.to(20250923052844n);

    // Should have executed the two up statements
    const calls = queryMock.mock.calls.map(c => c[0] as string);
    expect(calls.some(sql => typeof sql === 'string' && sql.includes("CREATE TABLE t"))).toBe(true);
    expect(calls.some(sql => typeof sql === 'string' && sql.includes("INSERT INTO t"))).toBe(true);
    // Should record versions
    expect(calls.some(sql => sql.includes("INSERT INTO nomad_migrations"))).toBe(true);
  });

  it("rolls back down to target version", async () => {
    const f1 = "/test/migrations/20250923052647_initialize_db.sql";
    const f2 = "/test/migrations/20250923052844_init_user_values.sql";
    listMigrationFilesMock.mockReturnValue([f1, f2]);

    const c1 = "CREATE TABLE t(id int);";
    const c2 = "INSERT INTO t VALUES (1);";
    readFileSyncMock
      .mockReturnValueOnce(c1)
      .mockReturnValueOnce(c2);

    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [c1], notx: false }, down: { statements: ["DROP TABLE t;"], notx: false }, noTransaction: false })
      .mockReturnValueOnce({ up: { statements: [c2], notx: false }, down: { statements: ["DELETE FROM t WHERE id=1;"], notx: false }, noTransaction: false });

    mockUpStatements = [c1.trim(), c2.trim()];
    mockDownStatements = ["DROP TABLE t;".trim(), "DELETE FROM t WHERE id=1;".trim()];

    mockAppliedRows = [
      { version: "20250923052647", name: "initialize_db", checksum: calculateChecksum(c1), applied_at: new Date(), rolled_back_at: null },
      { version: "20250923052844", name: "init_user_values", checksum: calculateChecksum(c2), applied_at: new Date(), rolled_back_at: null }
    ];

    await migrator.to(20250923052647n);

    const calls = queryMock.mock.calls.map(c => c[0] as string);
    // Should have executed a DELETE (down) and updated rolled_back_at
    expect(calls.some(sql => typeof sql === 'string' && sql.includes("DELETE FROM t"))).toBe(true);
    expect(calls.some(sql => typeof sql === 'string' && sql.includes("SET rolled_back_at"))).toBe(true);
  });

  it("is a no-op when already at target version", async () => {
    const f1 = "/test/migrations/20250923052647_initialize_db.sql";
    listMigrationFilesMock.mockReturnValue([f1]);
    const c1 = "CREATE TABLE t(id int);";
    readFileSyncMock.mockReturnValueOnce(c1);
    parseNomadSqlFileMock.mockReturnValueOnce({ up: { statements: [c1], notx: false }, down: { statements: ["DROP TABLE t;"], notx: false }, noTransaction: false });

    mockUpStatements = [c1.trim()];
    mockDownStatements = ["DROP TABLE t;".trim()];

    mockAppliedRows = [
      { version: "20250923052647", name: "initialize_db", checksum: calculateChecksum(c1), applied_at: new Date(), rolled_back_at: null }
    ];

    await migrator.to(20250923052647n);
    // No further DML expected beyond lock/ensure/select
    // Specifically, no INSERT into nomad_migrations and no SET rolled_back_at
    const calls = queryMock.mock.calls.map(c => c[0] as string);
    expect(calls.some(sql => typeof sql === 'string' && sql.includes("INSERT INTO nomad_migrations"))).toBe(false);
    expect(calls.some(sql => typeof sql === 'string' && sql.includes("SET rolled_back_at"))).toBe(false);
  });

  it("throws MissingFileError when file for rollback is missing", async () => {
    // Only lower version file exists; higher version applied
    const f1 = "/test/migrations/20250923052647_initialize_db.sql";
    listMigrationFilesMock.mockReturnValue([f1]);
    const c1 = "CREATE TABLE t(id int);";
    readFileSyncMock.mockReturnValueOnce(c1);
    parseNomadSqlFileMock.mockReturnValueOnce({ up: { statements: [c1], notx: false }, down: { statements: ["DROP TABLE t;"], notx: false }, noTransaction: false });

    mockAppliedRows = [
      { version: "20250923052647", name: "initialize_db", checksum: calculateChecksum(c1), applied_at: new Date(), rolled_back_at: null },
      { version: "20250923052844", name: "init_user_values", checksum: "abc", applied_at: new Date(), rolled_back_at: null }
    ];

    mockUpStatements = [c1.trim()];
    mockDownStatements = ["DROP TABLE t;".trim()];

    await expect(migrator.to(20250923052647n)).rejects.toBeInstanceOf(MissingFileError);
  });

  it("throws ChecksumMismatchError on rollback when drift detected without allowDrift", async () => {
    const f1 = "/test/migrations/20250923052647_initialize_db.sql";
    const f2 = "/test/migrations/20250923052844_init_user_values.sql";
    listMigrationFilesMock.mockReturnValue([f1, f2]);

    const c1 = "CREATE TABLE t(id int);";
    const c2 = "INSERT INTO t VALUES (1);";
    readFileSyncMock
      .mockReturnValueOnce(c1)
      .mockReturnValueOnce(c2);

    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [c1], notx: false }, down: { statements: ["DROP TABLE t;"], notx: false }, noTransaction: false })
      .mockReturnValueOnce({ up: { statements: [c2], notx: false }, down: { statements: ["DELETE FROM t WHERE id=1;"], notx: false }, noTransaction: false });

    // Applied checksum doesn't match file checksum for f2
    mockAppliedRows = [
      { version: "20250923052647", name: "initialize_db", checksum: calculateChecksum(c1), applied_at: new Date(), rolled_back_at: null },
      { version: "20250923052844", name: "init_user_values", checksum: "WRONG", applied_at: new Date(), rolled_back_at: null }
    ];

    mockUpStatements = [c1.trim(), c2.trim()];
    mockDownStatements = ["DROP TABLE t;".trim(), "DELETE FROM t WHERE id=1;".trim()];

    await expect(migrator.to(20250923052647n)).rejects.toBeInstanceOf(ChecksumMismatchError);
  });

  it("allows drift when allowDrift=true", async () => {
    const driftConfig: Config = { ...config, allowDrift: true } as any;
    migrator = new Migrator(driftConfig, mockPool as unknown as Pool);

    const f1 = "/test/migrations/20250923052647_initialize_db.sql";
    const f2 = "/test/migrations/20250923052844_init_user_values.sql";
    listMigrationFilesMock.mockReturnValue([f1, f2]);

    const c1 = "CREATE TABLE t(id int);";
    const c2 = "INSERT INTO t VALUES (1);";
    readFileSyncMock
      .mockReturnValueOnce(c1)
      .mockReturnValueOnce(c2);

    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [c1], notx: false }, down: { statements: ["DROP TABLE t;"], notx: false }, noTransaction: false })
      .mockReturnValueOnce({ up: { statements: [c2], notx: false }, down: { statements: ["DELETE FROM t WHERE id=1;"], notx: false }, noTransaction: false });

    // Applied checksum different but allowDrift is true
    mockAppliedRows = [
      { version: "20250923052647", name: "initialize_db", checksum: calculateChecksum(c1), applied_at: new Date(), rolled_back_at: null },
      { version: "20250923052844", name: "init_user_values", checksum: "WRONG", applied_at: new Date(), rolled_back_at: null }
    ];

    mockUpStatements = [c1.trim(), c2.trim()];
    mockDownStatements = ["DROP TABLE t;".trim(), "DELETE FROM t WHERE id=1;".trim()];

    await expect(migrator.to(20250923052647n)).resolves.toBeUndefined();
  });
});
