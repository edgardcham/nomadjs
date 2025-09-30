import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { Driver } from "../../src/driver/types.js";

const createPoolMock = vi.fn();

vi.mock("mysql2/promise", () => ({
  createPool: createPoolMock
}));

let createMySqlDriver: (options: {
  url: string;
  table: string;
  schema?: string;
  connectTimeoutMs?: number;
  pool?: any;
}) => Driver;
let ConnectionErrorRef: any;
let SqlErrorRef: any;

// Lazy import to ensure mocks are in place
beforeEach(async () => {
  vi.resetModules();
  ({ createMySqlDriver } = await import("../../src/driver/mysql.js"));
  ({ ConnectionError: ConnectionErrorRef, SqlError: SqlErrorRef } = await import("../../src/core/errors.js"));
});

describe("MySQL driver", () => {
  let mockConnection: any;
  let mockPool: any;
  let executeMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    executeMock = vi.fn().mockResolvedValue([[{ value: 1 }], []]);

    mockConnection = {
      execute: executeMock,
      beginTransaction: vi.fn().mockResolvedValue(undefined),
      commit: vi.fn().mockResolvedValue(undefined),
      rollback: vi.fn().mockResolvedValue(undefined),
      release: vi.fn().mockResolvedValue(undefined)
    };

    mockPool = {
      getConnection: vi.fn().mockResolvedValue(mockConnection),
      end: vi.fn().mockResolvedValue(undefined)
    };

    createPoolMock.mockReturnValue(mockPool);
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("quotes identifiers with backticks and escapes", () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    expect(driver.quoteIdent("users")).toBe("`users`");
    expect(driver.quoteIdent("strange`name")).toBe("`strange``name`");
  });

  it("uses millisecond precision timestamps", () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    expect(driver.nowExpression()).toBe("CURRENT_TIMESTAMP(3)");
    expect(driver.supportsTransactionalDDL).toBe(false);
  });

  it("configures pool with sensible defaults", () => {
    createMySqlDriver({
      url: "mysql://root:secret@localhost:3306/app",
      table: "nomad_migrations",
      connectTimeoutMs: 1500
    });

    expect(createPoolMock).toHaveBeenCalledWith(expect.objectContaining({
      uri: "mysql://root:secret@localhost:3306/app",
      connectTimeout: 1500,
      waitForConnections: true,
      multipleStatements: false,
      charset: "UTF8MB4",
      timezone: "Z"
    }));
  });

  it("creates migrations table with expected columns", async () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const connection = await driver.connect();

    await connection.ensureMigrationsTable();

    const sql = executeMock.mock.calls[0]?.[0] ?? "";
    expect(sql).toContain("CREATE TABLE IF NOT EXISTS `nomad_migrations`");
    expect(sql).toContain("version BIGINT PRIMARY KEY");
    expect(sql).toContain("applied_at DATETIME(3)");
    expect(sql).toContain("ENGINE=InnoDB");
  });

  it("upserts applied migration metadata", async () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const connection = await driver.connect();

    await connection.markMigrationApplied({
      version: 20250101010101n,
      name: "create_users",
      checksum: "abc123"
    });

    const call = executeMock.mock.calls.at(-1);
    expect(call?.[0]).toContain("ON DUPLICATE KEY UPDATE");
    expect(call?.[1]).toEqual(["20250101010101", "create_users", "abc123"]);
  });

  it("marks migrations as rolled back", async () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const connection = await driver.connect();

    await connection.markMigrationRolledBack(20250101010101n);

    const call = executeMock.mock.calls.at(-1);
    expect(call?.[0]).toContain("UPDATE `nomad_migrations`");
    expect(call?.[0]).toContain("rolled_back_at = CURRENT_TIMESTAMP(3)");
    expect(call?.[1]).toEqual(["20250101010101"]);
  });

  it("fetches applied migrations ordered by version", async () => {
    executeMock.mockImplementation(async (sql: string) => {
      if (sql.includes("SELECT version, name")) {
        return [[
          { version: "20250201010101", name: "b", checksum: "bbb", applied_at: new Date("2025-02-01T00:00:00Z"), rolled_back_at: null },
          { version: "20240101010101", name: "a", checksum: "aaa", applied_at: new Date("2024-01-01T00:00:00Z"), rolled_back_at: null }
        ], []];
      }
      return [[{ value: 1 }], []];
    });

    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const connection = await driver.connect();
    const rows = await connection.fetchAppliedMigrations();
    expect(rows.map(r => r.version)).toEqual([20240101010101n, 20250201010101n]);
  });

  it("acquires and releases named locks", async () => {
    executeMock.mockImplementation(async (sql: string) => {
      if (sql.startsWith("SELECT GET_LOCK")) {
        return [[{ "GET_LOCK(?, ?)": 1 }], []];
      }
      if (sql.startsWith("SELECT RELEASE_LOCK")) {
        return [[{ "RELEASE_LOCK(?)": 1 }], []];
      }
      return [[{ value: 1 }], []];
    });

    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const connection = await driver.connect();

    const acquired = await connection.acquireLock("abc123", 2500);
    expect(acquired).toBe(true);

    await connection.releaseLock("abc123");
    expect(executeMock).toHaveBeenCalledWith("SELECT RELEASE_LOCK(?)", ["abc123"]);
  });

  it("treats GET_LOCK returning 0 as timeout", async () => {
    executeMock.mockImplementation(async (sql: string) => {
      if (sql.startsWith("SELECT GET_LOCK")) {
        return [[{ "GET_LOCK(?, ?)": 0 }], []];
      }
      return [[{ value: 1 }], []];
    });

    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const connection = await driver.connect();

    const acquired = await connection.acquireLock("abc123", 1000);
    expect(acquired).toBe(false);
  });

  it("maps connection-related errors", () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const error = driver.mapError({ code: "ER_ACCESS_DENIED_ERROR", message: "Access denied" });
    expect(error).toBeInstanceOf(ConnectionErrorRef);
  });

  it("maps SQL errors that are not connection related", () => {
    const driver = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    const error = driver.mapError({ code: "ER_PARSE_ERROR", message: "syntax error" });
    expect(error).toBeInstanceOf(SqlErrorRef);
  });

  it("closes owned pools but not injected ones", async () => {
    const driverOwned = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations" });
    await driverOwned.close();
    expect(mockPool.end).toHaveBeenCalled();

    const injectedPool = {
      getConnection: vi.fn().mockResolvedValue(mockConnection),
      end: vi.fn()
    };
    const driverInjected = createMySqlDriver({ url: "mysql://localhost/db", table: "nomad_migrations", pool: injectedPool });
    await driverInjected.close();
    expect(injectedPool.end).not.toHaveBeenCalled();
  });
});
