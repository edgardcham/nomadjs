import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { createPostgresDriver } from "../../src/driver/postgres.js";
import { ConnectionError, SqlError, ParseConfigError } from "../../src/core/errors.js";

const mockClient = {
  query: vi.fn(),
  release: vi.fn()
};

let connectSpy: any;
let poolEnded = false;
let poolConfig: any;

vi.mock("pg", () => {
  class MockPool {
    constructor(options: any) {
      poolConfig = options;
      const connString = options?.connectionString;
      if (typeof connString === 'string' && connString.includes(':99999')) {
        const err: any = new RangeError('port should be >= 0 and < 65536');
        err.code = 'ERR_SOCKET_BAD_PORT';
        throw err;
      }
    }

    connect(): Promise<any> {
      return connectSpy();
    }

    query(sql: string, params?: any[]) {
      return mockClient.query(sql, params);
    }

    async end(): Promise<void> {
      poolEnded = true;
    }
  }

  return { Pool: MockPool };
});

function deriveExpectedPgLockKey(lockKeyHex: string): number {
  const buf = Buffer.from(lockKeyHex, "hex");
  const first = buf.subarray(0, 4);
  const num = first.readUInt32BE(0);
  return (num % 2147483647) + 1;
}

describe("Postgres driver", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockClient.query.mockReset();
    mockClient.release.mockReset();
    connectSpy = vi.fn().mockResolvedValue(mockClient);
    poolConfig = undefined;
    poolEnded = false;
  });

  afterEach(() => {
    vi.resetModules();
  });

  it("quotes identifiers safely", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    expect(driver.quoteIdent("migrations")).toBe('"migrations"');
    expect(driver.quoteIdent('weird"name')).toBe('"weird""name"');
  });

  it("uses provided connect timeout when creating pool", async () => {
    const driver: any = createPostgresDriver({
      url: "postgres://db",
      table: "nomad_migrations",
      schema: "public",
      connectTimeoutMs: 1234
    });

    await driver.connect();
    expect(poolConfig.connectionTimeoutMillis).toBe(1234);
  });

  it("probes connection via SELECT 1", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValue({ rows: [{ "?column?": 1 }] });

    await driver.probeConnection();

    expect(mockClient.query).toHaveBeenCalledWith("SELECT 1", undefined);
  });

  it("maps errors thrown during probe", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const error = Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
    mockClient.query.mockRejectedValue(error);

    await expect(driver.probeConnection()).rejects.toBeInstanceOf(ConnectionError);
  });

  it("ensures migrations table with schema-qualified name", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "analytics" });
    mockClient.query.mockResolvedValue({ rows: [] });

    const conn = await driver.connect();
    await conn.ensureMigrationsTable();

    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining('CREATE TABLE IF NOT EXISTS "analytics"."nomad_migrations"'), []);
  });

  it("fetches applied migrations ordered by version", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValueOnce({
      rows: [
        { version: "20240101120000", name: "bar", checksum: "def", applied_at: new Date("2024-01-02"), rolled_back_at: null },
        { version: "20230101120000", name: "foo", checksum: "abc", applied_at: new Date("2023-01-02"), rolled_back_at: null }
      ]
    });

    const conn = await driver.connect();
    const rows = await conn.fetchAppliedMigrations();

    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("SELECT version, name, checksum"), []);
    expect(rows[0]?.version).toBe(20230101120000n);
    expect(rows[1]?.version).toBe(20240101120000n);
  });

  it("upserts applied migration metadata", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValue({ rows: [] });

    const conn = await driver.connect();
    await conn.markMigrationApplied({ version: 20240101120000n, name: "create_users", checksum: "abc123" });

    const call = mockClient.query.mock.calls.at(-1);
    expect(call?.[0]).toContain("ON CONFLICT (version) DO UPDATE");
    expect(call?.[1]).toEqual(["20240101120000", "create_users", "abc123"]);
  });

  it("marks migration as rolled back", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValue({ rows: [] });

    const conn = await driver.connect();
    await conn.markMigrationRolledBack(20240101120000n);

    expect(mockClient.query).toHaveBeenCalledWith(expect.stringContaining("rolled_back_at = NOW()"), ["20240101120000"]);
  });

  it("acquires and releases advisory locks using derived key", async () => {
    const lockKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const expected = deriveExpectedPgLockKey(lockKey);

    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] });
    mockClient.query.mockResolvedValueOnce({ rows: [{ pg_advisory_unlock: true }] });

    const conn = await driver.connect();
    const acquired = await conn.acquireLock(lockKey, 5000);
    expect(acquired).toBe(true);
    expect(mockClient.query.mock.calls[0]).toEqual(["SELECT pg_try_advisory_lock($1)", [expected]]);

    await conn.releaseLock(lockKey);
    expect(mockClient.query.mock.calls[1]).toEqual(["SELECT pg_advisory_unlock($1)", [expected]]);
  });

  it("maps connection errors appropriately", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const error = driver.mapError({ code: "3D000", message: "database \"foo\" does not exist" });
    expect(error).toBeInstanceOf(ConnectionError);
  });

  it("maps SQL errors appropriately", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const error = driver.mapError({ code: "23505", message: "duplicate key value" });
    expect(error).toBeInstanceOf(SqlError);
  });

  it("returns false when lock acquisition fails", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValue({ rows: [{ pg_try_advisory_lock: false }] });

    const conn = await driver.connect();
    const acquired = await conn.acquireLock("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", 10);
    expect(acquired).toBe(false);
  });

  it("disposes connections and closes pool", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    mockClient.query.mockResolvedValue({ rows: [] });

    const conn = await driver.connect();
    await conn.dispose();
    expect(mockClient.release).toHaveBeenCalled();

    await driver.close();
    expect(poolEnded).toBe(true);
  });
  it("wraps connection failures raised during connect", async () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const networkError = Object.assign(new Error("getaddrinfo ENOTFOUND db.local"), { code: "ENOTFOUND" });
    connectSpy.mockRejectedValueOnce(networkError);

    await expect(driver.connect()).rejects.toBeInstanceOf(ConnectionError);
  });

  it("maps invalid port errors to connection errors", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const portError = new RangeError("port should be >= 0 and < 65536");

    const mapped = driver.mapError(portError);
    expect(mapped).toBeInstanceOf(ConnectionError);
  });

  it("maps getaddrinfo failures without explicit code to connection errors", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const mapped = driver.mapError(new Error("getaddrinfo ENOTFOUND example.com"));

    expect(mapped).toBeInstanceOf(ConnectionError);
  });

  it("maps timeout codes to connection errors", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const mapped = driver.mapError({ code: "ETIMEDOUT", message: "connect ETIMEDOUT 192.0.2.1:5432" });

    expect(mapped).toBeInstanceOf(ConnectionError);
  });

  it("maps EPERM network errors to connection errors", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const mapped = driver.mapError({ code: "EPERM", message: "connect EPERM 192.0.2.1:5432" });

    expect(mapped).toBeInstanceOf(ConnectionError);
  });

  it("maps pg searchParams parser errors to connection errors", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const mapped = driver.mapError(new TypeError("Cannot read properties of undefined (reading 'searchParams')"));

    expect(mapped).toBeInstanceOf(ConnectionError);
  });

  it("maps pool-constructor errors to connection errors", () => {
    const createDriver = () =>
      createPostgresDriver({ url: "postgresql://localhost:99999/db", table: "nomad_migrations", schema: "public" });

    expect(createDriver).toThrow(ConnectionError);
  });

  it("maps invalid connection strings to parse config errors", () => {
    const driver: any = createPostgresDriver({ url: "postgres://", table: "nomad_migrations", schema: "public" });
    const mapped = driver.mapError(new TypeError("Invalid connection string"));

    expect(mapped).toBeInstanceOf(ParseConfigError);
  });

});
