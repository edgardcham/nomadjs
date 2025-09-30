import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createSqliteDriver } from "../../src/driver/sqlite.js";
import { ConnectionError, SqlError } from "../../src/core/errors.js";

async function withConnection<T>(driver: ReturnType<typeof createSqliteDriver>, fn: (conn: Awaited<ReturnType<typeof driver.connect>>) => Promise<T>) {
  const conn = await driver.connect();
  try {
    return await fn(conn);
  } finally {
    await conn.dispose();
  }
}

describe("SQLite driver", () => {
  let dir: string;
  let dbPath: string;
  let url: string;
  let driver: ReturnType<typeof createSqliteDriver>;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "nomad-sqlite-driver-"));
    dbPath = join(dir, "nomad.sqlite");
    url = `sqlite://${dbPath}`;
    driver = createSqliteDriver({ url, table: "nomad_migrations" });
  });

  afterEach(async () => {
    await driver.close();
    rmSync(dir, { recursive: true, force: true });
  });

  it("quotes identifiers with double quotes", () => {
    expect(driver.quoteIdent("users")).toBe('"users"');
    expect(driver.quoteIdent('strange"name')).toBe('"strange""name"');
  });

  it("ensures migrations table and fetches applied migrations", async () => {
    await withConnection(driver, async conn => {
      await conn.ensureMigrationsTable();
      await conn.markMigrationApplied({
        version: 20240101120000n,
        name: "create_users",
        checksum: "abc123"
      });

      const rows = await conn.fetchAppliedMigrations();
      expect(rows).toHaveLength(1);
      const [row] = rows;
      expect(row.version).toBe(20240101120000n);
      expect(row.name).toBe("create_users");
      expect(row.checksum).toBe("abc123");
      expect(row.appliedAt).toBeInstanceOf(Date);
      expect(row.rolledBackAt).toBeNull();
    });
  });

  it("upserts applied rows and records rollbacks", async () => {
    await withConnection(driver, async conn => {
      await conn.ensureMigrationsTable();

      await conn.markMigrationApplied({
        version: 20240101120000n,
        name: "create_users",
        checksum: "abc123"
      });

      await conn.markMigrationApplied({
        version: 20240101120000n,
        name: "create_users",
        checksum: "def456"
      });

      await conn.markMigrationRolledBack(20240101120000n);

      const rows = await conn.fetchAppliedMigrations();
      expect(rows).toHaveLength(1);
      const [row] = rows;
      expect(row.checksum).toBe("def456");
      expect(row.rolledBackAt).toBeInstanceOf(Date);
    });
  });

  it("acquires and releases locks per connection", async () => {
    const connA = await driver.connect();
    const acquiredA = await connA.acquireLock("lock-key", 5000);
    expect(acquiredA).toBe(true);

    const connB = await driver.connect();
    const acquiredB = await connB.acquireLock("lock-key", 5000);
    expect(acquiredB).toBe(false);

    await connA.releaseLock("lock-key");
    const acquiredBAgain = await connB.acquireLock("lock-key", 5000);
    expect(acquiredBAgain).toBe(true);
    await connB.releaseLock("lock-key");

    await connA.dispose();
    await connB.dispose();
  });

  it("executes arbitrary SQL statements", async () => {
    await withConnection(driver, async conn => {
      await conn.ensureMigrationsTable();
      await conn.runStatement("CREATE TABLE demo(id INTEGER PRIMARY KEY, name TEXT)");
      await conn.runStatement("INSERT INTO demo(name) VALUES ('hello')");
      const result = await conn.query<{ name: string }>("SELECT name FROM demo");
      expect(result.rows).toEqual([{ name: "hello" }]);
    });
  });

  it("maps connection-related errors to ConnectionError", () => {
    const error = Object.assign(new Error("unable to open database file"), { code: "SQLITE_CANTOPEN" });
    const mapped = driver.mapError(error);
    expect(mapped).toBeInstanceOf(ConnectionError);
  });


  it("applies custom busy timeout when provided", async () => {
    const driverWithTimeout = createSqliteDriver({
      url,
      table: "nomad_timeout",
      connectTimeoutMs: 4321
    });

    try {
      await withConnection(driverWithTimeout, async conn => {
        const { rows } = await conn.query<{ busy_timeout?: number; timeout?: number }>("PRAGMA busy_timeout");
        const timeout = rows[0]?.busy_timeout ?? rows[0]?.timeout;
        expect(timeout).toBe(4321);
      });
    } finally {
      await driverWithTimeout.close();
    }
  });

  it("maps generic errors to SqlError", () => {
    const error = Object.assign(new Error("constraint failed"), { code: "SQLITE_CONSTRAINT" });
    const mapped = driver.mapError(error);
    expect(mapped).toBeInstanceOf(SqlError);
  });
});
