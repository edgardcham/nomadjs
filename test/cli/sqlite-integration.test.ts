import { describe, it, expect, beforeEach, afterEach, beforeAll } from "vitest";
import { execSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";

const nomadCmd = "node dist/esm/cli.js";

let SqliteDatabaseCtor: any;

beforeAll(async () => {
  try {
    const better = await import("better-sqlite3");
    if (better && typeof better.default === "function") {
      SqliteDatabaseCtor = better.default;
      return;
    }
  } catch {
    // Continue to fallback loader
  }

  try {
    const nodeSqlite = await import("node:sqlite");
    if (nodeSqlite && typeof nodeSqlite.DatabaseSync === "function") {
      SqliteDatabaseCtor = nodeSqlite.DatabaseSync;
      return;
    }
  } catch {
    // Ignore and raise unified error below
  }

  throw new Error("SQLite integration tests require better-sqlite3 (fallback to node:sqlite failed)");
});

function createDatabaseInstance(path: string) {
  if (!SqliteDatabaseCtor) {
    throw new Error("SQLite integration tests could not load a SQLite backend");
  }
  return new SqliteDatabaseCtor(path);
}

describe("CLI: SQLite integration", () => {
  let workDir: string;
  let dbPath: string;
  let migrationsDir: string;

  beforeEach(() => {
    workDir = mkdtempSync(join(tmpdir(), "nomad-sqlite-it-"));
    dbPath = join(workDir, "nomad.sqlite");
    migrationsDir = join(workDir, "migrations");
    mkdirSync(migrationsDir, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(workDir)) {
      rmSync(workDir, { recursive: true, force: true });
    }
  });

  function sqliteUrl(): string {
    return `sqlite://${dbPath}`;
  }

  function makeTableName(): string {
    return `nomad_sqlite_${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
  }

  function computeLockKey(table: string, urlOverride?: string): string {
    const url = urlOverride ?? sqliteUrl();
    const data = `${url}|${migrationsDir}||${table}`;
    return createHash("sha256").update(data).digest("hex");
  }

  function runCLI(args: string, expectError = false, urlOverride?: string): string {
    const targetUrl = urlOverride ?? sqliteUrl();
    const command = `${nomadCmd} --driver sqlite --url "${targetUrl}" --dir "${migrationsDir}" ${args}`;
    try {
      return execSync(command, {
        encoding: "utf8",
        env: { ...process.env, DATABASE_URL: targetUrl, NOMAD_DRIVER: "sqlite" }
      }).toString();
    } catch (error: any) {
      if (expectError) {
        return (error.stdout || error.stderr || error.message)?.toString();
      }
      throw new Error(`CLI failed: ${error.message}\nstdout: ${error.stdout}\nstderr: ${error.stderr}`);
    }
  }

  function openDb() {
    return createDatabaseInstance(dbPath);
  }

  it("applies and rolls back a migration", () => {
    const migrationFile = join(migrationsDir, "20240101120000_create_table.sql");
    writeFileSync(
      migrationFile,
      `-- +nomad up\nCREATE TABLE nomad_demo_users (id INTEGER PRIMARY KEY, name TEXT);\n-- +nomad down\nDROP TABLE nomad_demo_users;\n`
    );

    const upOutput = runCLI("up");
    expect(upOutput).toContain("↑ up 20240101120000 (create_table)");

    let db = openDb();
    const tablesAfterUp = db.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'nomad_demo_users'"
    ).all();
    expect(tablesAfterUp.length).toBe(1);
    db.close();

    const downOutput = runCLI("down");
    expect(downOutput).toContain("↓ down 20240101120000 (create_table)");

    db = openDb();
    const tablesAfterDown = db.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'nomad_demo_users'"
    ).all();
    expect(tablesAfterDown.length).toBe(0);
    db.close();
  });

  it("reports status as JSON", () => {
    const output = runCLI("status --json");
    expect(() => JSON.parse(output)).not.toThrow();
  });

  it("supports in-memory database", () => {
    const migrationFile = join(migrationsDir, "20240101125000_memory.sql");
    writeFileSync(
      migrationFile,
      `-- +nomad up\nCREATE TABLE nomad_memory (id INTEGER PRIMARY KEY);\n-- +nomad down\nDROP TABLE nomad_memory;\n`
    );

    const memoryUrl = "sqlite::memory:";
    const upOutput = runCLI("up", false, memoryUrl);
    expect(upOutput).toContain("↑ up 20240101125000 (memory)");

    const statusOutput = runCLI("status --json", false, memoryUrl);
    expect(() => JSON.parse(statusOutput)).not.toThrow();
  });

  it("fails when lock is already held", () => {
    const tableName = makeTableName();
    const migrationFile = join(migrationsDir, "20240101126000_lock.sql");
    writeFileSync(
      migrationFile,
      `-- +nomad up\nCREATE TABLE nomad_lock_test (id INTEGER PRIMARY KEY);\n-- +nomad down\nDROP TABLE nomad_lock_test;\n`
    );

    const lockKey = computeLockKey(tableName);
    const db = createDatabaseInstance(dbPath);
    db.exec("CREATE TABLE IF NOT EXISTS nomad_lock (lock_name TEXT PRIMARY KEY, acquired_at TEXT NOT NULL)");
    db.prepare("INSERT OR IGNORE INTO nomad_lock(lock_name, acquired_at) VALUES (?, datetime('now'))").run(lockKey);
    db.close();

    const output = runCLI(`--table ${tableName} up --lock-timeout 200`, true);
    expect(output).toContain("Failed to acquire migration lock");
  });
});
