
/**
 * CLI integration smoke tests for MySQL driver.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import mysql from "mysql2/promise";
import { isDatabaseAvailable } from "../utils/db";

const nomadCmd = "node dist/esm/cli.js";
const mysqlUrl = process.env.MYSQL_URL || "mysql://root:nomad@localhost:3307/nomad_test";
const shouldRunDbTests =
  process.env.NOMAD_TEST_WITH_DB_MYSQL === "true" &&
  isDatabaseAvailable(mysqlUrl, nomadCmd, "mysql");

if (!shouldRunDbTests) {
  console.warn("Skipping CLI MySQL integration tests: database unavailable or NOMAD_TEST_WITH_DB_MYSQL not set");
}

const describeIfDb = shouldRunDbTests ? describe : describe.skip;
const tablesUsed = new Set<string>();

function makeTableName(): string {
  const unique = `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`;
  const name = `nomad_mysql_${unique}`;
  tablesUsed.add(name);
  return name;
}

describeIfDb("CLI: MySQL integration", () => {
  const testDir = join(process.cwd(), "test-migrations-mysql");
  let connection: mysql.Connection;

  beforeAll(async () => {
    connection = await mysql.createConnection(mysqlUrl);
    await connection.query('DROP TABLE IF EXISTS mysql_smoke_users');
  });

  afterAll(async () => {
    try {
      for (const table of tablesUsed) {
        const lockKey = computeLockKey(table);
        await connection.query('SELECT RELEASE_LOCK(?)', [lockKey]);
        await connection.query(`DROP TABLE IF EXISTS ${table}`);
      }
      await connection.query('DROP TABLE IF EXISTS mysql_smoke_users');
    } finally {
      await connection.end();
    }
  });

  beforeEach(() => {
    mkdirSync(testDir, { recursive: true });
  });

  afterEach(async () => {
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
    for (const table of tablesUsed) {
      const lockKey = computeLockKey(table);
      await connection.query('SELECT RELEASE_LOCK(?)', [lockKey]);
      await connection.query(`DROP TABLE IF EXISTS ${table}`);
    }
    tablesUsed.clear();
    await connection.query('DROP TABLE IF EXISTS mysql_smoke_users');
  });

  function computeLockKey(table: string): string {
    const data = `${mysqlUrl || ""}|${testDir}||${table}`;
    return createHash("sha256").update(data).digest("hex");
  }

  function runCLI(args: string, table: string, expectError = false): string {
  const command = `${nomadCmd} --driver mysql --url "${mysqlUrl}" --dir "${testDir}" --table ${table} ${args}`;
  try {
    return execSync(command, {
      encoding: "utf8",
      env: { ...process.env, DATABASE_URL: mysqlUrl, NOMAD_DRIVER: "mysql" }
    }).toString();
  } catch (error: any) {
    if (expectError) {
      return (error.stdout || error.stderr || error.message)?.toString();
    }
    throw new Error(`CLI failed: ${error.message}
stdout: ${error.stdout}
stderr: ${error.stderr}`);
  }
}

  it("applies and rolls back a migration", async () => {
    const tableName = makeTableName();
    const migrationFile = join(testDir, "20240101120000_create_users.sql");
    writeFileSync(
      migrationFile,
      `-- +nomad up
CREATE TABLE mysql_smoke_users (id INT PRIMARY KEY);
-- +nomad down
DROP TABLE mysql_smoke_users;
`
    );

    const upOutput = runCLI("up", tableName);
    expect(upOutput).toContain("↑ up 20240101120000 (create_users)");

    const [rowsAfterUp] = await connection.query("SHOW TABLES LIKE 'mysql_smoke_users'");
    expect(Array.isArray(rowsAfterUp) && rowsAfterUp.length).toBeTruthy();

    const downOutput = runCLI("down", tableName);
    expect(downOutput).toContain("↓ down 20240101120000 (create_users)");

    const [rowsAfterDown] = await connection.query("SHOW TABLES LIKE 'mysql_smoke_users'");
    expect(Array.isArray(rowsAfterDown) && rowsAfterDown.length).toBe(0);
  });

  it("reports status in JSON", () => {
    const tableName = makeTableName();
    const statusOutput = runCLI("status --json", tableName);
    expect(() => JSON.parse(statusOutput)).not.toThrow();
  });
});
