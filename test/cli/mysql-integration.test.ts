/**
 * CLI integration smoke tests for MySQL driver.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { execSync } from "node:child_process";
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

describeIfDb("CLI: MySQL integration", () => {
  const testTable = `nomad_mysql_${Date.now()}`;
  const testDir = join(process.cwd(), `test-migrations-mysql`);
  let connection: mysql.Connection;

  beforeAll(async () => {
    connection = await mysql.createConnection(mysqlUrl);
    await connection.query(`DROP TABLE IF EXISTS ${testTable}`);
    await connection.query("DROP TABLE IF EXISTS mysql_smoke_users");
  });

  afterAll(async () => {
    try {
      await connection.query(`DROP TABLE IF EXISTS ${testTable}`);
      await connection.query("DROP TABLE IF EXISTS mysql_smoke_users");
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
    await connection.query("DROP TABLE IF EXISTS mysql_smoke_users");
  });

  function runCLI(args: string, expectError = false): string {
    const command = `${nomadCmd} --driver mysql --url "${mysqlUrl}" --dir "${testDir}" --table ${testTable} ${args}`;
    try {
      return execSync(command, {
        encoding: "utf8",
        env: { ...process.env, DATABASE_URL: mysqlUrl, NOMAD_DRIVER: "mysql" }
      }).toString();
    } catch (error: any) {
      if (expectError) {
        return (error.stdout || error.stderr || error.message)?.toString();
      }
      throw new Error(`CLI failed: ${error.message}\nstdout: ${error.stdout}\nstderr: ${error.stderr}`);
    }
  }

  it("applies and rolls back a migration", async () => {
    const migrationFile = join(testDir, "20240101120000_create_users.sql");
    writeFileSync(
      migrationFile,
      `-- +nomad up\nCREATE TABLE mysql_smoke_users (id INT PRIMARY KEY);\n-- +nomad down\nDROP TABLE mysql_smoke_users;\n`
    );

    const upOutput = runCLI("up");
    expect(upOutput).toContain("↑ up 20240101120000 (create_users)");

    const [rowsAfterUp] = await connection.query("SHOW TABLES LIKE 'mysql_smoke_users'");
    expect(Array.isArray(rowsAfterUp) && rowsAfterUp.length).toBeTruthy();

    const downOutput = runCLI("down");
    expect(downOutput).toContain("↓ down 20240101120000 (create_users)");

    const [rowsAfterDown] = await connection.query("SHOW TABLES LIKE 'mysql_smoke_users'");
    expect(Array.isArray(rowsAfterDown) && rowsAfterDown.length).toBe(0);
  });

  it("reports status in JSON", () => {
    const statusOutput = runCLI("status --json");
    expect(() => JSON.parse(statusOutput)).not.toThrow();
  });
});
