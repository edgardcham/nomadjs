/**
 * CLI integration tests for the redo command
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { execSync } from "child_process";
import { mkdirSync, rmSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { Pool } from "pg";

describe("CLI: nomad redo command", () => {
  const testDbUrl = process.env.DATABASE_URL || "postgresql://localhost/test";
  const testTable = `nomad_test_redo_${Date.now()}`;
  const testDir = join(process.cwd(), `test-migrations-redo`);
  const nomadCmd = "node dist/esm/cli.js";
  const pool = new Pool({ connectionString: testDbUrl });

  beforeAll(async () => {
    // Ensure test table doesn't exist
    try {
      await pool.query(`DROP TABLE IF EXISTS ${testTable}`);
    } catch {
      // Ignore if table doesn't exist
    }
  });

  afterAll(async () => {
    // Clean up test tables and close connection
    try {
      // Drop migration table
      await pool.query(`DROP TABLE IF EXISTS ${testTable}`);
      // Drop all tables created by test migrations
      await pool.query(`DROP TABLE IF EXISTS redo_test, test, first_table, second_table, test_table, test_tx, bad_test CASCADE`);
    } catch {
      // Ignore errors
    }
    await pool.end();
  });

  beforeEach(async () => {
    // Clean up any leftover tables from previous tests
    try {
      await pool.query(`DROP TABLE IF EXISTS redo_test, test, first_table, second_table, test_table, test_tx, bad_test CASCADE`);
    } catch {
      // Ignore errors
    }

    // Create test directory
    mkdirSync(testDir, { recursive: true });
  });

  afterEach(() => {
    // Clean up directory
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  const runCLI = (args: string, expectError = false): string => {
    try {
      const result = execSync(
        `${nomadCmd} --dir ${testDir} --table ${testTable} ${args}`,
        {
          encoding: "utf8",
          env: { ...process.env, DATABASE_URL: testDbUrl }
        }
      );
      return result.toString();
    } catch (error: any) {
      if (expectError) {
        return error.stdout || error.stderr || error.message;
      }
      throw new Error(`CLI failed: ${error.message}\nOutput: ${error.stdout}\nError: ${error.stderr}`);
    }
  };

  describe("Basic Usage", () => {
    it("should show help for redo command", () => {
      const output = runCLI("redo --help");
      expect(output).toContain("Rollback and reapply");
      expect(output).toContain("migration");
      expect(output).not.toContain("[migrationVersion]");
    });

    it.skip("should redo the last migration", async () => {
      // Create a migration
      const migrationFile = join(testDir, "20240101120000_test.sql");
      writeFileSync(migrationFile, `-- +nomad up
CREATE TABLE redo_test (id int);
-- +nomad down
DROP TABLE redo_test;`);

      // Apply the migration
      runCLI("up");

      // Redo should rollback and reapply
      const output = runCLI("redo");
      expect(output).toContain("Rolling back 20240101120000");
      expect(output).toContain("Reapplying 20240101120000");
      expect(output).toContain("Redo complete");

      // Verify the table still exists
      const result = await pool.query(`SELECT to_regclass('redo_test')`);
      expect(result.rows[0].to_regclass).toBe("redo_test");
    });

    it.skip("should redo a specific migration by version", async () => {
      // Create two migrations
      const migration1 = join(testDir, "20240101120000_first.sql");
      writeFileSync(migration1, `-- +nomad up
CREATE TABLE first_table (id int);
-- +nomad down
DROP TABLE first_table;`);

      const migration2 = join(testDir, "20240102120000_second.sql");
      writeFileSync(migration2, `-- +nomad up
CREATE TABLE second_table (id int);
-- +nomad down
DROP TABLE second_table;`);

      // Apply both migrations
      runCLI("up");

      // Redo the first migration specifically
      const output = runCLI("redo 20240101120000");
      expect(output).toContain("Rolling back 20240101120000");
      expect(output).toContain("Reapplying 20240101120000");
      expect(output).toContain("Redo complete");

      // Verify both tables still exist
      const result1 = await pool.query(`SELECT to_regclass('first_table')`);
      expect(result1.rows[0].to_regclass).toBe("first_table");

      const result2 = await pool.query(`SELECT to_regclass('second_table')`);
      expect(result2.rows[0].to_regclass).toBe("second_table");
    });
  });

  // Note: Additional error case tests are covered in the unit tests
  // The CLI integration tests focus on the happy path scenarios
});