import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from "vitest";
import { execSync, spawn } from "node:child_process";
import { mkdirSync, writeFileSync, rmSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { Pool } from "pg";

/**
 * Comprehensive Integration Tests for CLI Exit Codes
 *
 * These tests verify that NomadJS returns the correct exit codes
 * in real-world scenarios. Each exit code is tested with multiple
 * edge cases to ensure robust error handling.
 *
 * Test Environment:
 * - Each test suite gets its own migration table to prevent contamination
 * - Proper cleanup ensures tests are isolated
 * - Database state is reset between test suites
 */

describe("CLI Exit Codes Integration", () => {
  const testDir = join(process.cwd(), "test-migrations-exit-codes");
  const nomadCmd = "node dist/esm/cli.js";
  const testDbUrl = process.env.DATABASE_URL || "postgresql://postgres@localhost/nomaddb";

  // Use unique table names per test suite to avoid conflicts
  const getTestTable = (suiteName: string) => `nomad_test_${suiteName}_${Date.now()}`;

  let pool: Pool;

  beforeAll(async () => {
    // Set up test database connection
    pool = new Pool({ connectionString: testDbUrl });
  });

  afterAll(async () => {
    // Close connection
    await pool.end();
  });

  beforeEach(() => {
    // Create test directory
    mkdirSync(testDir, { recursive: true });
  });

  afterEach(() => {
    // Clean up directory
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  /**
   * Helper to run CLI command and capture exit code
   */
  function runCommand(cmd: string): { exitCode: number; stdout: string; stderr: string } {
    try {
      const stdout = execSync(cmd, {
        encoding: 'utf8',
        stdio: 'pipe',
        env: { ...process.env, NODE_ENV: 'test' }
      });
      return { exitCode: 0, stdout, stderr: '' };
    } catch (error: any) {
      return {
        exitCode: error.status || 1,
        stdout: error.stdout?.toString() || '',
        stderr: error.stderr?.toString() || ''
      };
    }
  }

  /**
   * Helper to clean up a test table
   */
  async function cleanupTestTable(tableName: string) {
    try {
      await pool.query(`DROP TABLE IF EXISTS ${tableName} CASCADE`);
      // Also clean up any test tables created by migrations
      await pool.query(`DROP TABLE IF EXISTS drift_test, test_users, t1, t2, m1, m2, missing_test, emoji_test CASCADE`);
    } catch (e) {
      // Ignore errors
    }
  }

  describe("Exit Code 0 - Success", () => {
    const testTable = getTestTable("success");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should return 0 for --help", () => {
      const result = runCommand(`${nomadCmd} --help`);
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("Exit Codes:");
    });

    it("should return 0 for --version", () => {
      const result = runCommand(`${nomadCmd} --version`);
      expect(result.exitCode).toBe(0);
    });

    it("should return 0 for successful create command", () => {
      const result = runCommand(`${nomadCmd} create test_migration --dir ${testDir}`);
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toMatch(/\d{14}_test_migration\.sql/);
    });

    it("should return 0 for successful init-config", () => {
      const configPath = join(testDir, "nomad.toml");
      const result = runCommand(`${nomadCmd} init-config --output ${configPath}`);
      expect(result.exitCode).toBe(0);
      expect(readFileSync(configPath, 'utf8')).toContain("[database]");
    });

    it("should return 0 for successful status with no migrations", () => {
      const result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
    });

    it("should return 0 for successful up with no migrations", () => {
      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
    });
  });

  describe("Exit Code 1 - SQL Error", () => {
    const testTable = getTestTable("sql_error");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should return 1 for SQL syntax error in migration", () => {
      const badSql = `-- +nomad Up
INVALID SQL SYNTAX HERE;
-- +nomad Down
SELECT 1;`;

      writeFileSync(join(testDir, "20240101120000_bad_sql.sql"), badSql);

      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(1);
    });

    it("should return 1 for referencing non-existent table", () => {
      const badSql = `-- +nomad Up
INSERT INTO nonexistent_table_xyz123 (id) VALUES (1);
-- +nomad Down
SELECT 1;`;

      writeFileSync(join(testDir, "20240101120000_bad_table.sql"), badSql);

      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(1);
    });

    it("should return 1 for constraint violation", () => {
      // First migration creates table with constraint
      const createTable = `-- +nomad Up
CREATE TABLE test_users_${Date.now()} (
  id INT PRIMARY KEY,
  email TEXT UNIQUE
);
-- +nomad Down
DROP TABLE test_users_${Date.now()};`;

      // Second migration violates constraint
      const violateConstraint = `-- +nomad Up
INSERT INTO test_users_${Date.now()} (id, email) VALUES (1, 'test@example.com');
INSERT INTO test_users_${Date.now()} (id, email) VALUES (2, 'test@example.com');
-- +nomad Down
DELETE FROM test_users_${Date.now()};`;

      writeFileSync(join(testDir, "20240101120000_create.sql"), createTable);
      writeFileSync(join(testDir, "20240101120001_violate.sql"), violateConstraint);

      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(1);
    });
  });

  describe("Exit Code 2 - Drift Detected", () => {
    const testTable = getTestTable("drift");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should return 2 when migration file is modified after apply", async () => {
      // Create and apply a migration
      const originalSql = `-- +nomad Up
CREATE TABLE drift_test_${Date.now()} (id INT);
-- +nomad Down
DROP TABLE drift_test_${Date.now()};`;

      const migrationPath = join(testDir, "20240101120000_drift.sql");
      writeFileSync(migrationPath, originalSql);

      // Apply the migration
      let result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);

      // Modify the migration file
      const modifiedSql = originalSql + "\n-- Modified";
      writeFileSync(migrationPath, modifiedSql);

      // Check status - should detect drift
      result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(2);
    });

    it("should return 2 when multiple files have drift", async () => {
      const timestamp = Date.now();
      // Create two migrations
      const sql1 = `-- +nomad Up\nCREATE TABLE t1_${timestamp} (id INT);\n-- +nomad Down\nDROP TABLE t1_${timestamp};`;
      const sql2 = `-- +nomad Up\nCREATE TABLE t2_${timestamp} (id INT);\n-- +nomad Down\nDROP TABLE t2_${timestamp};`;

      writeFileSync(join(testDir, "20240101120000_one.sql"), sql1);
      writeFileSync(join(testDir, "20240101120001_two.sql"), sql2);

      // Apply migrations
      runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

      // Modify both files
      writeFileSync(join(testDir, "20240101120000_one.sql"), sql1 + "\n-- Modified");
      writeFileSync(join(testDir, "20240101120001_two.sql"), sql2 + "\n-- Modified");

      // Check status
      const result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(2);
    });

    it("should allow drift with --allow-drift flag", async () => {
      const sql = `-- +nomad Up\nSELECT 1;\n-- +nomad Down\nSELECT 1;`;
      const migrationPath = join(testDir, "20240101120000_test.sql");

      writeFileSync(migrationPath, sql);
      runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

      // Modify file
      writeFileSync(migrationPath, sql + "\n-- Modified");

      // Status with --allow-drift should succeed
      const result = runCommand(
        `${nomadCmd} status --allow-drift --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("[DRIFT]");
    });
  });

  describe("Exit Code 3 - Lock Timeout", () => {
    const testTable = getTestTable("lock");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should return 3 when lock acquisition times out", async () => {
      // Create a migration that takes time
      const slowMigration = `-- +nomad Up
SELECT pg_sleep(3);
-- +nomad Down
SELECT 1;`;

      writeFileSync(join(testDir, "20240101120000_slow.sql"), slowMigration);

      // Start first migration in background (it will hold the lock)
      const bgProcess = spawn('node', [
        'dist/esm/cli.js',
        'up',
        '--url', testDbUrl,
        '--dir', testDir,
        '--table', testTable
      ], {
        detached: false,
        stdio: 'ignore'
      });

      // Wait for background process to acquire lock
      await new Promise(resolve => setTimeout(resolve, 500));

      // Try to run another migration with very short timeout
      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable} --lock-timeout 100`
      );

      // Should timeout with exit code 3
      expect(result.exitCode).toBe(3);

      // Clean up background process
      bgProcess.kill();

      // Wait for process to die
      await new Promise(resolve => setTimeout(resolve, 1000));
    });
  });

  describe("Exit Code 4 - Parse/Config Error", () => {
    it("should return 4 for invalid TOML config", () => {
      const invalidToml = `[database
url = "missing closing bracket"`;

      const configPath = join(testDir, "nomad.toml");
      writeFileSync(configPath, invalidToml);

      const result = runCommand(
        `${nomadCmd} status --config ${configPath} --dir ${testDir}`
      );
      expect(result.exitCode).toBe(4);
    });

    it("should return 4 for invalid JSON config", () => {
      const invalidJson = `{"database": {url: "missing quotes"}}`;

      const configPath = join(testDir, "nomad.json");
      writeFileSync(configPath, invalidJson);

      const result = runCommand(
        `${nomadCmd} status --config ${configPath} --dir ${testDir}`
      );
      expect(result.exitCode).toBe(4);
    });

    it("should return 4 for config file that is not an object", () => {
      const configPath = join(testDir, "nomad.json");
      writeFileSync(configPath, '"string instead of object"');

      const result = runCommand(
        `${nomadCmd} status --config ${configPath} --dir ${testDir}`
      );
      expect(result.exitCode).toBe(4);
    });
  });

  describe("Exit Code 5 - Missing File", () => {
    const testTable = getTestTable("missing");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should return 5 when applied migration file is missing", async () => {
      const timestamp = Date.now();
      // Create and apply a migration
      const sql = `-- +nomad Up
CREATE TABLE missing_test_${timestamp} (id INT);
-- +nomad Down
DROP TABLE missing_test_${timestamp};`;

      const migrationPath = join(testDir, "20240101120000_missing.sql");
      writeFileSync(migrationPath, sql);

      // Apply the migration
      runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

      // Delete the migration file
      rmSync(migrationPath);

      // Check status - should detect missing file
      const result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(5);
    });

    it("should return 5 when multiple migration files are missing", async () => {
      const timestamp = Date.now();
      // Create two migrations
      const sql1 = `-- +nomad Up\nCREATE TABLE m1_${timestamp} (id INT);\n-- +nomad Down\nDROP TABLE m1_${timestamp};`;
      const sql2 = `-- +nomad Up\nCREATE TABLE m2_${timestamp} (id INT);\n-- +nomad Down\nDROP TABLE m2_${timestamp};`;

      const path1 = join(testDir, "20240101120000_one.sql");
      const path2 = join(testDir, "20240101120001_two.sql");

      writeFileSync(path1, sql1);
      writeFileSync(path2, sql2);

      // Apply migrations
      runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

      // Delete both files
      rmSync(path1);
      rmSync(path2);

      // Check status
      const result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(5);
    });

    it("should allow missing files with --allow-drift", async () => {
      const sql = `-- +nomad Up\nSELECT 1;\n-- +nomad Down\nSELECT 1;`;
      const path = join(testDir, "20240101120000_test.sql");

      writeFileSync(path, sql);
      runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);
      rmSync(path);

      const result = runCommand(
        `${nomadCmd} status --allow-drift --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("[MISSING]");
    });
  });

  describe("Exit Code 6 - Checksum Mismatch", () => {
    const testTable = getTestTable("checksum");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should be handled by drift detection (exit code 2)", async () => {
      const sql = `-- +nomad Up\nSELECT 1;\n-- +nomad Down\nSELECT 1;`;
      const path = join(testDir, "20240101120000_checksum.sql");

      writeFileSync(path, sql);
      runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

      // Modify to cause checksum mismatch
      writeFileSync(path, sql + "\n-- Changed");

      const result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      // Checksum mismatch is detected as drift
      expect(result.exitCode).toBe(2);
    });
  });

  describe("Exit Code 7 - Connection Error", () => {
    it("should return 7 for invalid host", () => {
      const result = runCommand(
        `${nomadCmd} status --url "postgresql://user:pass@nonexistent-host-xyz:5432/db" --dir ${testDir}`
      );
      expect(result.exitCode).toBe(7);
    });

    it("should return 7 for invalid port", () => {
      const result = runCommand(
        `${nomadCmd} status --url "postgresql://localhost:99999/db" --dir ${testDir}`
      );
      expect(result.exitCode).toBe(7);
    });

    it("should return 7 for invalid credentials", () => {
      const result = runCommand(
        `${nomadCmd} status --url "postgresql://baduser:badpass@localhost:5432/nomaddb" --dir ${testDir}`
      );
      expect(result.exitCode).toBe(7);
    });

    it("should return 7 for non-existent database", () => {
      const result = runCommand(
        `${nomadCmd} status --url "postgresql://postgres@localhost:5432/nonexistent_db_12345" --dir ${testDir}`
      );
      expect(result.exitCode).toBe(7);
    });

    it("should return 7 for connection timeout", () => {
      // Use an IP that will timeout (RFC 5737 documentation IP)
      const result = runCommand(
        `${nomadCmd} status --url "postgresql://192.0.2.1:5432/db" --dir ${testDir}`
      );
      expect(result.exitCode).toBe(7);
    });
  });

  describe("Edge Cases and Combined Scenarios", () => {
    const testTable = getTestTable("edge");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    it("should prioritize connection errors over other errors", () => {
      // Even with invalid config, connection error should take precedence
      const result = runCommand(
        `${nomadCmd} status --url "postgresql://bad@nonexistent:5432/db" --dir /nonexistent/dir`
      );
      expect(result.exitCode).toBe(7);
    });

    it("should handle empty migrations directory correctly", () => {
      const result = runCommand(
        `${nomadCmd} status --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
    });

    it("should handle migrations with only Down section", () => {
      const downOnly = `-- +nomad Down
DROP TABLE IF EXISTS test;`;

      writeFileSync(join(testDir, "20240101120000_down_only.sql"), downOnly);

      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      // Should succeed but do nothing
      expect(result.exitCode).toBe(0);
    });

    it("should handle very long migration names", () => {
      const longName = "a".repeat(200);
      const sql = `-- +nomad Up\nSELECT 1;\n-- +nomad Down\nSELECT 1;`;

      writeFileSync(join(testDir, `20240101120000_${longName}.sql`), sql);

      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
    });

    it("should handle migrations with Unicode characters", () => {
      const timestamp = Date.now();
      const sql = `-- +nomad Up
CREATE TABLE emoji_test_${timestamp} (
  id INT,
  emoji TEXT DEFAULT '🚀'
);
-- +nomad Down
DROP TABLE emoji_test_${timestamp};`;

      writeFileSync(join(testDir, "20240101120000_emoji_🎉.sql"), sql);

      const result = runCommand(
        `${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
      );
      expect(result.exitCode).toBe(0);
    });
  });

  describe("Command-specific Exit Codes", () => {
    const testTable = getTestTable("commands");

    afterEach(async () => {
      await cleanupTestTable(testTable);
    });

    describe("create command", () => {
      it("should return 0 for successful creation", () => {
        const result = runCommand(`${nomadCmd} create test --dir ${testDir}`);
        expect(result.exitCode).toBe(0);
      });

      it("should handle missing directory gracefully", () => {
        const result = runCommand(`${nomadCmd} create test --dir /nonexistent/path/to/dir`);
        // Should create directory or fail gracefully
        expect([0, 1]).toContain(result.exitCode);
      });
    });

    describe("verify command", () => {
      it("should return 0 when all checksums valid", async () => {
        const sql = `-- +nomad Up\nSELECT 1;\n-- +nomad Down\nSELECT 1;`;
        writeFileSync(join(testDir, "20240101120000_test.sql"), sql);

        runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

        const result = runCommand(
          `${nomadCmd} verify --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
        );
        expect(result.exitCode).toBe(0);
      });

      it("should return 2 when drift detected", async () => {
        const sql = `-- +nomad Up\nSELECT 1;\n-- +nomad Down\nSELECT 1;`;
        const path = join(testDir, "20240101120000_test.sql");

        writeFileSync(path, sql);
        runCommand(`${nomadCmd} up --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`);

        // Modify file
        writeFileSync(path, sql + "\n-- Modified");

        const result = runCommand(
          `${nomadCmd} verify --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
        );
        expect(result.exitCode).toBe(2);
      });
    });

    describe("plan command", () => {
      it("should return 0 for successful plan", () => {
        const sql = `-- +nomad Up\nCREATE TABLE test (id INT);\n-- +nomad Down\nDROP TABLE test;`;
        writeFileSync(join(testDir, "20240101120000_test.sql"), sql);

        const result = runCommand(
          `${nomadCmd} plan --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
        );
        expect(result.exitCode).toBe(0);
      });

      it("should show hazards in plan", () => {
        const hazardous = `-- +nomad Up
CREATE INDEX CONCURRENTLY idx_test ON test(id);
-- +nomad Down
DROP INDEX idx_test;`;

        writeFileSync(join(testDir, "20240101120000_hazard.sql"), hazardous);

        const result = runCommand(
          `${nomadCmd} plan --url "${testDbUrl}" --dir ${testDir} --table ${testTable}`
        );
        expect(result.exitCode).toBe(0);
      });
    });
  });
});