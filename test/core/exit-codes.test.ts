import { describe, it, expect } from "vitest";
import {
  ExitCode,
  NomadError,
  SqlError,
  DriftError,
  LockTimeoutError,
  ParseConfigError,
  MissingFileError,
  ChecksumMismatchError,
  ConnectionError,
  getExitCodeDescription,
  formatExitCodesHelp
} from "../../src/core/errors.js";

describe("Exit Codes", () => {
  describe("ExitCode enum", () => {
    it("should have correct numeric values", () => {
      expect(ExitCode.SUCCESS).toBe(0);
      expect(ExitCode.SQL_ERROR).toBe(1);
      expect(ExitCode.DRIFT_DETECTED).toBe(2);
      expect(ExitCode.LOCK_TIMEOUT).toBe(3);
      expect(ExitCode.PARSE_CONFIG_ERROR).toBe(4);
      expect(ExitCode.MISSING_FILE).toBe(5);
      expect(ExitCode.CHECKSUM_MISMATCH).toBe(6);
      expect(ExitCode.CONNECTION_ERROR).toBe(7);
    });
  });

  describe("Error classes", () => {
    it("should create SqlError with correct exit code", () => {
      const error = new SqlError("SELECT failed", { sql: "SELECT * FROM users" });
      expect(error).toBeInstanceOf(NomadError);
      expect(error.exitCode).toBe(ExitCode.SQL_ERROR);
      expect(error.message).toBe("SELECT failed");
      expect(error.sql).toBe("SELECT * FROM users");
    });

    it("should create DriftError with single version", () => {
      const error = new DriftError(["20250101120000"]);
      expect(error.exitCode).toBe(ExitCode.DRIFT_DETECTED);
      expect(error.message).toBe("Drift detected in migration 20250101120000");
    });

    it("should create DriftError with multiple versions", () => {
      const error = new DriftError(["20250101120000", "20250102130000"]);
      expect(error.exitCode).toBe(ExitCode.DRIFT_DETECTED);
      expect(error.message).toBe("Drift detected in 2 migrations: 20250101120000, 20250102130000");
    });

    it("should create LockTimeoutError", () => {
      const error = new LockTimeoutError(30000);
      expect(error.exitCode).toBe(ExitCode.LOCK_TIMEOUT);
      expect(error.message).toBe("Failed to acquire migration lock within 30000ms");
    });

    it("should create ParseConfigError", () => {
      const error = new ParseConfigError("Invalid TOML syntax at line 5");
      expect(error.exitCode).toBe(ExitCode.PARSE_CONFIG_ERROR);
      expect(error.message).toBe("Invalid TOML syntax at line 5");
    });

    it("should create MissingFileError with single file", () => {
      const error = new MissingFileError(["20250101120000_create_users.sql"]);
      expect(error.exitCode).toBe(ExitCode.MISSING_FILE);
      expect(error.message).toBe("Missing migration file: 20250101120000_create_users.sql");
    });

    it("should create MissingFileError with multiple files", () => {
      const error = new MissingFileError(["20250101120000_one.sql", "20250102130000_two.sql"]);
      expect(error.exitCode).toBe(ExitCode.MISSING_FILE);
      expect(error.message).toBe("Missing 2 migration files: 20250101120000_one.sql, 20250102130000_two.sql");
    });

    it("should create ChecksumMismatchError", () => {
      const error = new ChecksumMismatchError({
        version: 20250101120000n,
        name: "create_users",
        expectedChecksum: "abc123",
        actualChecksum: "def456",
        filepath: "/migrations/20250101120000_create_users.sql"
      });
      expect(error.exitCode).toBe(ExitCode.CHECKSUM_MISMATCH);
      expect(error.message).toContain("Checksum mismatch for migration 20250101120000");
      expect(error.message).toContain("Expected: abc123");
      expect(error.message).toContain("Actual: def456");
      expect(error.version).toBe(20250101120000n);
      expect(error.expectedChecksum).toBe("abc123");
      expect(error.actualChecksum).toBe("def456");
      expect(error.filepath).toBe("/migrations/20250101120000_create_users.sql");
    });

    it("should create ConnectionError", () => {
      const error = new ConnectionError("Connection refused");
      expect(error.exitCode).toBe(ExitCode.CONNECTION_ERROR);
      expect(error.message).toBe("Database connection error: Connection refused");
    });
  });

  describe("Edge cases", () => {
    it("should handle empty arrays in DriftError", () => {
      const error = new DriftError([]);
      expect(error.exitCode).toBe(ExitCode.DRIFT_DETECTED);
      expect(error.message).toBe("Drift detected in 0 migrations: ");
    });

    it("should handle empty arrays in MissingFileError", () => {
      const error = new MissingFileError([]);
      expect(error.exitCode).toBe(ExitCode.MISSING_FILE);
      expect(error.message).toBe("Missing 0 migration files: ");
    });

    it("should handle SqlError without SQL string", () => {
      const error = new SqlError("Query failed");
      expect(error.sql).toBeUndefined();
      expect(error.message).toBe("Query failed");
    });

    it("should preserve error names", () => {
      expect(new SqlError("test").name).toBe("SqlError");
      expect(new DriftError(["test"]).name).toBe("DriftError");
      expect(new LockTimeoutError(1000).name).toBe("LockTimeoutError");
      expect(new ParseConfigError("test").name).toBe("ParseConfigError");
      expect(new MissingFileError(["test"]).name).toBe("MissingFileError");
      expect(new ChecksumMismatchError({
        version: 1n,
        name: "test",
        expectedChecksum: "a",
        actualChecksum: "b",
        filepath: "test.sql"
      }).name).toBe("ChecksumMismatchError");
      expect(new ConnectionError("test").name).toBe("ConnectionError");
    });

    it("should handle unknown exit code in getExitCodeDescription", () => {
      const result = getExitCodeDescription(999 as ExitCode);
      expect(result).toBe("Unknown error");
    });

    it("should preserve stack traces", () => {
      const error = new SqlError("test");
      expect(error.stack).toBeDefined();
      expect(error.stack).toContain("SqlError");
    });

    it("should serialize to JSON properly", () => {
      const error = new DriftError(["20250101120000"]);
      const json = JSON.stringify({
        message: error.message,
        exitCode: error.exitCode,
        name: error.name
      });
      const parsed = JSON.parse(json);
      expect(parsed.exitCode).toBe(2);
      expect(parsed.name).toBe("DriftError");
    });

    it("should handle very long version lists", () => {
      const versions = Array.from({ length: 100 }, (_, i) =>
        `2025010${i.toString().padStart(2, '0')}120000`
      );
      const error = new DriftError(versions);
      expect(error.message).toContain("100 migrations");
      expect(error.message.length).toBeLessThan(10000); // Reasonable message length
    });

    it("should handle special characters in error messages", () => {
      const error = new ParseConfigError("Invalid char: \n\t\"'`");
      expect(error.message).toBe("Invalid char: \n\t\"'`");
    });
  });

  describe("Helper functions", () => {
    it("should get correct exit code descriptions", () => {
      expect(getExitCodeDescription(ExitCode.SUCCESS)).toBe("Success");
      expect(getExitCodeDescription(ExitCode.SQL_ERROR)).toBe("SQL execution error");
      expect(getExitCodeDescription(ExitCode.DRIFT_DETECTED)).toBe("Drift detected in applied migrations");
      expect(getExitCodeDescription(ExitCode.LOCK_TIMEOUT)).toBe("Lock acquisition timeout");
      expect(getExitCodeDescription(ExitCode.PARSE_CONFIG_ERROR)).toBe("Parse or configuration error");
      expect(getExitCodeDescription(ExitCode.MISSING_FILE)).toBe("Missing migration file");
      expect(getExitCodeDescription(ExitCode.CHECKSUM_MISMATCH)).toBe("Checksum mismatch");
      expect(getExitCodeDescription(ExitCode.CONNECTION_ERROR)).toBe("Database connection error");
    });

    it("should format exit codes help text", () => {
      const help = formatExitCodesHelp();
      expect(help).toContain("0 - Success");
      expect(help).toContain("1 - SQL execution error");
      expect(help).toContain("2 - Drift detected");
      expect(help).toContain("3 - Lock acquisition timeout");
      expect(help).toContain("4 - Parse or configuration error");
      expect(help).toContain("5 - Missing migration file");
      expect(help).toContain("6 - Checksum mismatch");
      expect(help).toContain("7 - Database connection error");

      // Check formatting
      const lines = help.split("\n");
      expect(lines).toHaveLength(8);
      lines.forEach(line => {
        expect(line).toMatch(/^  \d - .+$/);
      });
    });
  });

  describe("Error inheritance", () => {
    it("should have all errors inherit from NomadError", () => {
      const errors = [
        new SqlError("test"),
        new DriftError(["test"]),
        new LockTimeoutError(1000),
        new ParseConfigError("test"),
        new MissingFileError(["test"]),
        new ChecksumMismatchError({
          version: 1n,
          name: "test",
          expectedChecksum: "a",
          actualChecksum: "b",
          filepath: "test.sql"
        }),
        new ConnectionError("test")
      ];

      errors.forEach(error => {
        expect(error).toBeInstanceOf(NomadError);
        expect(error).toBeInstanceOf(Error);
        expect(typeof error.exitCode).toBe("number");
        expect(error.exitCode).toBeGreaterThanOrEqual(1);
        expect(error.exitCode).toBeLessThanOrEqual(7);
      });
    });
  });
});
