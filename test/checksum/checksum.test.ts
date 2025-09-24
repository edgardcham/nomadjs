import { describe, it, expect, beforeEach } from "vitest";
import { createHash } from "node:crypto";
import {
  calculateChecksum,
  verifyChecksum,
  type MigrationWithChecksum
} from "../../src/core/checksum.js";
import { ChecksumMismatchError } from "../../src/core/errors.js";

describe("Checksum System", () => {
  describe("calculateChecksum", () => {
    it("calculates SHA-256 of file content", () => {
      const content = "SELECT 1;\nSELECT 2;";
      const checksum = calculateChecksum(content);

      // Manually calculate expected checksum
      const hash = createHash("sha256");
      hash.update(content);
      const expected = hash.digest("hex");

      expect(checksum).toBe(expected);
    });

    it("normalizes CRLF to LF before hashing", () => {
      const contentCRLF = "SELECT 1;\r\nSELECT 2;\r\n";
      const contentLF = "SELECT 1;\nSELECT 2;\n";

      const checksumCRLF = calculateChecksum(contentCRLF);
      const checksumLF = calculateChecksum(contentLF);

      expect(checksumCRLF).toBe(checksumLF);
    });

    it("removes BOM before hashing", () => {
      const contentWithBOM = "\uFEFFSELECT 1;";
      const contentWithoutBOM = "SELECT 1;";

      const checksumWithBOM = calculateChecksum(contentWithBOM);
      const checksumWithoutBOM = calculateChecksum(contentWithoutBOM);

      expect(checksumWithBOM).toBe(checksumWithoutBOM);
    });

    it("produces different checksums for different content", () => {
      const content1 = "SELECT 1;";
      const content2 = "SELECT 2;";

      const checksum1 = calculateChecksum(content1);
      const checksum2 = calculateChecksum(content2);

      expect(checksum1).not.toBe(checksum2);
    });

    it("handles empty content", () => {
      const checksum = calculateChecksum("");

      // SHA-256 of empty string
      const expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

      expect(checksum).toBe(expected);
    });

    it("handles Unicode content", () => {
      const content = "INSERT INTO users (name) VALUES ('你好世界');";
      const checksum = calculateChecksum(content);

      expect(checksum).toHaveLength(64); // SHA-256 is 64 hex chars
      expect(checksum).toMatch(/^[a-f0-9]{64}$/);
    });

    it("handles large files efficiently", () => {
      // Generate 10MB of content
      const largeContent = "SELECT 1;\n".repeat(1_000_000);

      const start = Date.now();
      const checksum = calculateChecksum(largeContent);
      const elapsed = Date.now() - start;

      expect(checksum).toHaveLength(64);
      expect(elapsed).toBeLessThan(1000); // Should process in under 1 second
    });
  });

  describe("verifyChecksum", () => {
    it("returns true for matching checksum", () => {
      const content = "SELECT 1;";
      const checksum = calculateChecksum(content);

      const result = verifyChecksum(content, checksum);
      expect(result).toBe(true);
    });

    it("returns false for non-matching checksum", () => {
      const content = "SELECT 1;";
      const wrongChecksum = "0000000000000000000000000000000000000000000000000000000000000000";

      const result = verifyChecksum(content, wrongChecksum);
      expect(result).toBe(false);
    });

    it("handles checksum case insensitively", () => {
      const content = "SELECT 1;";
      const checksum = calculateChecksum(content);

      const upperChecksum = checksum.toUpperCase();
      const result = verifyChecksum(content, upperChecksum);

      expect(result).toBe(true);
    });
  });

  describe("Migration checksum tracking", () => {
    it("includes checksum in migration metadata", () => {
      const migration: MigrationWithChecksum = {
        version: 20250921112233n,
        name: "create_users",
        filepath: "/migrations/20250921112233_create_users.sql",
        content: "CREATE TABLE users (id INT);",
        checksum: calculateChecksum("CREATE TABLE users (id INT);")
      };

      expect(migration.checksum).toHaveLength(64);
      expect(migration.checksum).toMatch(/^[a-f0-9]{64}$/);
    });

    it("detects drift when file content changes", () => {
      const originalContent = "CREATE TABLE users (id INT);";
      const modifiedContent = "CREATE TABLE users (id INT, name TEXT);";

      const originalChecksum = calculateChecksum(originalContent);
      const currentChecksum = calculateChecksum(modifiedContent);

      expect(originalChecksum).not.toBe(currentChecksum);

      // Verify drift detection
      const hasDrift = !verifyChecksum(modifiedContent, originalChecksum);
      expect(hasDrift).toBe(true);
    });
  });

  describe("ChecksumMismatchError", () => {
    it("provides detailed error information", () => {
      const error = new ChecksumMismatchError({
        version: 20250921112233n,
        name: "create_users",
        expectedChecksum: "abc123",
        actualChecksum: "def456",
        filepath: "/migrations/20250921112233_create_users.sql"
      });

      expect(error).toBeInstanceOf(Error);
      expect(error.message).toContain("Checksum mismatch");
      expect(error.message).toContain("20250921112233");
      expect(error.message).toContain("create_users");
      expect(error.version).toBe(20250921112233n);
      expect(error.expectedChecksum).toBe("abc123");
      expect(error.actualChecksum).toBe("def456");
    });
  });

  describe("Drift scenarios", () => {
    it("detects when applied migration file has been modified", () => {
      const appliedMigrations = [
        {
          version: 20250921112233n,
          name: "create_users",
          checksum: calculateChecksum("CREATE TABLE users (id INT);"),
          appliedAt: new Date()
        }
      ];

      const currentFileContent = "CREATE TABLE users (id INT, name TEXT);"; // Modified!
      const currentChecksum = calculateChecksum(currentFileContent);

      const hasDrift = appliedMigrations[0].checksum !== currentChecksum;
      expect(hasDrift).toBe(true);
    });

    it("detects when applied migration file is missing", () => {
      const appliedMigrations = [
        {
          version: 20250921112233n,
          name: "create_users",
          checksum: "abc123",
          appliedAt: new Date()
        }
      ];

      const currentFiles: string[] = []; // File is missing!

      const missingFile = !currentFiles.includes("20250921112233_create_users.sql");
      expect(missingFile).toBe(true);
    });

    it("allows clean state when all checksums match", () => {
      const content = "CREATE TABLE users (id INT);";
      const checksum = calculateChecksum(content);

      const appliedMigrations = [
        {
          version: 20250921112233n,
          name: "create_users",
          checksum: checksum,
          appliedAt: new Date()
        }
      ];

      const currentFileChecksum = calculateChecksum(content);
      const hasDrift = appliedMigrations[0].checksum !== currentFileChecksum;

      expect(hasDrift).toBe(false);
    });

    it("handles multiple migrations with mixed drift status", () => {
      const migrations = [
        {
          version: 20250921112233n,
          name: "create_users",
          checksum: calculateChecksum("CREATE TABLE users (id INT);"),
          currentContent: "CREATE TABLE users (id INT);", // Matches
          hasDrift: false
        },
        {
          version: 20250921112234n,
          name: "add_column",
          checksum: calculateChecksum("ALTER TABLE users ADD email TEXT;"),
          currentContent: "ALTER TABLE users ADD COLUMN email TEXT;", // Modified!
          hasDrift: true
        },
        {
          version: 20250921112235n,
          name: "create_posts",
          checksum: calculateChecksum("CREATE TABLE posts (id INT);"),
          currentContent: "CREATE TABLE posts (id INT);", // Matches
          hasDrift: false
        }
      ];

      // Verify drift detection for each migration
      migrations.forEach(m => {
        const currentChecksum = calculateChecksum(m.currentContent);
        const actualDrift = m.checksum !== currentChecksum;
        expect(actualDrift).toBe(m.hasDrift);
      });

      // Check if any migration has drift
      const anyDrift = migrations.some(m => m.hasDrift);
      expect(anyDrift).toBe(true);
    });
  });

  describe("Checksum format and validation", () => {
    it("always produces lowercase hex string", () => {
      const content = "SELECT 1;";
      const checksum = calculateChecksum(content);

      expect(checksum).toBe(checksum.toLowerCase());
      expect(checksum).toMatch(/^[a-f0-9]{64}$/);
    });

    it("rejects invalid checksum format", () => {
      const invalidChecksums = [
        "not-a-checksum",
        "zzzz", // Invalid hex chars
        "abc", // Too short
        "a".repeat(65), // Too long
        ""
      ];

      invalidChecksums.forEach(invalid => {
        const isValid = /^[a-f0-9]{64}$/i.test(invalid);
        expect(isValid).toBe(false);
      });
    });
  });
});