import { describe, it, expect } from "vitest";
import { calculateChecksum, verifyChecksum, isValidChecksum } from "../../src/core/checksum.js";

describe("Checksum Edge Cases", () => {
  describe("Special characters and encoding edge cases", () => {
    it("handles files with only whitespace", () => {
      const whitespaceContent = "   \n\t\r\n  \t  ";
      const checksum = calculateChecksum(whitespaceContent);

      expect(checksum).toHaveLength(64);
      // Should produce consistent checksum for whitespace
      const checksum2 = calculateChecksum("   \n\t\n  \t  "); // CRLF normalized
      expect(checksum).toBe(checksum2);
    });

    it("handles files with mixed newline styles", () => {
      const mixed = "Line 1\rLine 2\nLine 3\r\nLine 4";
      const normalized = "Line 1\nLine 2\nLine 3\nLine 4"; // All should normalize to LF

      const checksumMixed = calculateChecksum(mixed);
      const checksumNormalized = calculateChecksum(normalized);

      // Both should produce same checksum after normalization
      expect(checksumMixed).toBe(checksumNormalized);
    });

    it("handles multiple BOMs (malformed files)", () => {
      const multipleBOMs = "\uFEFF\uFEFFSELECT 1;";
      const singleBOM = "\uFEFFSELECT 1;";
      const noBOM = "SELECT 1;";

      const checksum1 = calculateChecksum(multipleBOMs);
      const checksum2 = calculateChecksum(singleBOM);
      const checksum3 = calculateChecksum(noBOM);

      // Only first BOM should be removed
      expect(checksum1).not.toBe(checksum2);
      expect(checksum2).toBe(checksum3);
    });

    it("handles null bytes in content", () => {
      const withNull = "SELECT 1;\0SELECT 2;";
      const checksum = calculateChecksum(withNull);

      expect(checksum).toHaveLength(64);
      // Should handle null bytes without error
      const verified = verifyChecksum(withNull, checksum);
      expect(verified).toBe(true);
    });

    it("handles very long single-line SQL", () => {
      // Generate a single line with 100k characters
      const longLine = "SELECT " + "'x'".repeat(50000) + " FROM test";
      const checksum = calculateChecksum(longLine);

      expect(checksum).toHaveLength(64);
      expect(checksum).toMatch(/^[a-f0-9]{64}$/);
    });

    it("handles binary data masquerading as text", () => {
      // Simulate binary data in string
      const binaryLike = String.fromCharCode(...Array.from({ length: 256 }, (_, i) => i));
      const checksum = calculateChecksum(binaryLike);

      expect(checksum).toHaveLength(64);
      expect(checksum).toMatch(/^[a-f0-9]{64}$/);
    });

    it("handles emoji and 4-byte UTF-8 characters", () => {
      const emojiSQL = "INSERT INTO messages (text) VALUES ('Hello 👋 🌍 🚀');";
      const checksum = calculateChecksum(emojiSQL);

      expect(checksum).toHaveLength(64);
      // Should be deterministic
      const checksum2 = calculateChecksum(emojiSQL);
      expect(checksum).toBe(checksum2);
    });

    it("handles RTL (right-to-left) text", () => {
      const rtlSQL = "INSERT INTO users (name) VALUES ('مرحبا بالعالم');"; // Arabic
      const checksum = calculateChecksum(rtlSQL);

      expect(checksum).toHaveLength(64);
      expect(checksum).toMatch(/^[a-f0-9]{64}$/);
    });

    it("handles zero-width characters", () => {
      const withZeroWidth = "SELECT\u200B 1;"; // Zero-width space
      const without = "SELECT 1;";

      const checksum1 = calculateChecksum(withZeroWidth);
      const checksum2 = calculateChecksum(without);

      // Should produce different checksums (zero-width chars are significant)
      expect(checksum1).not.toBe(checksum2);
    });
  });

  describe("Checksum comparison edge cases", () => {
    it("handles undefined and null gracefully", () => {
      expect(() => calculateChecksum(null as any)).toThrow();
      expect(() => calculateChecksum(undefined as any)).toThrow();

      expect(verifyChecksum("test", null as any)).toBe(false);
      expect(verifyChecksum("test", undefined as any)).toBe(false);
    });

    it("handles empty string checksum", () => {
      const result = verifyChecksum("test", "");
      expect(result).toBe(false);
    });

    it("handles checksum with spaces", () => {
      const content = "SELECT 1;";
      const checksum = calculateChecksum(content);
      const withSpaces = ` ${checksum} `;

      // Should fail - spaces are not trimmed
      const result = verifyChecksum(content, withSpaces);
      expect(result).toBe(false);
    });

    it("handles checksum with mixed case", () => {
      const content = "SELECT 1;";
      const checksum = calculateChecksum(content);

      // Should work with any case
      expect(verifyChecksum(content, checksum.toUpperCase())).toBe(true);
      expect(verifyChecksum(content, checksum.toLowerCase())).toBe(true);

      // Mixed case
      const mixed = checksum.split("").map((c, i) =>
        i % 2 === 0 ? c.toUpperCase() : c.toLowerCase()
      ).join("");
      expect(verifyChecksum(content, mixed)).toBe(true);
    });
  });

  describe("isValidChecksum validation", () => {
    it("validates correct checksums", () => {
      const valid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
      expect(isValidChecksum(valid)).toBe(true);
    });

    it("rejects checksums with invalid characters", () => {
      const invalid = [
        "g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", // 'g' is invalid
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85!", // '!' at end
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85 ", // space at end
      ];

      invalid.forEach(checksum => {
        expect(isValidChecksum(checksum)).toBe(false);
      });
    });

    it("rejects checksums with wrong length", () => {
      const tooShort = "e3b0c44298fc1c149afbf4c8996fb924";
      const tooLong = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85500";

      expect(isValidChecksum(tooShort)).toBe(false);
      expect(isValidChecksum(tooLong)).toBe(false);
    });

    it("accepts uppercase checksums", () => {
      const uppercase = "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855";
      expect(isValidChecksum(uppercase)).toBe(true);
    });
  });

  describe("Performance and memory edge cases", () => {
    it("handles extremely large files without memory issues", () => {
      // Generate 50MB of content (in chunks to avoid string length limits)
      const chunks: string[] = [];
      const chunkSize = 1024 * 1024; // 1MB chunks
      const chunk = "x".repeat(chunkSize);

      for (let i = 0; i < 50; i++) {
        chunks.push(chunk);
      }

      const largeContent = chunks.join("");

      const start = Date.now();
      const checksum = calculateChecksum(largeContent);
      const elapsed = Date.now() - start;

      expect(checksum).toHaveLength(64);
      expect(elapsed).toBeLessThan(5000); // Should complete in under 5 seconds
    });

    it("produces consistent checksums across multiple calls", () => {
      const content = "SELECT * FROM users;";
      const checksums = Array.from({ length: 100 }, () => calculateChecksum(content));

      // All checksums should be identical
      const uniqueChecksums = new Set(checksums);
      expect(uniqueChecksums.size).toBe(1);
    });

    it("handles rapid concurrent checksum calculations", async () => {
      const content = "SELECT 1;";
      const promises = Array.from({ length: 100 }, async () => {
        return calculateChecksum(content);
      });

      const results = await Promise.all(promises);
      const uniqueResults = new Set(results);

      // All should produce same checksum
      expect(uniqueResults.size).toBe(1);
    });
  });

  describe("SQL-specific edge cases", () => {
    it("handles SQL with dollar quotes", () => {
      const sql = `
        CREATE FUNCTION test() RETURNS void AS $$
        BEGIN
          SELECT 1;
        END;
        $$ LANGUAGE plpgsql;
      `;

      const checksum = calculateChecksum(sql);
      expect(checksum).toHaveLength(64);

      // Should be consistent
      const checksum2 = calculateChecksum(sql);
      expect(checksum).toBe(checksum2);
    });

    it("handles SQL with escaped quotes", () => {
      const sql = "SELECT 'It''s a test', \"column\"\"name\"\" FROM test;";
      const checksum = calculateChecksum(sql);

      expect(checksum).toHaveLength(64);
      expect(verifyChecksum(sql, checksum)).toBe(true);
    });

    it("handles COPY data with special characters", () => {
      const sql = `
        COPY users FROM stdin;
        1\tJohn\t\\N
        2\tJane\t\\\\.
        \\.
      `;

      const checksum = calculateChecksum(sql);
      expect(checksum).toHaveLength(64);
    });

    it("differentiates between similar SQL statements", () => {
      const sql1 = "SELECT * FROM users WHERE id = 1";
      const sql2 = "SELECT * FROM users WHERE id = 2";
      const sql3 = "SELECT * FROM users WHERE id=1"; // No spaces

      const checksum1 = calculateChecksum(sql1);
      const checksum2 = calculateChecksum(sql2);
      const checksum3 = calculateChecksum(sql3);

      // All should be different
      expect(checksum1).not.toBe(checksum2);
      expect(checksum1).not.toBe(checksum3);
      expect(checksum2).not.toBe(checksum3);
    });
  });
});