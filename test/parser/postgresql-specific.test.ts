import { describe, it, expect } from "vitest";
import { splitSqlStatements } from "../../src/parser/enhanced-parser.js";

describe("PostgreSQL-specific syntax", () => {
  describe("E-strings (escape strings)", () => {
    it("handles basic E-strings", () => {
      const sql = "SELECT E'Hello\\nWorld' FROM test";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT E'Hello\\nWorld' FROM test"]);
    });

    it("handles E-strings with escaped quotes", () => {
      const sql = "SELECT E'It\\'s a test' FROM test";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT E'It\\'s a test' FROM test"]);
    });

    it("handles E-strings with semicolons", () => {
      const sql = "SELECT E'Line 1;\\nLine 2' FROM test; SELECT 2";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toBe("SELECT E'Line 1;\\nLine 2' FROM test");
      expect(statements[1]).toBe("SELECT 2");
    });

    it("handles E-strings with backslash at end", () => {
      const sql = "SELECT E'test\\\\' FROM test"; // \\ represents single backslash
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT E'test\\\\' FROM test"]);
    });

    it("handles E-strings with escaped backslash before quote", () => {
      const sql = "SELECT E'test\\\\\\'more' FROM test"; // \\' is backslash then escaped quote
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT E'test\\\\\\'more' FROM test"]);
    });

    it("handles multiline E-strings", () => {
      const sql = `SELECT E'Line 1\\n\
Line 2\\n\
Line 3' FROM test`;
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain("Line 1");
      expect(statements[0]).toContain("Line 3");
    });

    it("handles E-strings followed by regular strings", () => {
      const sql = "SELECT E'\\n', 'regular;string' FROM test";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT E'\\n', 'regular;string' FROM test"]);
    });

    it("handles complex SQL with E-strings", () => {
      const sql = `
        INSERT INTO logs (message) VALUES (E'Error:\\nFile not found');
        UPDATE users SET bio = E'John\\'s bio:\\n- Developer\\n- Writer' WHERE id = 1;
        SELECT E'Test;\\nComplete' FROM dual;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(3);
      expect(statements[0]).toContain("E'Error:\\nFile not found'");
      expect(statements[1]).toContain("E'John\\'s bio:");
      expect(statements[2]).toContain("E'Test;\\nComplete'");
    });

    it("handles E-string that could be SQL injection", () => {
      const sql = "SELECT E'\\'; DROP TABLE users; --' FROM test";
      const statements = splitSqlStatements(sql);
      // Should treat entire thing as one string
      expect(statements).toHaveLength(1);
      expect(statements[0]).toBe("SELECT E'\\'; DROP TABLE users; --' FROM test");
    });
  });

  describe("Other PostgreSQL string types", () => {
    it("handles U& strings (Unicode)", () => {
      const sql = "SELECT U&'\\0441\\043B\\043E\\043D' FROM test";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT U&'\\0441\\043B\\043E\\043D' FROM test"]);
    });

    it("handles B strings (bit strings)", () => {
      const sql = "SELECT B'101010' FROM test; SELECT 2";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toBe("SELECT B'101010' FROM test");
    });

    it("handles X strings (hex)", () => {
      const sql = "SELECT X'1a2b3c' FROM test";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT X'1a2b3c' FROM test"]);
    });
  });
});