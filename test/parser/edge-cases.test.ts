import { describe, it, expect } from "vitest";
import { splitSqlStatements, parseNomadSql } from "../../src/parser/enhanced-parser.js";

describe("Parser Edge Cases - Trying to Break It", () => {
  describe("Malformed inputs", () => {
    it("handles empty string", () => {
      const statements = splitSqlStatements("");
      expect(statements).toEqual([]);
    });

    it("handles only whitespace", () => {
      const statements = splitSqlStatements("   \n\t  \r\n  ");
      expect(statements).toEqual([]);
    });

    it("handles only semicolons", () => {
      const statements = splitSqlStatements(";;;");
      expect(statements).toEqual([]);
    });

    it("handles only comments", () => {
      const sql = `
        -- comment 1
        /* comment 2 */
        -- comment 3
      `;
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual([]);
    });
  });

  describe("Unclosed quotes and comments", () => {
    it("handles unclosed single quote", () => {
      const sql = "SELECT 'unclosed";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 'unclosed"]);
    });

    it("handles unclosed double quote", () => {
      const sql = 'SELECT "unclosed';
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(['SELECT "unclosed']);
    });

    it("handles unclosed block comment", () => {
      const sql = "SELECT 1; /* unclosed comment";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 1"]);
    });

    it("handles unclosed dollar quote", () => {
      const sql = "SELECT $$unclosed";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT $$unclosed"]);
    });
  });

  describe("Nested and mixed quotes", () => {
    it("handles single quotes inside double quotes", () => {
      const sql = `SELECT "column's name" FROM users`;
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual([`SELECT "column's name" FROM users`]);
    });

    it("handles double quotes inside single quotes", () => {
      const sql = `SELECT 'He said "hello"' FROM users`;
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual([`SELECT 'He said "hello"' FROM users`]);
    });

    it("handles quotes inside dollar quotes", () => {
      const sql = `SELECT $$It's "complex" ; stuff$$ FROM users`;
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual([`SELECT $$It's "complex" ; stuff$$ FROM users`]);
    });

    it("handles dollar quotes with same tag appearing in content", () => {
      const sql = `CREATE FUNCTION test() RETURNS text AS $tag$
        SELECT '$tag$ is not the end';
      $tag$ LANGUAGE sql`;
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain("$tag$");
    });
  });

  describe("Special PostgreSQL syntax", () => {
    it("handles COPY with CSV format", () => {
      const sql = `
        COPY users (id, name) FROM stdin WITH CSV;
        1,"John; Doe"
        2,"Jane, Smith"
        \\.
        SELECT 1;
      `.trim();
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("COPY users");
      expect(statements[0]).toContain("\\.");
      expect(statements[1]).toBe("SELECT 1");
    });

    it("handles DO blocks", () => {
      const sql = `
        DO $$
        DECLARE
          r record;
        BEGIN
          FOR r IN SELECT * FROM users LOOP
            RAISE NOTICE 'User: %', r.name;
          END LOOP;
        END $$;
        SELECT 1;
      `.trim();
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("DO $$");
      expect(statements[1]).toBe("SELECT 1");
    });

    it("handles multiple dollar quote tags in one statement", () => {
      const sql = `
        CREATE FUNCTION complex() RETURNS void AS $outer$
        BEGIN
          EXECUTE $inner$ SELECT 1 $inner$;
          RAISE NOTICE $msg$ Hello World $msg$;
        END;
        $outer$ LANGUAGE plpgsql;
      `.trim();
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain("$outer$");
      expect(statements[0]).toContain("$inner$");
      expect(statements[0]).toContain("$msg$");
    });
  });

  describe("Unicode and special characters", () => {
    it("handles Unicode characters", () => {
      const sql = "SELECT '你好世界' FROM users; SELECT '🎉' FROM posts";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("你好世界");
      expect(statements[1]).toContain("🎉");
    });

    it("handles null bytes stripped", () => {
      const sql = "SELECT 1;\0SELECT 2";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
    });

    it("handles mixed line endings", () => {
      const sql = "SELECT 1;\rSELECT 2;\nSELECT 3;\r\nSELECT 4";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(4);
    });

    it("handles tabs and special whitespace", () => {
      const sql = "SELECT\t1;\n\tSELECT  2";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toBe("SELECT\t1");
      expect(statements[1]).toBe("SELECT  2");
    });
  });

  describe("parseNomadSql edge cases", () => {
    it("handles missing up/down sections", () => {
      const sql = `
        -- +nomad notx
        -- Just a comment, no actual sections
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.up.statements).toEqual([]);
      expect(result.down.statements).toEqual([]);
      expect(result.noTransaction).toBe(true);
    });

    it("handles multiple up/down sections (uses last)", () => {
      const sql = `
        -- +nomad up
        SELECT 1;
        -- +nomad up
        SELECT 2;
        -- +nomad down
        DELETE FROM users;
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.up.statements).toEqual(["SELECT 2"]);
      expect(result.down.statements).toEqual(["DELETE FROM users"]);
    });

    it("handles directives with extra spaces", () => {
      const sql = `
        --   +nomad   notx
        --  +nomad    tags:   test  ,  prod  ,  staging
        -- +nomad   up
        SELECT 1;
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.noTransaction).toBe(true);
      expect(result.tags).toEqual(["test", "prod", "staging"]);
    });

    it("handles block without endblock at EOF", () => {
      const sql = `
        -- +nomad up
        -- +nomad block
        SELECT 1;
        SELECT 2;
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.up.statements).toHaveLength(1);
      expect(result.up.statements[0]).toContain("SELECT 1");
      expect(result.up.statements[0]).toContain("SELECT 2");
    });

    it("handles nested block attempts (not supported, treats as content)", () => {
      const sql = `
        -- +nomad up
        -- +nomad block
        SELECT 1;
        -- +nomad block
        SELECT 2;
        -- +nomad endblock
        SELECT 3;
        -- +nomad endblock
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.up.statements).toHaveLength(1);
      // The inner block/endblock should be treated as content
      expect(result.up.statements[0]).toContain("-- +nomad block");
    });

    it("handles SQL injection attempts in comments", () => {
      const sql = `
        -- +nomad up'; DROP TABLE users; --
        SELECT 1;
        -- +nomad down
        SELECT 2;
      `;
      const result = parseNomadSql(sql, "test.sql");
      // Should not execute the injection, just treat first line as invalid directive
      expect(result.up.statements).toEqual([]);
      expect(result.down.statements).toEqual(["SELECT 2"]);
    });

    it("handles extremely long lines", () => {
      const longString = "x".repeat(10000);
      const sql = `
        -- +nomad up
        SELECT '${longString}';
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.up.statements).toHaveLength(1);
      expect(result.up.statements[0]).toContain(longString);
    });

    it("handles case variations in directives", () => {
      const sql = `
        -- +NOMAD UP
        SELECT 1;
        -- +Nomad Down
        SELECT 2;
        -- +NoMaD noTX
      `;
      const result = parseNomadSql(sql, "test.sql");
      expect(result.up.statements).toEqual(["SELECT 1"]);
      expect(result.down.statements).toEqual(["SELECT 2"]);
      expect(result.noTransaction).toBe(true);
    });
  });

  describe("Performance edge cases", () => {
    it("handles many small statements efficiently", () => {
      const statements = [];
      for (let i = 0; i < 1000; i++) {
        statements.push(`SELECT ${i}`);
      }
      const sql = statements.join("; ");

      const start = Date.now();
      const parsed = splitSqlStatements(sql);
      const elapsed = Date.now() - start;

      expect(parsed).toHaveLength(1000);
      expect(elapsed).toBeLessThan(100); // Should parse in under 100ms
    });

    it("handles deeply nested dollar quotes", () => {
      let sql = "SELECT ";
      for (let i = 0; i < 10; i++) {
        sql += `$tag${i}$ nested `;
      }
      for (let i = 9; i >= 0; i--) {
        sql += `$tag${i}$`;
      }

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain("$tag0$");
      expect(statements[0]).toContain("$tag9$");
    });
  });

  describe("Real-world problematic SQL", () => {
    it("handles PostgreSQL E-strings (basic support)", () => {
      // Note: Full E-string support would require understanding backslash escapes
      // For now, we treat E'...' similar to regular strings
      const sql = "SELECT E'test\\nstring' FROM test";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toBe("SELECT E'test\\nstring' FROM test");
    });

    it("handles JSON operators", () => {
      const sql = "SELECT data->>'name' FROM users; SELECT data#>'{address,city}' FROM users";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
    });

    it("handles array syntax", () => {
      const sql = "SELECT ARRAY[1,2,3]; SELECT '{1,2,3}'::int[]";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
    });

    it("handles regex patterns", () => {
      const sql = "SELECT * FROM users WHERE email ~ '^[^;]+@[^;]+\\.[^;]+$'";
      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
    });
  });
});