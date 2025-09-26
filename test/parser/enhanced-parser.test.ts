import { describe, it, expect } from "vitest";
import { splitSqlStatements, parseNomadSql } from "../../src/parser/enhanced-parser.js";

describe("splitSqlStatements", () => {
  describe("basic statement splitting", () => {
    it("splits simple statements on semicolons", () => {
      const sql = "SELECT 1; SELECT 2; SELECT 3";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 1", "SELECT 2", "SELECT 3"]);
    });

    it("preserves statements without trailing semicolon", () => {
      const sql = "SELECT 1";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 1"]);
    });

    it("ignores semicolons in single quotes", () => {
      const sql = "SELECT 'test;semicolon' FROM users";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 'test;semicolon' FROM users"]);
    });

    it("ignores semicolons in double quotes", () => {
      const sql = 'SELECT "col;name" FROM users';
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(['SELECT "col;name" FROM users']);
    });
  });

  describe("dollar quotes", () => {
    it("handles simple dollar quotes", () => {
      const sql = `
        CREATE FUNCTION test() RETURNS void AS $$
        BEGIN
          SELECT 1;
          SELECT 2;
        END;
        $$ LANGUAGE plpgsql;
        SELECT 3;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("CREATE FUNCTION");
      expect(statements[0]).toContain("$$");
      expect(statements[1]).toBe("SELECT 3");
    });

    it("handles tagged dollar quotes", () => {
      const sql = `
        CREATE FUNCTION test() RETURNS void AS $func$
        BEGIN
          SELECT 1;
        END;
        $func$ LANGUAGE plpgsql;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain("$func$");
    });

    it("handles nested dollar quotes with different tags", () => {
      const sql = `
        CREATE FUNCTION outer() RETURNS void AS $outer$
        BEGIN
          EXECUTE $inner$ SELECT 1; SELECT 2; $inner$;
        END;
        $outer$ LANGUAGE plpgsql;
        SELECT 3;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("$outer$");
      expect(statements[0]).toContain("$inner$");
      expect(statements[1]).toBe("SELECT 3");
    });

    it("handles dollar quotes with numbers in tags", () => {
      const sql = `
        CREATE FUNCTION test() RETURNS void AS $tag123$
        BEGIN
          SELECT 1;
        END;
        $tag123$ LANGUAGE plpgsql;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0]).toContain("$tag123$");
    });
  });

  describe("comments", () => {
    it("ignores semicolons in line comments", () => {
      const sql = `
        SELECT 1; -- comment with ; semicolon
        SELECT 2;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("SELECT 1");
      expect(statements[1]).toBe("SELECT 2");
    });

    it("ignores semicolons in block comments", () => {
      const sql = `
        SELECT 1; /* comment with ; semicolon */
        SELECT 2;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
    });

    it("handles nested block comments", () => {
      const sql = `
        SELECT 1; /* outer /* inner ; */ comment */
        SELECT 2;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
    });
  });

  describe("CRLF and BOM handling", () => {
    it("normalizes CRLF to LF", () => {
      const sql = "SELECT 1;\r\nSELECT 2;\r\nSELECT 3";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 1", "SELECT 2", "SELECT 3"]);
    });

    it("strips BOM from beginning", () => {
      const sql = "\uFEFFSELECT 1; SELECT 2";
      const statements = splitSqlStatements(sql);
      expect(statements).toEqual(["SELECT 1", "SELECT 2"]);
    });
  });

  describe("complex real-world examples", () => {
    it("handles COPY statement", () => {
      const sql = `
        COPY users (id, name, email) FROM stdin;
        1	John	john@example.com
        2	Jane	jane@example.com
        \\.
        SELECT COUNT(*) FROM users;
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("COPY users");
      expect(statements[0]).toContain("\\.");
      expect(statements[1]).toContain("COUNT(*)");
    });

    it("handles complex PL/pgSQL function", () => {
      const sql = `
        CREATE OR REPLACE FUNCTION update_modified_column()
        RETURNS TRIGGER AS $$
        BEGIN
          NEW.modified = now();
          RETURN NEW;
        END;
        $$ language 'plpgsql';

        CREATE TRIGGER update_users_modtime
        BEFORE UPDATE ON users
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
      `.trim();

      const statements = splitSqlStatements(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0]).toContain("CREATE OR REPLACE FUNCTION");
      expect(statements[1]).toContain("CREATE TRIGGER");
    });
  });
});

describe("parseNomadSql with blocks", () => {
  it("handles block directive", () => {
    const sql = `
      -- +nomad up
      SELECT 1;

      -- +nomad block
      COPY users FROM stdin;
      1	John
      2	Jane
      \\.
      -- +nomad endblock

      SELECT 2;

      -- +nomad down
      DELETE FROM users;
    `.trim();

    const result = parseNomadSql(sql, "test.sql");
    expect(result.up.statements).toHaveLength(3);
    expect(result.up.statements[0]).toBe("SELECT 1");
    expect(result.up.statements[1]).toContain("COPY users FROM stdin");
    expect(result.up.statements[1]).toContain("\\.");
    expect(result.up.statements[2]).toBe("SELECT 2");
    expect(result.down.statements).toHaveLength(1);
  });

  it("handles notx directive", () => {
    const sql = `
      -- +nomad notx
      -- +nomad up
      CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
      -- +nomad down
      DROP INDEX idx_users_email;
    `.trim();

    const result = parseNomadSql(sql, "test.sql");
    expect(result.noTransaction).toBe(true);
  });

  it("handles tags directive", () => {
    const sql = `
      -- +nomad tags: test, seed, development
      -- +nomad up
      INSERT INTO users VALUES (1, 'Test User');
      -- +nomad down
      DELETE FROM users WHERE id = 1;
    `.trim();

    const result = parseNomadSql(sql, "test.sql");
    expect(result.tags).toEqual(["test", "seed", "development"]);
  });

  it("parses complex migration with all features", () => {
    const sql = `
      -- +nomad tags: core, critical
      -- +nomad notx

      -- +nomad up

      -- Create function with dollar quotes
      CREATE OR REPLACE FUNCTION audit_trigger() RETURNS TRIGGER AS $audit$
      BEGIN
        INSERT INTO audit_log (table_name, action) VALUES (TG_TABLE_NAME, TG_OP);
        RETURN NEW;
      END;
      $audit$ LANGUAGE plpgsql;

      -- Create index concurrently (requires notx)
      CREATE INDEX CONCURRENTLY idx_users_active ON users(active);

      -- +nomad block
      -- Complex multi-line insert
      INSERT INTO settings (key, value, description) VALUES
        ('feature.enabled', 'true', 'Enable new feature; with semicolon'),
        ('feature.timeout', '30', 'Timeout in seconds');
      -- +nomad endblock

      -- +nomad down

      DROP INDEX IF EXISTS idx_users_active;
      DROP FUNCTION IF EXISTS audit_trigger();
      DELETE FROM settings WHERE key LIKE 'feature.%';
    `.trim();

    const result = parseNomadSql(sql, "test.sql");

    expect(result.noTransaction).toBe(true);
    expect(result.tags).toEqual(["core", "critical"]);
    expect(result.up.statements).toHaveLength(3);
    expect(result.up.statements[0]).toContain("CREATE OR REPLACE FUNCTION");
    expect(result.up.statements[1]).toContain("CREATE INDEX CONCURRENTLY");
    expect(result.up.statements[2]).toContain("INSERT INTO settings");
    expect(result.down.statements).toHaveLength(3);
  });

  it("records statement metadata with line and column information", () => {
    const sql = `-- +nomad Up
SELECT 1;

INSERT INTO users VALUES (1);
-- +nomad Down
DELETE FROM users WHERE id = 1;`;

    const result = parseNomadSql(sql, "migrations/20240101120000_test.sql");

    expect(result.up.statementMeta).toEqual([
      expect.objectContaining({ line: 2, column: 1 }),
      expect.objectContaining({ line: 4, column: 1 })
    ]);

    expect(result.down.statementMeta).toEqual([
      expect.objectContaining({ line: 6, column: 1 })
    ]);
  });

  it("ignores leading comments when mapping statement positions", () => {
    const sql = `-- +nomad Up
-- This comment references UPDATE foo SET bar = 'baz';
UPDATE foo SET bar = 'baz';
UPDATE foo SET bar = 'baz';
-- +nomad Down
-- Another comment with UPDATE foo SET bar = 'baz';
UPDATE foo SET bar = 'baz';`;

    const result = parseNomadSql(sql, "test.sql");

    expect(result.up.statementMeta).toEqual([
      expect.objectContaining({ line: 3, column: 1 }),
      expect.objectContaining({ line: 4, column: 1 })
    ]);

    expect(result.down.statementMeta).toEqual([
      expect.objectContaining({ line: 7, column: 1 })
    ]);
  });

  it("preserves metadata for block statements with CRLF line endings", () => {
    const sql = `-- +nomad Up\r
-- +nomad block\r
  INSERT INTO things VALUES (1);\r
  INSERT INTO things VALUES (2);\r
\\.\r
-- +nomad endblock\r
-- +nomad Down\r
DELETE FROM things;\r\n`;

    const result = parseNomadSql(sql, "test.sql");

    expect(result.up.statements).toHaveLength(1);
    expect(result.up.statementMeta).toEqual([
      expect.objectContaining({ line: 3, column: 3 })
    ]);
    expect(result.down.statementMeta).toEqual([
      expect.objectContaining({ line: 8, column: 1 })
    ]);
  });
});
