import { describe, it, expect, beforeEach } from "vitest";
import { parseSQL } from "../../src/parser/enhanced-parser";
import { detectHazards, wrapInTransaction, validateHazards, type Hazard } from "../../src/core/hazards";

describe("Transaction & Hazard Detection", () => {
  describe("Hazard Detection", () => {
    describe("CREATE INDEX CONCURRENTLY", () => {
      it("should detect basic CREATE INDEX CONCURRENTLY", () => {
        const sql = "CREATE INDEX CONCURRENTLY idx_users_email ON users(email);";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0]).toMatchObject({
          type: "CREATE_INDEX_CONCURRENTLY",
          line: 1,
          column: 1,
          statement: sql.trim()
        });
      });

      it("should detect with IF NOT EXISTS", () => {
        const sql = "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_test ON table_name(col);";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
      });

      it("should detect UNIQUE INDEX CONCURRENTLY", () => {
        const sql = "CREATE UNIQUE INDEX CONCURRENTLY idx_unique ON users(email);";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
      });

      it("should detect multi-line CREATE INDEX CONCURRENTLY", () => {
        const sql = `
          CREATE INDEX CONCURRENTLY idx_composite
          ON users(first_name, last_name)
          WHERE deleted_at IS NULL;
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
        expect(hazards[0].line).toBe(2);
      });

      it("should NOT detect regular CREATE INDEX", () => {
        const sql = "CREATE INDEX idx_users_email ON users(email);";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(0);
      });

      it("should handle case variations", () => {
        const variations = [
          "create index concurrently idx ON tbl(col);",
          "CREATE index CONCURRENTLY idx ON tbl(col);",
          "CrEaTe InDeX cOnCuRrEnTlY idx ON tbl(col);"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
        });
      });
    });

    describe("DROP INDEX CONCURRENTLY", () => {
      it("should detect DROP INDEX CONCURRENTLY", () => {
        const sql = "DROP INDEX CONCURRENTLY idx_users_email;";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("DROP_INDEX_CONCURRENTLY");
      });

      it("should detect with IF EXISTS", () => {
        const sql = "DROP INDEX CONCURRENTLY IF EXISTS idx_test;";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("DROP_INDEX_CONCURRENTLY");
      });

      it("should detect with CASCADE", () => {
        const sql = "DROP INDEX CONCURRENTLY idx_test CASCADE;";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("DROP_INDEX_CONCURRENTLY");
      });
    });

    describe("REINDEX", () => {
      it("should detect REINDEX variations", () => {
        const variations = [
          "REINDEX TABLE users;",
          "REINDEX INDEX idx_users_email;",
          "REINDEX DATABASE mydb;",
          "REINDEX SCHEMA public;",
          "REINDEX SYSTEM mydb;",
          "REINDEX (VERBOSE) TABLE users;",
          "REINDEX CONCURRENTLY TABLE users;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("REINDEX");
        });
      });
    });

    describe("VACUUM", () => {
      it("should detect VACUUM variations", () => {
        const variations = [
          "VACUUM;",
          "VACUUM FULL;",
          "VACUUM ANALYZE;",
          "VACUUM (FULL, ANALYZE) users;",
          "VACUUM (VERBOSE, SKIP_LOCKED) users;",
          "VACUUM FREEZE users;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("VACUUM");
        });
      });

      it("should NOT detect VACUUM in comments", () => {
        const sql = "-- Need to run VACUUM later\nSELECT 1;";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(0);
      });
    });

    describe("CLUSTER", () => {
      it("should detect CLUSTER variations", () => {
        const variations = [
          "CLUSTER users;",
          "CLUSTER users USING idx_users_email;",
          "CLUSTER VERBOSE users;",
          "CLUSTER;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("CLUSTER");
        });
      });
    });

    describe("REFRESH MATERIALIZED VIEW CONCURRENTLY", () => {
      it("should detect REFRESH MATERIALIZED VIEW CONCURRENTLY", () => {
        const sql = "REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats;";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(1);
        expect(hazards[0].type).toBe("REFRESH_MATERIALIZED_VIEW_CONCURRENTLY");
      });

      it("should detect with WITH DATA/NO DATA", () => {
        const variations = [
          "REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH DATA;",
          "REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH NO DATA;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("REFRESH_MATERIALIZED_VIEW_CONCURRENTLY");
        });
      });

      it("should NOT detect regular REFRESH MATERIALIZED VIEW", () => {
        const sql = "REFRESH MATERIALIZED VIEW user_stats;";
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(0);
      });
    });

    describe("ALTER TYPE", () => {
      it("should detect ALTER TYPE ADD VALUE", () => {
        const variations = [
          "ALTER TYPE user_status ADD VALUE 'banned';",
          "ALTER TYPE user_status ADD VALUE IF NOT EXISTS 'banned';",
          "ALTER TYPE user_status ADD VALUE 'banned' BEFORE 'active';",
          "ALTER TYPE user_status ADD VALUE 'banned' AFTER 'inactive';"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("ALTER_TYPE");
        });
      });

      it("should detect other ALTER TYPE operations", () => {
        const variations = [
          "ALTER TYPE user_status RENAME TO user_state;",
          "ALTER TYPE user_status OWNER TO admin;",
          "ALTER TYPE user_status SET SCHEMA public;",
          "ALTER TYPE point RENAME ATTRIBUTE x TO longitude;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("ALTER_TYPE");
        });
      });
    });

    describe("ALTER SYSTEM", () => {
      it("should detect ALTER SYSTEM", () => {
        const variations = [
          "ALTER SYSTEM SET work_mem = '256MB';",
          "ALTER SYSTEM RESET work_mem;",
          "ALTER SYSTEM RESET ALL;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("ALTER_SYSTEM");
        });
      });
    });

    describe("CREATE/DROP DATABASE", () => {
      it("should detect CREATE DATABASE", () => {
        const variations = [
          "CREATE DATABASE testdb;",
          "CREATE DATABASE testdb WITH OWNER = admin;",
          "CREATE DATABASE testdb TEMPLATE template0;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("CREATE_DATABASE");
        });
      });

      it("should detect DROP DATABASE", () => {
        const variations = [
          "DROP DATABASE testdb;",
          "DROP DATABASE IF EXISTS testdb;",
          "DROP DATABASE testdb WITH (FORCE);"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toBe("DROP_DATABASE");
        });
      });
    });

    describe("CREATE/DROP TABLESPACE", () => {
      it("should detect tablespace operations", () => {
        const variations = [
          "CREATE TABLESPACE fastspace LOCATION '/ssd/postgresql';",
          "DROP TABLESPACE fastspace;",
          "ALTER TABLESPACE fastspace RENAME TO ssdspace;"
        ];

        variations.forEach(sql => {
          const hazards = detectHazards(sql);
          expect(hazards).toHaveLength(1);
          expect(hazards[0].type).toMatch(/TABLESPACE/);
        });
      });
    });

    describe("Multiple Hazards", () => {
      it("should detect multiple hazards in one migration", () => {
        const sql = `
          CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
          VACUUM ANALYZE users;
          REINDEX TABLE users;
          ALTER TYPE user_status ADD VALUE 'pending';
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(4);
        const types = hazards.map(h => h.type);
        expect(types).toContain("CREATE_INDEX_CONCURRENTLY");
        expect(types).toContain("VACUUM");
        expect(types).toContain("REINDEX");
        expect(types).toContain("ALTER_TYPE");
      });

      it("should track line numbers correctly for multiple hazards", () => {
        const sql = `
          SELECT 1;
          CREATE INDEX CONCURRENTLY idx1 ON t1(c1);
          SELECT 2;
          VACUUM t1;
          SELECT 3;
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(2);
        expect(hazards[0].line).toBe(3);
        expect(hazards[1].line).toBe(5);
      });
    });

    describe("Edge Cases", () => {
      it("should handle hazards in strings (should NOT detect)", () => {
        const sql = `
          INSERT INTO logs (message) VALUES ('Need to run VACUUM later');
          UPDATE config SET value = 'CREATE INDEX CONCURRENTLY' WHERE key = 'todo';
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(0);
      });

      it("should handle hazards in comments (should NOT detect)", () => {
        const sql = `
          -- TODO: CREATE INDEX CONCURRENTLY
          /* VACUUM FULL might be needed */
          SELECT 1; -- REINDEX later
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(0);
      });

      it("should handle hazards in dollar quotes (should NOT detect)", () => {
        const sql = `
          CREATE FUNCTION test() RETURNS void AS $$
          BEGIN
            RAISE NOTICE 'Need to VACUUM';
            -- This is not a real CREATE INDEX CONCURRENTLY
          END;
          $$ LANGUAGE plpgsql;
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(0);
      });

      it("should handle hazards with extra whitespace", () => {
        const sql = `
          CREATE    INDEX    CONCURRENTLY    idx_test    ON    users(email);
          VACUUM     (   FULL   )    users;
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(2);
      });

      it("should handle semicolon-less statements", () => {
        const sql = `
          CREATE INDEX CONCURRENTLY idx_test ON users(email)
          VACUUM users
        `;
        const hazards = detectHazards(sql);

        expect(hazards).toHaveLength(2);
      });
    });
  });

  describe("Transaction Wrapping", () => {
    it("should wrap SQL in transaction by default", () => {
      const sql = "CREATE TABLE users (id INT);";
      const wrapped = wrapInTransaction(sql);

      expect(wrapped).toBe(
        "BEGIN;\n" +
        "CREATE TABLE users (id INT);\n" +
        "COMMIT;"
      );
    });

    it("should handle multiple statements", () => {
      const sql = `CREATE TABLE users (id INT);
CREATE TABLE posts (id INT);`;
      const wrapped = wrapInTransaction(sql);

      expect(wrapped).toBe(
        "BEGIN;\n" +
        "CREATE TABLE users (id INT);\n" +
        "CREATE TABLE posts (id INT);\n" +
        "COMMIT;"
      );
    });

    it("should not double-wrap if transaction exists", () => {
      const sql = `
        BEGIN;
        CREATE TABLE users (id INT);
        COMMIT;
      `;
      const wrapped = wrapInTransaction(sql.trim());

      expect(wrapped).toBe(sql.trim());
    });

    it("should not wrap if notx directive is present", () => {
      const sql = "CREATE INDEX CONCURRENTLY idx_test ON users(email);";
      const wrapped = wrapInTransaction(sql, true);

      expect(wrapped).toBe(sql);
    });

    it("should detect existing transaction patterns", () => {
      const variations = [
        "BEGIN; SELECT 1; COMMIT;",
        "BEGIN TRANSACTION; SELECT 1; COMMIT;",
        "START TRANSACTION; SELECT 1; COMMIT;",
        "begin; select 1; commit;",
        "BEGIN WORK; SELECT 1; COMMIT WORK;"
      ];

      variations.forEach(sql => {
        const wrapped = wrapInTransaction(sql);
        expect(wrapped).toBe(sql);
      });
    });
  });

  describe("Parser Integration with notx", () => {
    it("should parse notx directive", () => {
      const sql = `
        -- +nomad Up
        -- +nomad notx
        CREATE INDEX CONCURRENTLY idx_test ON users(email);

        -- +nomad Down
        DROP INDEX idx_test;
      `;

      const parsed = parseSQL(sql);
      expect(parsed.up.notx).toBe(true);
      expect(parsed.down.notx).toBe(false);
    });

    it("should handle NO TRANSACTION alias", () => {
      const sql = `
        -- +nomad Up
        -- +nomad NO TRANSACTION
        CREATE INDEX CONCURRENTLY idx_test ON users(email);
      `;

      const parsed = parseSQL(sql);
      expect(parsed.up.notx).toBe(true);
    });

    it("should handle case variations of notx", () => {
      const variations = [
        "-- +nomad notx",
        "-- +nomad NoTx",
        "-- +nomad NOTX",
        "-- +nomad no transaction",
        "-- +nomad NO TRANSACTION",
        "-- +nomad No Transaction"
      ];

      variations.forEach(directive => {
        const sql = `
          -- +nomad Up
          ${directive}
          CREATE INDEX CONCURRENTLY idx_test ON users(email);
        `;

        const parsed = parseSQL(sql);
        expect(parsed.up.notx).toBe(true);
      });
    });

    it("should track notx per direction", () => {
      const sql = `
        -- +nomad Up
        -- +nomad notx
        CREATE INDEX CONCURRENTLY idx_up ON users(email);

        -- +nomad Down
        CREATE TABLE test (id INT);
      `;

      const parsed = parseSQL(sql);
      expect(parsed.up.notx).toBe(true);
      expect(parsed.down.notx).toBe(false);
    });
  });

  describe("Hazard Validation", () => {
    it("should throw error when hazard detected without notx", () => {
      const sql = "CREATE INDEX CONCURRENTLY idx_test ON users(email);";
      const hazards = detectHazards(sql);

      expect(() => {
        validateHazards(hazards, false);
      }).toThrow(/Hazardous operation detected/);
    });

    it("should not throw when hazard detected with notx", () => {
      const sql = "CREATE INDEX CONCURRENTLY idx_test ON users(email);";
      const hazards = detectHazards(sql);

      expect(() => {
        validateHazards(hazards, true);
      }).not.toThrow();
    });

    it("should not throw when no hazards", () => {
      const sql = "CREATE TABLE users (id INT);";
      const hazards = detectHazards(sql);

      expect(() => {
        validateHazards(hazards, false);
      }).not.toThrow();
    });

    it("should list all hazards in error message", () => {
      const sql = `
        CREATE INDEX CONCURRENTLY idx1 ON t1(c1);
        VACUUM t1;
        REINDEX TABLE t1;
      `;
      const hazards = detectHazards(sql);

      expect(() => {
        validateHazards(hazards, false);
      }).toThrow(/Hazardous operation detected/);

      try {
        validateHazards(hazards, false);
      } catch (e: any) {
        expect(e.message).toContain("CREATE_INDEX_CONCURRENTLY");
        expect(e.message).toContain("VACUUM");
        expect(e.message).toContain("REINDEX");
      }
    });

    it("should suggest using notx directive in error", () => {
      const sql = "CREATE INDEX CONCURRENTLY idx_test ON users(email);";
      const hazards = detectHazards(sql);

      expect(() => {
        validateHazards(hazards, false);
      }).toThrow(/Use.*-- \+nomad notx/);
    });
  });

  describe("Auto-notx Mode", () => {
    it("should allow hazards with auto-notx enabled", () => {
      const sql = "CREATE INDEX CONCURRENTLY idx_test ON users(email);";
      const hazards = detectHazards(sql);

      const result = validateHazards(hazards, false, { autoNotx: true });
      expect(result.shouldSkipTransaction).toBe(true);
      expect(result.hazardsDetected).toHaveLength(1);
    });

    it("should respect explicit notx over auto-notx", () => {
      const sql = "CREATE TABLE users (id INT);";
      const hazards = detectHazards(sql);

      const result = validateHazards(hazards, true, { autoNotx: false });
      expect(result.shouldSkipTransaction).toBe(true);
      expect(result.hazardsDetected).toHaveLength(0);
    });

    it("should log warning when auto-notx is used", () => {
      const sql = "VACUUM users;";
      const hazards = detectHazards(sql);

      const logs: string[] = [];
      const result = validateHazards(hazards, false, {
        autoNotx: true,
        logger: (msg) => logs.push(msg)
      });

      expect(result.shouldSkipTransaction).toBe(true);
      expect(logs.length).toBe(1);
      expect(logs[0]).toContain("Auto-notx");
      expect(logs[0]).toContain("VACUUM");
    });
  });

  describe("Complex Scenarios", () => {
    it("should handle migration with mixed operations", () => {
      const sql = `
        -- First, create the table (safe in transaction)
        CREATE TABLE user_stats (
          user_id INT,
          score INT
        );

        -- Then create index concurrently (needs notx)
        CREATE INDEX CONCURRENTLY idx_user_stats ON user_stats(user_id);
      `;

      const hazards = detectHazards(sql);
      expect(hazards).toHaveLength(1);
      expect(hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
    });

    it("should handle PL/pgSQL functions with hazard-like text", () => {
      const sql = `
        CREATE OR REPLACE FUNCTION maintenance_job() RETURNS void AS $$
        DECLARE
          msg TEXT := 'Need to VACUUM and REINDEX';
        BEGIN
          RAISE NOTICE 'CREATE INDEX CONCURRENTLY not needed';
          -- This function doesn't actually run these commands
        END;
        $$ LANGUAGE plpgsql;
      `;

      const hazards = detectHazards(sql);
      expect(hazards).toHaveLength(0);
    });

    it("should handle COPY statements with hazard-like content", () => {
      const sql = `
        COPY maintenance_log (message) FROM stdin;
        Run VACUUM FULL tonight
        CREATE INDEX CONCURRENTLY tomorrow
        REINDEX all tables weekly
        \\.
      `;

      const hazards = detectHazards(sql);
      expect(hazards).toHaveLength(0);
    });

    it("should detect real hazards after COPY block", () => {
      const sql = `
        COPY users (name) FROM stdin;
        John Doe
        \\.

        CREATE INDEX CONCURRENTLY idx_users_name ON users(name);
      `;

      const hazards = detectHazards(sql);
      expect(hazards).toHaveLength(1);
      expect(hazards[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
    });

    it("should handle escaped quotes with hazard-like content", () => {
      const sql = `
        INSERT INTO config (key, value) VALUES
          ('maintenance', E'Run VACUUM\\nThen REINDEX'),
          ('todo', 'CREATE INDEX CONCURRENTLY');
      `;

      const hazards = detectHazards(sql);
      expect(hazards).toHaveLength(0);
    });
  });
});