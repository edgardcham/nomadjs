import { describe, expect, it } from "vitest";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseNomadSql } from "../../src/parser/nomadSql.js";

describe("parseNomadSql", () => {
  it("parses directives and statements", () => {
    const dir = mkdtempSync(join(tmpdir(), "nomad-parser-"));
    try {
      const file = join(dir, "20250101010101_example.sql");
      writeFileSync(
        file,
        `-- comment before\n-- +nomad Up\nCREATE TABLE test (id SERIAL);\n-- +nomad StatementBegin\nINSERT INTO test(id) VALUES (1);\nINSERT INTO test(id) VALUES (2);\n-- +nomad StatementEnd\n-- +nomad NO TRANSACTION\n-- +nomad Down\nDROP TABLE test;\n`
      );

      const migration = parseNomadSql(file, 20250101010101);

      expect(migration.version).toBe(20250101010101);
      expect(migration.noTransaction).toBe(true);
      expect(migration.up.statements).toHaveLength(2);
      expect(migration.up.statements[0]).toContain("CREATE TABLE test");
      // block should be merged as a single statement
      expect(migration.up.statements[1]).toMatch(/INSERT INTO test/);
      expect(migration.down.statements).toEqual(["DROP TABLE test;"]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
