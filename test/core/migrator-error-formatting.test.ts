import { describe, it, expect, vi } from "vitest";
import { Migrator, type MigrationFile } from "../../src/core/migrator.js";
import { SqlError } from "../../src/core/errors.js";
import { parseNomadSql } from "../../src/parser/enhanced-parser.js";
import type { Config } from "../../src/config.js";

vi.mock("pg", () => ({
  Pool: vi.fn()
}));

describe("Migrator SQL error formatting", () => {
  it("attaches file, line, and column to SqlError when a statement fails", async () => {
    const filepath = "/migrations/20240101120000_create_users.sql";
    const content = `-- +nomad Up\nCREATE TABLE users (\n  id INT PRIMARY KEY\n);\n\n-- +nomad Down\nDROP TABLE users;`;
    const parsed = parseNomadSql(content, filepath);

    const migration: MigrationFile = {
      version: BigInt("20240101120000"),
      name: "create_users",
      filepath,
      content,
      checksum: "dummy",
      parsed
    };

    const queryMock = vi.fn(async (sql: string) => {
      if (sql === "BEGIN" || sql === "ROLLBACK" || sql === "COMMIT") {
        return { rows: [] } as any;
      }

      if (sql.startsWith("CREATE TABLE")) {
        const err = new Error("syntax error at or near \"PRIMARY\"");
        (err as any).position = "27"; // points to PRIMARY keyword on line 2
        throw err;
      }

      return { rows: [] } as any;
    });

    const client = { query: queryMock } as any;
    const config: Config = {
      driver: "postgres",
      url: "postgresql://localhost/test",
      dir: "./migrations",
      table: "nomad_migrations",
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    } as any;

    const migrator = new Migrator(config, { query: vi.fn(), connect: vi.fn(), end: vi.fn() } as any);

    await expect((migrator as any).applyUpWithClient(migration, client))
      .rejects.toMatchObject({
        constructor: SqlError,
        file: filepath,
        line: 3,
        column: 6
      });
  });
});
