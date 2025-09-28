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

    const runStatement = vi.fn(async (sql: string) => {
      if (sql.startsWith("CREATE TABLE")) {
        const err = new Error("syntax error at or near \"PRIMARY\"");
        (err as any).position = "27"; // points to PRIMARY keyword on line 2
        throw err;
      }
      return undefined;
    });

    const connection = {
      beginTransaction: vi.fn().mockResolvedValue(undefined),
      runStatement,
      markMigrationApplied: vi.fn().mockResolvedValue(undefined),
      commitTransaction: vi.fn().mockResolvedValue(undefined),
      rollbackTransaction: vi.fn().mockResolvedValue(undefined)
    } as any;
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

    await expect((migrator as any).applyUpWithConnection(migration, connection))
      .rejects.toMatchObject({
        constructor: SqlError,
        file: filepath,
        line: 3,
        column: 6
      });
  });
});
