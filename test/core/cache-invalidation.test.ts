import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import type { Config } from "../../src/config.js";
import { Pool } from "pg";
import { statSync, readFileSync } from "node:fs";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";

vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");

describe("Migration file caching", () => {
  const config: Config = {
    driver: "postgres",
    url: "postgres://localhost/test",
    dir: "./migrations",
    table: "nomad_migrations"
  } as any;

  let migrator: Migrator;
  let mockPool: any;

  beforeEach(() => {
    vi.resetAllMocks();
    process.env.NODE_ENV = undefined;

    mockPool = {
      query: vi.fn(),
      connect: vi.fn().mockResolvedValue({ query: vi.fn(), release: vi.fn() })
    };

    (listMigrationFiles as unknown as vi.Mock).mockReturnValue(["./migrations/20240101010101_init.sql"]);
    (filenameToVersion as unknown as vi.Mock).mockReturnValue("20240101010101");

    migrator = new Migrator(config, mockPool as unknown as Pool);
  });

  afterEach(() => {
    delete process.env.NOMAD_CACHE_HASH_GUARD;
    delete process.env.NODE_ENV;
  });

  it("reuses cache when mtime and size match", async () => {
    const stats = { mtimeMs: 1000, size: 100 } as any;
    (statSync as unknown as vi.Mock).mockReturnValue(stats);
    (readFileSync as unknown as vi.Mock).mockReturnValueOnce("-- +nomad Up\nSELECT 1;\n");
    (calculateChecksum as unknown as vi.Mock).mockReturnValue("checksum1");
    (parseNomadSqlFile as unknown as vi.Mock).mockReturnValue({
      up: { statements: ["SELECT 1;"], statementMeta: [], notx: false },
      down: { statements: [], statementMeta: [], notx: false },
      tags: []
    });

    await migrator.loadMigrationFiles();

    (readFileSync as unknown as vi.Mock).mockClear();
    (calculateChecksum as unknown as vi.Mock).mockReturnValue("checksum1");
    (statSync as unknown as vi.Mock).mockReturnValue(stats);

    await migrator.loadMigrationFiles();

    expect(readFileSync).not.toHaveBeenCalled();
  });

  it("reparses when size changes but mtime matches", async () => {
    (statSync as unknown as vi.Mock)
      .mockReturnValueOnce({ mtimeMs: 2000, size: 100 } as any)
      .mockReturnValueOnce({ mtimeMs: 2000, size: 150 } as any);
    (readFileSync as unknown as vi.Mock).mockReturnValueOnce("-- +nomad Up\nSELECT 1;\n");
    (calculateChecksum as unknown as vi.Mock).mockReturnValue("checksum1");
    (parseNomadSqlFile as unknown as vi.Mock).mockReturnValue({
      up: { statements: ["SELECT 1;"], statementMeta: [], notx: false },
      down: { statements: [], statementMeta: [], notx: false },
      tags: []
    });

    await migrator.loadMigrationFiles();

    (readFileSync as unknown as vi.Mock).mockClear();
    (calculateChecksum as unknown as vi.Mock).mockReturnValueOnce("checksum2");

    await migrator.loadMigrationFiles();

    expect(readFileSync).toHaveBeenCalled();
  });

  it("reparses when hash guard enabled even if mtime+size unchanged", async () => {
    process.env.NOMAD_CACHE_HASH_GUARD = "true";

    (statSync as unknown as vi.Mock).mockReturnValue({ mtimeMs: 3000, size: 100 } as any);
    (readFileSync as unknown as vi.Mock).mockReturnValueOnce("-- +nomad Up\nSELECT 1;\n");
    (calculateChecksum as unknown as vi.Mock)
      .mockReturnValueOnce("checksum1")
      .mockReturnValueOnce("checksum2");
    (parseNomadSqlFile as unknown as vi.Mock).mockReturnValue({
      up: { statements: ["SELECT 1;"], statementMeta: [], notx: false },
      down: { statements: [], statementMeta: [], notx: false },
      tags: []
    });

    await migrator.loadMigrationFiles();

    (readFileSync as unknown as vi.Mock).mockClear();
    (readFileSync as unknown as vi.Mock).mockReturnValueOnce("-- +nomad Up\nSELECT 1;\n -- drift");

    await migrator.loadMigrationFiles();

    expect(readFileSync).toHaveBeenCalled();
  });
});
