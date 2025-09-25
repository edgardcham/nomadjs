import { describe, it, expect, beforeEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import type { Config } from "../../src/config.js";

vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Tag filtering with include-ancestors", () => {
  let migrator: Migrator;
  let mockPool: any;
  let queryMock: ReturnType<typeof vi.fn>;
  let listMigrationFilesMock: ReturnType<typeof vi.fn>;
  let readFileSyncMock: ReturnType<typeof vi.fn>;
  let parseNomadSqlFileMock: ReturnType<typeof vi.fn>;
  let filenameToVersionMock: ReturnType<typeof vi.fn>;

  const config: Config = {
    driver: "postgres",
    url: "postgresql://test:test@localhost:5432/testdb",
    dir: "/test/migrations",
    table: "nomad_migrations",
    allowDrift: false,
    autoNotx: false
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});

    queryMock = vi.fn();
    mockPool = {
      query: queryMock,
      end: vi.fn(),
      connect: vi.fn().mockResolvedValue({ query: queryMock, release: vi.fn() })
    };
    (Pool as any).mockImplementation(() => mockPool);

    listMigrationFilesMock = listMigrationFiles as any;
    readFileSyncMock = readFileSync as any;
    parseNomadSqlFileMock = parseNomadSqlFile as any;
    filenameToVersionMock = filenameToVersion as any;
    filenameToVersionMock.mockImplementation((filepath: string) => {
      const m = filepath.match(/(\d{14})/);
      return m ? m[1] : undefined;
    });

    migrator = new Migrator(config, mockPool);
  });

  it("planUp includes earlier pending when includeAncestors=true", async () => {
    listMigrationFilesMock.mockReturnValue([
      "/test/migrations/20240101120000_init.sql",
      "/test/migrations/20240102120000_users.sql"
    ]);

    const sql = "SELECT 1;";
    readFileSyncMock.mockReturnValue(sql);
    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: undefined })
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: ["users"] });

    // No applied
    queryMock
      .mockResolvedValueOnce({ rows: [] }) // ensureTable
      .mockResolvedValueOnce({ rows: [] });

    const plan = await migrator.planUp({ filter: { tags: ["users"] } as any, includeAncestors: true });
    expect(plan.migrations.map(m => m.name)).toEqual(["init", "users"]);
  });

  it("planUp warns when earlier pending are excluded and includeAncestors=false", async () => {
    listMigrationFilesMock.mockReturnValue([
      "/test/migrations/20240101120000_init.sql",
      "/test/migrations/20240102120000_users.sql"
    ]);

    const sql = "SELECT 1;";
    readFileSyncMock.mockReturnValue(sql);
    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: undefined })
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: ["users"] });

    // No applied
    queryMock
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [] });

    const plan = await migrator.planUp({ filter: { tags: ["users"] } as any });
    expect(plan.migrations.map(m => m.name)).toEqual(["users"]);
    expect(plan.summary.warnings?.some(w => /ancestors|earlier pending/i.test(w))).toBe(true);
  });
});

