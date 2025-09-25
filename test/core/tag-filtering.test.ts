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

describe("Tag Filtering", () => {
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
      connect: vi.fn().mockResolvedValue({
        query: queryMock,
        release: vi.fn()
      })
    };
    (Pool as any).mockImplementation(() => mockPool);

    listMigrationFilesMock = listMigrationFiles as any;
    readFileSyncMock = readFileSync as any;
    parseNomadSqlFileMock = parseNomadSqlFile as any;
    filenameToVersionMock = filenameToVersion as any;
    filenameToVersionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    migrator = new Migrator(config, mockPool);
  });

  it("planUp filters pending by tags", async () => {
    listMigrationFilesMock.mockReturnValue([
      "/test/migrations/20240101120000_create.sql",
      "/test/migrations/20240102120000_seed.sql",
      "/test/migrations/20240103120000_add_index.sql"
    ]);

    const sql = "SELECT 1;";
    readFileSyncMock.mockReturnValue(sql);
    // Untagged
    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [], notx: false }, noTransaction: false, tags: undefined })
      // Tagged with seed
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [], notx: false }, noTransaction: false, tags: ["seed"] })
      // Tagged with users
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [], notx: false }, noTransaction: false, tags: ["users"] });

    // No applied rows
    queryMock
      .mockResolvedValueOnce({ rows: [] }) // ensureTable
      .mockResolvedValueOnce({ rows: [] });

    const plan = await migrator.planUp({ filter: { tags: ["seed"] } as any });
    expect(plan.migrations.map(m => m.name)).toEqual(["seed"]);
  });

  it("down rolls back only matching head", async () => {
    listMigrationFilesMock.mockReturnValue([
      "/test/migrations/20240101120000_one.sql",
      "/test/migrations/20240102120000_two.sql",
      "/test/migrations/20240103120000_three.sql"
    ]);

    const sql = "SELECT 1;";
    readFileSyncMock.mockReturnValue(sql);
    // v1 tagged seed, v2 untagged, v3 tagged seed
    parseNomadSqlFileMock
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: ["seed"] })
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: undefined })
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, noTransaction: false, tags: ["seed"] });

    // All applied
    queryMock
      .mockResolvedValueOnce({ rows: [] }) // ensureTable
      .mockResolvedValueOnce({
        rows: [
          { version: "20240101120000", name: "one", checksum: calculateChecksum(sql), applied_at: new Date(), rolled_back_at: null },
          { version: "20240102120000", name: "two", checksum: calculateChecksum(sql), applied_at: new Date(), rolled_back_at: null },
          { version: "20240103120000", name: "three", checksum: calculateChecksum(sql), applied_at: new Date(), rolled_back_at: null }
        ]
      });

    const plan = await migrator.planDown({ count: 3, filter: { tags: ["seed"] } as any });

    // Head is v3[tagged], so we may roll back v3 only; stop before v2 (untagged)
    expect(plan.migrations.map(m => m.name)).toEqual(["three"]);

    // If the head were untagged, expect empty
    // Simulate by flipping tag of v3 to undefined
  });
});

