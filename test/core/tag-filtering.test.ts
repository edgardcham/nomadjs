
import { describe, it, expect, beforeEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { readFileSync } from "node:fs";
import type { Config } from "../../src/config.js";
import type { AppliedMigrationRow } from "../../src/driver/types.js";
import { createDriverMock } from "../helpers/driver-mock.js";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Tag Filtering", () => {
  let migrator: Migrator;
  let listFilesMock: ReturnType<typeof vi.mocked>;
  let readFileMock: ReturnType<typeof vi.mocked>;
  let parseFileMock: ReturnType<typeof vi.mocked>;
  let filenameToVersionMock: ReturnType<typeof vi.mocked>;
  const driver = createDriverMock();

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
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});

    listFilesMock = vi.mocked(listMigrationFiles);
    readFileMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
    parseFileMock = vi.mocked(parseNomadSqlFile);
    filenameToVersionMock = vi.mocked(filenameToVersion);

    filenameToVersionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    migrator = new Migrator(config, driver);
  });

  it("planUp filters pending by tags", async () => {
    listFilesMock.mockReturnValue([
      "/test/migrations/20240101120000_create.sql",
      "/test/migrations/20240102120000_seed.sql",
      "/test/migrations/20240103120000_add_index.sql"
    ]);

    const sql = "SELECT 1;";
    readFileMock.mockReturnValue(sql);
    parseFileMock
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [], notx: false }, tags: undefined } as any)
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [], notx: false }, tags: ["seed"] } as any)
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [], notx: false }, tags: ["users"] } as any);

    const ensureConn = driver.enqueueConnection({
      ensureMigrationsTable: vi.fn().mockResolvedValue(undefined),
      fetchAppliedMigrations: vi.fn().mockResolvedValue([])
    });
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([])
    });

    const plan = await migrator.planUp({ filter: { tags: ["seed"] } as any });

    expect(plan.migrations.map(m => m.name)).toEqual(["seed"]);
    expect(ensureConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
  });

  it("planDown rolls back only matching head", async () => {
    listFilesMock.mockReturnValue([
      "/test/migrations/20240101120000_one.sql",
      "/test/migrations/20240102120000_two.sql",
      "/test/migrations/20240103120000_three.sql"
    ]);

    const sql = "SELECT 1;";
    readFileMock.mockReturnValue(sql);
    parseFileMock
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, tags: ["seed"] } as any)
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, tags: undefined } as any)
      .mockReturnValueOnce({ up: { statements: [sql], notx: false }, down: { statements: [sql], notx: false }, tags: ["seed"] } as any);

    const appliedAt = new Date();
    const rows: AppliedMigrationRow[] = [
      { version: 20240101120000n, name: "one", checksum: calculateChecksum(sql), appliedAt, rolledBackAt: null },
      { version: 20240102120000n, name: "two", checksum: calculateChecksum(sql), appliedAt, rolledBackAt: null },
      { version: 20240103120000n, name: "three", checksum: calculateChecksum(sql), appliedAt, rolledBackAt: null }
    ];

    const ensureConn = driver.enqueueConnection({
      ensureMigrationsTable: vi.fn().mockResolvedValue(undefined),
      fetchAppliedMigrations: vi.fn().mockResolvedValue(rows)
    });
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(rows)
    });

    const plan = await migrator.planDown({ count: 3, filter: { tags: ["seed"] } as any });
    expect(plan.migrations.map(m => m.name)).toEqual(["three"]);
    expect(ensureConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
  });
});
