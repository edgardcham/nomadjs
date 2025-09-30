
import { describe, it, expect, beforeEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { readFileSync } from "node:fs";
import type { Config } from "../../src/config.js";
import { createDriverMock } from "../helpers/driver-mock.js";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Tag ancestors", () => {
  let migrator: Migrator;
  let listFilesMock: ReturnType<typeof vi.mocked>;
  let readFileMock: ReturnType<typeof vi.mocked>;
  let parseFileMock: ReturnType<typeof vi.mocked>;
  let filenameToVersionMock: ReturnType<typeof vi.mocked>;
  const driver = createDriverMock();

  const config: Config = {
    driver: "postgres",
    url: "postgresql://test@test/db",
    dir: "/migrations",
    table: "nomad_migrations",
    allowDrift: false,
    autoNotx: false
  };

  beforeEach(() => {
    vi.clearAllMocks();
    listFilesMock = vi.mocked(listMigrationFiles);
    readFileMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
    parseFileMock = vi.mocked(parseNomadSqlFile);
    filenameToVersionMock = vi.mocked(filenameToVersion);
    filenameToVersionMock.mockImplementation((filepath: string) => (filepath.match(/(\d{14})/) || [])[1]);
    migrator = new Migrator(config, driver);
  });

  it("includes earlier ancestors when requested", async () => {
    listFilesMock.mockReturnValue([
      "/migrations/20240101000000_one.sql",
      "/migrations/20240102000000_two.sql",
      "/migrations/20240103000000_three.sql"
    ]);

    readFileMock.mockReturnValue("SELECT 1;");
    parseFileMock
      .mockReturnValueOnce({ up: { statements: ["SELECT 1;"], notx: false }, down: { statements: [], notx: false }, tags: [], noTransaction: false } as any)
      .mockReturnValueOnce({ up: { statements: ["SELECT 1;"], notx: false }, down: { statements: [], notx: false }, tags: ["users"], noTransaction: false } as any)
      .mockReturnValueOnce({ up: { statements: ["SELECT 1;"], notx: false }, down: { statements: [], notx: false }, tags: ["users"], noTransaction: false } as any);

    const ensureConn = driver.enqueueConnection({
      ensureMigrationsTable: vi.fn().mockResolvedValue(undefined),
      fetchAppliedMigrations: vi.fn().mockResolvedValue([])
    });
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([])
    });

    const plan = await migrator.planUp({ filter: { tags: ["users"] } as any, includeAncestors: true });

    expect(plan.migrations.map(m => m.version.toString())).toEqual(["20240101000000", "20240102000000", "20240103000000"]);
    expect(ensureConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
  });
});
