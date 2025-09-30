
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

describe.each(["postgres", "mysql"] as const)("Verbose logging (%s)", flavor => {
  let migrator: Migrator;
  let listFilesMock: ReturnType<typeof vi.mocked>;
  let readFileMock: ReturnType<typeof vi.mocked>;
  let parseFileMock: ReturnType<typeof vi.mocked>;
  let filenameToVersionMock: ReturnType<typeof vi.mocked>;
  let driver = createDriverMock({ flavor });

  const config: Config = {
    driver: flavor,
    url: flavor === "mysql" ? "mysql://test@test/db" : "postgresql://test@test/db",
    dir: "/migrations",
    table: "nomad_migrations",
    schema: flavor === "postgres" ? "public" : undefined,
    allowDrift: false,
    autoNotx: false,
    verbose: true
  } as any;

  beforeEach(() => {
    vi.clearAllMocks();
    listFilesMock = vi.mocked(listMigrationFiles);
    readFileMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
    parseFileMock = vi.mocked(parseNomadSqlFile);
    filenameToVersionMock = vi.mocked(filenameToVersion);

    filenameToVersionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    driver = createDriverMock({ flavor });
    migrator = new Migrator(config, driver);
  });

  it("prints per-statement timing during up", async () => {
    listFilesMock.mockReturnValue([
      "/migrations/20240101120000_verbose.sql"
    ]);
    readFileMock.mockReturnValue("CREATE TABLE t(id int);\nINSERT INTO t VALUES (1);");
    parseFileMock.mockReturnValue({
      up: {
        statements: ["CREATE TABLE t(id int);", "INSERT INTO t VALUES (1);"] ,
        statementMeta: [
          { sql: "CREATE TABLE t(id int);", line: 1, column: 1 },
          { sql: "INSERT INTO t VALUES (1);", line: 2, column: 1 }
        ],
        notx: false
      },
      down: { statements: [], statementMeta: [], notx: false },
      noTransaction: false
    } as any);

    const execConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([])
    });
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([])
    });

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    await migrator.up();

    expect(execConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(execConn.markMigrationApplied).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
    const joined = logSpy.mock.calls.map(c => String(c[0])).join("\n");
    expect(joined).toMatch(/→ executing m1\/1 .*20240101120000/i);
    expect(joined).toMatch(/s1\/2 .*CREATE TABLE t\(id int\)/i);
    expect(joined).toMatch(/s2\/2 .*INSERT INTO t VALUES/i);
  });
});
