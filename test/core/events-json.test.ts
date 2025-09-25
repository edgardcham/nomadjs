import { describe, it, expect, beforeEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import type { Config } from "../../src/config.js";

vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

describe("Events JSON", () => {
  let migrator: Migrator;
  let mockPool: any;
  let queryMock: ReturnType<typeof vi.fn>;
  let listFilesMock: ReturnType<typeof vi.fn>;
  let readFileMock: ReturnType<typeof vi.fn>;
  let parseFileMock: ReturnType<typeof vi.fn>;
  let filenameToVersionMock: ReturnType<typeof vi.fn>;

  const baseConfig: Config = {
    driver: "postgres",
    url: "postgresql://test@test/db",
    dir: "/migrations",
    table: "nomad_migrations",
    allowDrift: false,
    autoNotx: false
  } as any;

  beforeEach(() => {
    vi.clearAllMocks();
    queryMock = vi.fn(async (sql: string) => {
      if (/pg_try_advisory_lock/i.test(sql)) return { rows: [{ pg_try_advisory_lock: true }] } as any;
      if (/pg_advisory_unlock/i.test(sql)) return { rows: [{ pg_advisory_unlock: true }] } as any;
      return { rows: [] } as any;
    });
    mockPool = {
      query: queryMock,
      connect: vi.fn().mockResolvedValue({ query: queryMock, release: vi.fn() }),
      end: vi.fn()
    };
    (Pool as any).mockImplementation(() => mockPool);

    listFilesMock = listMigrationFiles as any;
    readFileMock = readFileSync as any;
    parseFileMock = parseNomadSqlFile as any;
    filenameToVersionMock = filenameToVersion as any;
    filenameToVersionMock.mockImplementation((fp: string) => (fp.match(/(\d{14})/) || [])[1]);
  });

  it("emits lock/apply/stmt/apply/lock events for up", async () => {
    const config: Config = { ...baseConfig, eventsJson: true } as any;
    migrator = new Migrator(config, mockPool);

    listFilesMock.mockReturnValue([
      "/migrations/20240101120000_v.sql"
    ]);
    readFileMock.mockReturnValue("CREATE TABLE t(id int); INSERT INTO t VALUES (1);");
    parseFileMock.mockReturnValue({
      up: { statements: ["CREATE TABLE t(id int);", "INSERT INTO t VALUES (1);"] , notx: false },
      down: { statements: [], notx: false },
      noTransaction: false,
      tags: ["test"]
    });

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    await migrator.up();

    const lines = logSpy.mock.calls.map(c => String(c[0])).filter(s => s.trim().startsWith("{"));
    const events = lines.map(s => JSON.parse(s));

    const idxStart = events.findIndex(e => e.event === "apply-start" && e.version === "20240101120000");
    expect(idxStart).toBeGreaterThanOrEqual(0);
    expect(events.filter(e => e.event === "stmt-run").length).toBe(2);
    const idxEnd = events.findIndex(e => e.event === "apply-end" && e.version === "20240101120000");
    expect(idxEnd).toBeGreaterThan(idxStart);
    const idxUnlock = events.findIndex(e => e.event === "lock-released");
    expect(idxUnlock).toBeGreaterThan(idxEnd);
  });
});
