import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile, type ParsedMigration } from "../../src/parser/enhanced-parser.js";
import { readFileSync } from "node:fs";
import { Pool } from "pg";
import type { Config } from "../../src/config.js";
import { calculateChecksum } from "../../src/core/checksum.js";

vi.mock("pg");
vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

type TestMigration = {
  version: string;
  name: string;
  filepath: string;
  content: string;
  parsed: ParsedMigration;
  checksum: string;
};

type AppliedRowInput = {
  version: string;
  name?: string;
  checksum?: string;
  rolledBackAt?: string | null;
};

const MIG_DIR = "/migrations";

const baseConfig: Config = {
  driver: "postgres",
  url: "postgresql://test@test/db",
  dir: MIG_DIR,
  table: "nomad_migrations",
  allowDrift: false,
  autoNotx: false
} as any;

let mockPool: any;
let queryMock: ReturnType<typeof vi.fn>;
let listFilesMock: ReturnType<typeof vi.fn>;
let readFileMock: ReturnType<typeof vi.fn>;
let parseFileMock: ReturnType<typeof vi.fn>;
let filenameToVersionMock: ReturnType<typeof vi.fn>;
let logSpy: ReturnType<typeof vi.spyOn> | undefined;

beforeEach(() => {
  vi.clearAllMocks();

  queryMock = vi.fn(async () => ({ rows: [] }));
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
});

afterEach(() => {
  logSpy?.mockRestore();
  logSpy = undefined;
});

describe("Events JSON", () => {
  it("emits lock/apply/stmt/end events for up migrations", async () => {
    const migrations = [
      buildMigration("20240101010101", "create_users", [
        "CREATE TABLE users(id int);",
        "INSERT INTO users VALUES (1);"
      ], [
        "DROP TABLE users;"
      ]),
      buildMigration("20240101020202", "create_posts", [
        "CREATE TABLE posts(id int);"
      ], [
        "DROP TABLE posts;"
      ])
    ];

    const { migrator, events } = setupTest({ migrations });

    await migrator.up();

    const kinds = events.map(e => e.event);
    expect(kinds).toEqual([
      "lock-acquired",
      "apply-start",
      "stmt-run",
      "stmt-run",
      "apply-end",
      "apply-start",
      "stmt-run",
      "apply-end",
      "lock-released"
    ]);

    expect(events[1]).toMatchObject({
      event: "apply-start",
      direction: "up",
      version: "20240101010101",
      name: "create_users"
    });
    expect(events[4]).toMatchObject({
      event: "apply-end",
      direction: "up",
      version: "20240101010101",
      name: "create_users"
    });
    expect(events[6]).toMatchObject({
      event: "stmt-run",
      direction: "up",
      version: "20240101020202"
    });
    expect(events.at(-1)).toMatchObject({ event: "lock-released" });
  });

  it("emits structured events when rolling back with down()", async () => {
    const migration = buildMigration("20240102030303", "drop_users", [
      "CREATE TABLE users(id int);"
    ], [
      "DROP TABLE users;",
      "DROP TYPE user_status;"
    ]);

    const checksum = migration.checksum;
    const { migrator, events, setAppliedRows } = setupTest({
      migrations: [migration]
    });
    setAppliedRows([
      { version: migration.version, name: migration.name, checksum }
    ]);

    await migrator.down();

    const kinds = events.map(e => e.event);
    expect(kinds).toEqual([
      "lock-acquired",
      "apply-start",
      "stmt-run",
      "stmt-run",
      "apply-end",
      "lock-released"
    ]);

    expect(events[1]).toMatchObject({
      event: "apply-start",
      direction: "down",
      version: migration.version
    });
    expect(events.filter(e => e.event === "stmt-run")).toHaveLength(2);
    expect(events[4]).toMatchObject({
      event: "apply-end",
      direction: "down",
      version: migration.version
    });
  });

  it("emits down/up cycles for redo()", async () => {
    const migration = buildMigration("20240103040404", "redo_me", [
      "CREATE TABLE t(id int);"
    ], [
      "DROP TABLE t;"
    ]);

    const { migrator, events, setAppliedRows } = setupTest({
      migrations: [migration]
    });
    setAppliedRows([
      { version: migration.version, name: migration.name, checksum: migration.checksum }
    ]);

    await migrator.redo();

    const kinds = events.map(e => e.event);
    expect(kinds).toEqual([
      "lock-acquired",
      "apply-start",
      "stmt-run",
      "apply-end",
      "apply-start",
      "stmt-run",
      "apply-end",
      "lock-released"
    ]);

    expect(events[1]).toMatchObject({ direction: "down", version: migration.version });
    expect(events[3]).toMatchObject({ event: "apply-end", direction: "down" });
    expect(events[4]).toMatchObject({ event: "apply-start", direction: "up" });
    expect(events[6]).toMatchObject({ event: "apply-end", direction: "up" });
  });

  it("avoids duplicate lock events and emits down events when migrating to an older version", async () => {
    const migrationA = buildMigration("20240101010101", "base", ["CREATE TABLE a(id int);"]);
    const migrationB = buildMigration("20240104050505", "feature", ["ALTER TABLE a ADD COLUMN note text;"], ["ALTER TABLE a DROP COLUMN note;"]);

    const { migrator, events, setAppliedRows } = setupTest({
      migrations: [migrationA, migrationB]
    });
    setAppliedRows([
      { version: migrationA.version, checksum: migrationA.checksum },
      { version: migrationB.version, checksum: migrationB.checksum }
    ]);

    await migrator.to(BigInt(migrationA.version));

    const kinds = events.map(e => e.event);
    expect(kinds).toEqual([
      "lock-acquired",
      "apply-start",
      "stmt-run",
      "apply-end",
      "lock-released"
    ]);

    const lockEvents = events.filter(e => e.event === "lock-acquired");
    expect(lockEvents).toHaveLength(1);
    expect(events[1]).toMatchObject({ direction: "down", version: migrationB.version });
  });

  it("emits apply events when migrating up to a target version", async () => {
    const migrationA = buildMigration("20240101010101", "base", ["CREATE TABLE a(id int);"]);
    const migrationB = buildMigration("20240104050505", "feature", ["ALTER TABLE a ADD COLUMN note text;"]);

    const { migrator, events, setAppliedRows } = setupTest({
      migrations: [migrationA, migrationB]
    });
    setAppliedRows([
      { version: migrationA.version, checksum: migrationA.checksum }
    ]);

    await migrator.to(BigInt(migrationB.version));

    const kinds = events.map(e => e.event);
    expect(kinds).toEqual([
      "lock-acquired",
      "apply-start",
      "stmt-run",
      "apply-end",
      "lock-released"
    ]);

    expect(events[1]).toMatchObject({ direction: "up", version: migrationB.version, name: migrationB.name });
  });

  it("announces verify lifecycle with NDJSON when enabled", async () => {
    const migration = buildMigration("20240106060606", "verify_me", ["SELECT 1;"], ["SELECT 1;"]);
    const { migrator, events, setAppliedRows } = setupTest({ migrations: [migration] });
    setAppliedRows([
      { version: migration.version, checksum: migration.checksum, name: migration.name }
    ]);

    await migrator.verify();

    expect(events.map(e => e.event)).toEqual([
      "verify-start",
      "verify-end"
    ]);

    expect(events[0]).toHaveProperty("ts");
    expect(events[1]).toMatchObject({
      event: "verify-end",
      valid: true,
      driftCount: 0,
      missingCount: 0
    });
  });
});

function setupTest(options: {
  migrations: TestMigration[];
}) {
  const sorted = [...options.migrations].sort((a, b) => (a.version < b.version ? -1 : a.version > b.version ? 1 : 0));
  const byFile = new Map(sorted.map(m => [m.filepath, m]));
  const byVersion = new Map(sorted.map(m => [m.version, m]));

  listFilesMock.mockReturnValue(sorted.map(m => m.filepath));
  readFileMock.mockImplementation((filepath: string) => {
    const mig = byFile.get(filepath);
    if (!mig) throw new Error(`Missing mock for ${filepath}`);
    return mig.content;
  });
  parseFileMock.mockImplementation((filepath: string) => {
    const mig = byFile.get(filepath);
    if (!mig) throw new Error(`Missing mock for ${filepath}`);
    return mig.parsed;
  });
  filenameToVersionMock.mockImplementation((filepath: string) => {
    const mig = byFile.get(filepath);
    if (mig) return mig.version;
    const match = filepath.match(/(\d{14})/);
    if (!match) throw new Error(`Cannot derive version from ${filepath}`);
    return match[1];
  });

  let appliedRows = [] as Array<ReturnType<typeof toDbRow>>;

  queryMock.mockImplementation(async (sql: string) => {
    if (/pg_try_advisory_lock/i.test(sql)) {
      return { rows: [{ pg_try_advisory_lock: true }] };
    }
    if (/pg_advisory_unlock/i.test(sql)) {
      return { rows: [{ pg_advisory_unlock: true }] };
    }
    if (/SELECT version, name, checksum, applied_at, rolled_back_at/i.test(sql)) {
      return { rows: appliedRows };
    }
    return { rows: [] };
  });

  const config: Config = { ...baseConfig, eventsJson: true } as any;
  const migrator = new Migrator(config, mockPool);

  const events: any[] = [];
  logSpy = vi.spyOn(console, "log").mockImplementation((value: any) => {
    const str = typeof value === "string" ? value : String(value);
    const candidate = str.trim();
    if (!candidate.startsWith("{")) return;
    events.push(JSON.parse(candidate));
  });

  return {
    migrator,
    events,
    setAppliedRows(inputs: AppliedRowInput[]) {
      appliedRows = inputs.map(row => toDbRow(row, byVersion));
    }
  };
}

function buildMigration(
  version: string,
  name: string,
  upStatements: string[],
  downStatements: string[] = []
): TestMigration {
  const filepath = `${MIG_DIR}/${version}_${name}.sql`;
  const content = [
    "-- +nomad Up",
    ...upStatements,
    "-- +nomad Down",
    ...downStatements
  ].join("\n");

  const parsed = {
    up: {
      statements: upStatements,
      statementMeta: upStatements.map((sql, idx) => ({ sql, line: idx + 1, column: 1 })),
      notx: false
    },
    down: {
      statements: downStatements,
      statementMeta: downStatements.map((sql, idx) => ({ sql, line: idx + 1, column: 1 })),
      notx: false
    },
    noTransaction: false,
    tags: [] as string[]
  };

  return {
    version,
    name,
    filepath,
    content,
    parsed,
    checksum: calculateChecksum(content)
  };
}

function toDbRow(input: AppliedRowInput, lookup: Map<string, TestMigration>) {
  const migration = lookup.get(input.version);
  if (!migration) {
    throw new Error(`Missing migration for version ${input.version}`);
  }
  return {
    version: input.version,
    name: input.name ?? migration.name,
    checksum: input.checksum ?? migration.checksum,
    applied_at: new Date().toISOString(),
    rolled_back_at: input.rolledBackAt ?? null
  };
}
