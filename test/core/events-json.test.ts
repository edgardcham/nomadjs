
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { MockedFunction } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile, type ParsedMigration } from "../../src/parser/enhanced-parser.js";
import { readFileSync } from "node:fs";
import { calculateChecksum } from "../../src/core/checksum.js";
import type { Config } from "../../src/config.js";
import type { Driver, DriverConnection, AppliedMigrationRow } from "../../src/driver/types.js";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");

interface TestMigration {
  version: string;
  name: string;
  filepath: string;
  content: string;
  parsed: ParsedMigration;
  checksum: string;
}

interface AppliedRowInput {
  version: string;
  name?: string;
  checksum?: string;
  rolledBackAt?: string | null;
}

const MIG_DIR = "/migrations";


let listFilesMock: MockedFunction<typeof listMigrationFiles>;
let readFileMock: MockedFunction<typeof readFileSync>;
let parseFileMock: MockedFunction<typeof parseNomadSqlFile>;
let filenameToVersionMock: MockedFunction<typeof filenameToVersion>;
let consoleLogSpy: ReturnType<typeof vi.spyOn> | undefined;

beforeEach(() => {
  listFilesMock = vi.mocked(listMigrationFiles);
  readFileMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
  parseFileMock = vi.mocked(parseNomadSqlFile);
  filenameToVersionMock = vi.mocked(filenameToVersion);
});

afterEach(() => {
  consoleLogSpy?.mockRestore();
  consoleLogSpy = undefined;
});

describe.each(["postgres", "mysql"] as const)("Events JSON (%s)", flavor => {
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

    const { migrator, events } = setupTest({ migrations, flavor });

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

  it("emits down/up cycles for redo()", async () => {
    const migration = buildMigration("20240103040404", "redo_me", [
      "CREATE TABLE t(id int);"
    ], [
      "DROP TABLE t;"
    ]);

    const { migrator, events, setAppliedRows } = setupTest({
      migrations: [migration],
      flavor
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
      migrations: [migrationA, migrationB],
      flavor
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
      migrations: [migrationA, migrationB],
      flavor
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
    const { migrator, events, setAppliedRows } = setupTest({ migrations: [migration], flavor });
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

function setupTest(options: { migrations: TestMigration[]; flavor: "postgres" | "mysql" }) {
  const sorted = [...options.migrations].sort((a, b) => (a.version < b.version ? -1 : a.version > b.version ? 1 : 0));
  const byFile = new Map(sorted.map(m => [m.filepath, m]));
  const byVersion = new Map(sorted.map(m => [m.version, m]));
  let appliedRows: AppliedMigrationRow[] = [];

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

  const driver = createStatefulDriver(options.flavor, appliedRowsRef => {
    appliedRows = appliedRowsRef;
  });
  const config: Config = {
    driver: options.flavor,
    url: options.flavor === "mysql" ? "mysql://test@test/db" : "postgresql://test@test/db",
    dir: MIG_DIR,
    table: "nomad_migrations",
    schema: options.flavor === "postgres" ? "public" : undefined,
    allowDrift: false,
    autoNotx: false,
    eventsJson: true
  } as any;
  const migrator = new Migrator(config, driver);

  const events: any[] = [];
  consoleLogSpy = vi.spyOn(console, "log").mockImplementation((value: any) => {
    const text = typeof value === "string" ? value : String(value);
    const trimmed = text.trim();
    if (!trimmed.startsWith("{")) return;
    events.push(JSON.parse(trimmed));
  });

  return {
    migrator,
    events,
    setAppliedRows(inputs: AppliedRowInput[]) {
      appliedRows = inputs.map(input => {
        const mig = byVersion.get(input.version);
        const checksum = input.checksum ?? mig?.checksum ?? `chk-${input.version}`;
        const name = input.name ?? mig?.name ?? input.version;
        return {
          version: BigInt(input.version),
          name,
          checksum,
          appliedAt: new Date("2024-01-01T00:00:00Z"),
          rolledBackAt: input.rolledBackAt ? new Date(input.rolledBackAt) : null
        } satisfies AppliedMigrationRow;
      });
      driver.__setAppliedRows(appliedRows);
    }
  };
}

function createStatefulDriver(flavor: "postgres" | "mysql", onStateChange: (rows: AppliedMigrationRow[]) => void): Driver & { __setAppliedRows(rows: AppliedMigrationRow[]): void } {
  let appliedRows: AppliedMigrationRow[] = [];

  const createConnection = (): DriverConnection => {
    return {
      ensureMigrationsTable: vi.fn().mockResolvedValue(undefined),
      fetchAppliedMigrations: vi.fn().mockResolvedValue(appliedRows.map(row => ({ ...row }))),
      markMigrationApplied: vi.fn(async ({ version, name, checksum }) => {
        const existing = appliedRows.find(row => row.version === version);
        if (existing) {
          existing.name = name;
          existing.checksum = checksum;
          existing.appliedAt = new Date();
          existing.rolledBackAt = null;
        } else {
          appliedRows.push({
            version,
            name,
            checksum,
            appliedAt: new Date(),
            rolledBackAt: null
          });
        }
      }),
      markMigrationRolledBack: vi.fn(async (version: bigint) => {
        const existing = appliedRows.find(row => row.version === version);
        if (existing) {
          existing.rolledBackAt = new Date();
        }
      }),
      acquireLock: vi.fn().mockResolvedValue(true),
      releaseLock: vi.fn().mockResolvedValue(undefined),
      beginTransaction: vi.fn().mockResolvedValue(undefined),
      commitTransaction: vi.fn().mockResolvedValue(undefined),
      rollbackTransaction: vi.fn().mockResolvedValue(undefined),
      runStatement: vi.fn().mockResolvedValue(undefined),
      dispose: vi.fn().mockResolvedValue(undefined)
    } as DriverConnection;
  };

  const driver: Driver & { __setAppliedRows(rows: AppliedMigrationRow[]): void } = {
    supportsTransactionalDDL: flavor === "postgres",
    connect: vi.fn(async () => createConnection()),
    close: vi.fn().mockResolvedValue(undefined),
    quoteIdent: vi.fn(identifier => flavor === "mysql" ? `\`${identifier}\`` : `"${identifier}"`),
    nowExpression: vi.fn(() => flavor === "mysql" ? "CURRENT_TIMESTAMP(3)" : "NOW()"),
    mapError: vi.fn(error => (error instanceof Error ? error : new Error(String(error)))),
    probeConnection: vi.fn().mockResolvedValue(undefined),
    __setAppliedRows(rows: AppliedMigrationRow[]) {
      appliedRows = rows.map(row => ({ ...row }));
      onStateChange(appliedRows);
    }
  } as Driver & { __setAppliedRows(rows: AppliedMigrationRow[]): void };

  driver.__setAppliedRows([]);
  return driver;
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

  const parsed: ParsedMigration = {
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
    statementBlocks: [],
    tags: [],
    noTransaction: false
  } as ParsedMigration;

  return {
    version,
    name,
    filepath,
    content,
    parsed,
    checksum: calculateChecksum(content)
  };
}

function setAppliedRowsOnDriver(driver: Driver & { __setAppliedRows(rows: AppliedMigrationRow[]): void }, rows: AppliedMigrationRow[]) {
  driver.__setAppliedRows(rows);
}

