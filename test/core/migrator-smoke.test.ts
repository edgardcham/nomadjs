import { describe, it, expect, beforeEach, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import type { Config } from "../../src/config.js";
import { createDriverMock, type DriverMock } from "../helpers/driver-mock.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { readFileSync } from "node:fs";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");

const MIGRATION_VERSION = 20240101120000n;
const MIGRATION_NAME = "create_users";
const FILEPATH = `/migrations/${MIGRATION_VERSION}_${MIGRATION_NAME}.sql`;

const MIGRATION_SQL = {
  up: ["CREATE TABLE users (id INT);"] as const,
  down: ["DROP TABLE users;"] as const
};

function setupStubs() {
  vi.mocked(listMigrationFiles).mockReturnValue([FILEPATH]);
  vi.mocked(filenameToVersion).mockReturnValue(String(MIGRATION_VERSION));
  vi.mocked(readFileSync).mockReturnValue("-- migration file");
  vi.mocked(parseNomadSqlFile).mockReturnValue({
    up: { statements: MIGRATION_SQL.up, notx: false },
    down: { statements: MIGRATION_SQL.down, notx: false },
    tags: []
  } as any);
  vi.mocked(calculateChecksum).mockReturnValue("chk:create_users");
}

describe.each(["postgres", "mysql", "sqlite"] as const)("Migrator smoke (%s)", flavor => {
  let config: Config;
  let driver: DriverMock;
  let migrator: Migrator;

  beforeEach(() => {
    setupStubs();

    config = {
      driver: flavor,
      url:
        flavor === "postgres"
          ? "postgresql://localhost/test"
          : flavor === "mysql"
            ? "mysql://localhost/test"
            : "sqlite:///tmp/smoke.sqlite",
      dir: "/migrations",
      table: "nomad_migrations",
      schema: flavor === "postgres" ? "public" : undefined,
      allowDrift: false,
      autoNotx: false,
      lockTimeout: 30000
    };

    driver = createDriverMock({ flavor });
    migrator = new Migrator(config, driver);
  });

  it("applies pending migrations", async () => {
    const ensureConn = driver.enqueueConnection({});
    const fetchConn = driver.enqueueConnection({ fetchAppliedMigrations: vi.fn().mockResolvedValue([]) });
    const execConn = driver.enqueueConnection({
      acquireLock: vi.fn().mockResolvedValue(true),
      markMigrationApplied: vi.fn().mockResolvedValue(undefined)
    });

    execConn.runStatement.mockResolvedValue(undefined);

    await migrator.up();

    expect(ensureConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
  });

  it("rolls back applied migrations", async () => {
    const ensureConn = driver.enqueueConnection({});
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([
        {
          version: MIGRATION_VERSION,
          name: MIGRATION_NAME,
          checksum: "chk:create_users",
          appliedAt: new Date(),
          rolledBackAt: null
        }
      ])
    });
    const execConn = driver.enqueueConnection({ acquireLock: vi.fn().mockResolvedValue(true) });

    execConn.runStatement.mockResolvedValue(undefined);

    await migrator.down();

    expect(ensureConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
  });

  it("redo re-applies last migration", async () => {
    const ensureConn = driver.enqueueConnection({});
    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([
        {
          version: MIGRATION_VERSION,
          name: MIGRATION_NAME,
          checksum: "chk:create_users",
          appliedAt: new Date(),
          rolledBackAt: null
        }
      ])
    });
    const execConn = driver.enqueueConnection({ acquireLock: vi.fn().mockResolvedValue(true) });

    execConn.runStatement.mockResolvedValue(undefined);

    await migrator.redo();

    expect(ensureConn.ensureMigrationsTable).toHaveBeenCalled();
    expect(fetchConn.fetchAppliedMigrations).toHaveBeenCalled();
    if (flavor === "postgres") {
      expect(execConn.beginTransaction).toHaveBeenCalledTimes(2);
      expect(execConn.commitTransaction).toHaveBeenCalledTimes(2);
    } else {
      expect(execConn.beginTransaction).not.toHaveBeenCalled();
      expect(execConn.commitTransaction).not.toHaveBeenCalled();
    }
  });
});
