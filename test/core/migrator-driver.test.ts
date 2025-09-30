import { describe, it, expect, vi } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import type { Driver, DriverConnection } from "../../src/driver/types.js";
import type { Config } from "../../src/config.js";

describe("Migrator driver seams", () => {
  const baseConfig: Config = {
    driver: "postgres",
    url: "postgresql://localhost:5432/test",
    dir: "migrations",
    table: "nomad_migrations",
    allowDrift: false,
    autoNotx: false
  };

  function buildDriver(connection: DriverConnection) {
    const connectMock = vi.fn<[], Promise<DriverConnection>>().mockResolvedValue(connection);

    const driver: Driver = {
      supportsTransactionalDDL: true,
      connect: connectMock,
      close: vi.fn().mockResolvedValue(undefined),
      quoteIdent: vi.fn(id => `"${id}"`),
      nowExpression: vi.fn(() => "NOW()"),
      mapError: vi.fn(error => (error instanceof Error ? error : new Error(String(error)))),
      probeConnection: vi.fn().mockResolvedValue(undefined)
    };

    return { driver, connectMock };
  }

  function createConnection(overrides: Partial<DriverConnection> = {}): DriverConnection {
    return {
      ensureMigrationsTable: vi.fn().mockResolvedValue(undefined),
      fetchAppliedMigrations: vi.fn().mockResolvedValue([]),
      markMigrationApplied: vi.fn().mockResolvedValue(undefined),
      markMigrationRolledBack: vi.fn().mockResolvedValue(undefined),
      acquireLock: vi.fn().mockResolvedValue(true),
      releaseLock: vi.fn().mockResolvedValue(undefined),
      beginTransaction: vi.fn().mockResolvedValue(undefined),
      commitTransaction: vi.fn().mockResolvedValue(undefined),
      rollbackTransaction: vi.fn().mockResolvedValue(undefined),
      runStatement: vi.fn().mockResolvedValue(undefined),
      dispose: vi.fn().mockResolvedValue(undefined),
      ...overrides
    } as DriverConnection;
  }

  it("uses driver connection for ensureTable", async () => {
    const connection = createConnection();
    const { driver, connectMock } = buildDriver(connection);

    const migrator = new Migrator(baseConfig, driver);
    await migrator.ensureTable();

    expect(connectMock).toHaveBeenCalledTimes(1);
    expect(connection.ensureMigrationsTable).toHaveBeenCalledTimes(1);
    expect(connection.dispose).toHaveBeenCalledTimes(1);
  });

  it("maps applied migration rows from driver", async () => {
    const appliedAt = new Date("2024-01-01T00:00:00Z");
    const connection = createConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue([
        {
          version: 20240101120000n,
          name: "create_users",
          checksum: "abc123",
          appliedAt,
          rolledBackAt: null
        }
      ])
    });
    const { driver } = buildDriver(connection);

    const migrator = new Migrator(baseConfig, driver);
    const applied = await migrator.getAppliedMigrations();

    expect(applied).toEqual([
      {
        version: 20240101120000n,
        name: "create_users",
        checksum: "abc123",
        appliedAt,
        rolledBackAt: null
      }
    ]);
    expect(connection.fetchAppliedMigrations).toHaveBeenCalledTimes(1);
    expect(connection.dispose).toHaveBeenCalledTimes(1);
  });
});
