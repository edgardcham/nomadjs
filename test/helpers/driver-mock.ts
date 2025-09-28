import { vi } from "vitest";
import type { Pool } from "pg";
import type { Driver, DriverConnection, AppliedMigrationRow } from "../../src/driver/types.js";

export type DriverConnectionMock = DriverConnection & {
  ensureMigrationsTable: ReturnType<typeof vi.fn<[], Promise<void>>>;
  fetchAppliedMigrations: ReturnType<typeof vi.fn<[], Promise<AppliedMigrationRow[]>>>;
  markMigrationApplied: ReturnType<typeof vi.fn<[{
    version: bigint;
    name: string;
    checksum: string;
  }], Promise<void>>>;
  markMigrationRolledBack: ReturnType<typeof vi.fn<[bigint], Promise<void>>>;
  acquireLock: ReturnType<typeof vi.fn<[string, number], Promise<boolean>>>;
  releaseLock: ReturnType<typeof vi.fn<[string], Promise<void>>>;
  beginTransaction: ReturnType<typeof vi.fn<[], Promise<void>>>;
  commitTransaction: ReturnType<typeof vi.fn<[], Promise<void>>>;
  rollbackTransaction: ReturnType<typeof vi.fn<[], Promise<void>>>;
  runStatement: ReturnType<typeof vi.fn<[string], Promise<void>>>;
  dispose: ReturnType<typeof vi.fn<[], Promise<void>>>;
};

export type DriverMock = Driver & {
  enqueueConnection: (overrides?: Partial<DriverConnectionMock>) => DriverConnectionMock;
};

function createConnectionMock(overrides: Partial<DriverConnectionMock> = {}): DriverConnectionMock {
  const connection = {
    ensureMigrationsTable: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    fetchAppliedMigrations: vi.fn<[], Promise<AppliedMigrationRow[]>>().mockResolvedValue([]),
    markMigrationApplied: vi.fn<[{
      version: bigint;
      name: string;
      checksum: string;
    }], Promise<void>>().mockResolvedValue(undefined),
    markMigrationRolledBack: vi.fn<[bigint], Promise<void>>().mockResolvedValue(undefined),
    acquireLock: vi.fn<[string, number], Promise<boolean>>().mockResolvedValue(true),
    releaseLock: vi.fn<[string], Promise<void>>().mockResolvedValue(undefined),
    beginTransaction: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    commitTransaction: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    rollbackTransaction: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    runStatement: vi.fn<[string], Promise<void>>().mockResolvedValue(undefined),
    dispose: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    ...overrides
  } satisfies Partial<DriverConnectionMock>;

  return connection as DriverConnectionMock;
}

export function createDriverMock(mockPool: Pool): DriverMock {
  const connectionQueue: DriverConnectionMock[] = [];

  const connect = vi.fn<[], Promise<DriverConnectionMock>>(async () => {
    if (connectionQueue.length === 0) {
      throw new Error("No driver connection mock enqueued");
    }
    return connectionQueue.shift()!;
  });

  const driver = {
    supportsTransactionalDDL: true,
    getPool: vi.fn<[], Pool>(() => mockPool),
    connect: connect as unknown as Driver["connect"],
    close: vi.fn<[], Promise<void>>().mockResolvedValue(undefined) as unknown as Driver["close"],
    quoteIdent: vi.fn<[string], string>(identifier => `"${identifier}"`) as unknown as Driver["quoteIdent"],
    nowExpression: vi.fn<[], string>(() => "NOW()") as unknown as Driver["nowExpression"],
    mapError: vi.fn<[unknown], Error>(error => (error instanceof Error ? error : new Error(String(error)))) as unknown as Driver["mapError"],
    enqueueConnection(overrides: Partial<DriverConnectionMock> = {}) {
      const conn = createConnectionMock(overrides);
      connectionQueue.push(conn);
      return conn;
    }
  } as DriverMock;

  return driver;
}
