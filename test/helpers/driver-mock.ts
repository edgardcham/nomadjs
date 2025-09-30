import { vi } from "vitest";
import type { Driver, DriverConnection, AppliedMigrationRow } from "../../src/driver/types.js";

export type DriverConnectionMock = DriverConnection & {
  ensureMigrationsTable: ReturnType<typeof vi.fn<[], Promise<void>>>;
  fetchAppliedMigrations: ReturnType<typeof vi.fn<[], Promise<AppliedMigrationRow[]>>>;
  markMigrationApplied: ReturnType<typeof vi.fn<[
    {
      version: bigint;
      name: string;
      checksum: string;
    }
  ], Promise<void>>>;
  markMigrationRolledBack: ReturnType<typeof vi.fn<[bigint], Promise<void>>>;
  acquireLock: ReturnType<typeof vi.fn<[string, number], Promise<boolean>>>;
  releaseLock: ReturnType<typeof vi.fn<[string], Promise<void>>>;
  beginTransaction: ReturnType<typeof vi.fn<[], Promise<void>>>;
  query: ReturnType<typeof vi.fn<[string, unknown[]?], Promise<{ rows: unknown[] }>>>;
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
    markMigrationApplied: vi
      .fn<[
        {
          version: bigint;
          name: string;
          checksum: string;
        }
      ], Promise<void>>()
      .mockResolvedValue(undefined),
    markMigrationRolledBack: vi.fn<[bigint], Promise<void>>().mockResolvedValue(undefined),
    acquireLock: vi.fn<[string, number], Promise<boolean>>().mockResolvedValue(true),
    releaseLock: vi.fn<[string], Promise<void>>().mockResolvedValue(undefined),
    beginTransaction: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    query: vi.fn<[string, unknown[]?], Promise<{ rows: unknown[] }>>().mockResolvedValue({ rows: [] }),
    commitTransaction: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    rollbackTransaction: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    runStatement: vi.fn<[string], Promise<void>>().mockResolvedValue(undefined),
    dispose: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    ...overrides
  } satisfies Partial<DriverConnectionMock>;

  return connection as DriverConnectionMock;
}

export type DriverFlavor = "postgres" | "mysql" | "sqlite";

export interface DriverMockOptions {
  flavor?: DriverFlavor;
}

export function createDriverMock(options: DriverMockOptions = {}): DriverMock {
  const flavor = options.flavor || "postgres";
  const connectionQueue: DriverConnectionMock[] = [];

  const connect = vi.fn<[], Promise<DriverConnectionMock>>(async () => {
    if (connectionQueue.length === 0) {
      throw new Error("No driver connection mock enqueued");
    }
    return connectionQueue.shift()!;
  });

  const driver = {
    supportsTransactionalDDL: flavor === "postgres",
    connect: connect as unknown as Driver["connect"],
    close: vi.fn<[], Promise<void>>().mockResolvedValue(undefined) as unknown as Driver["close"],
    quoteIdent: vi.fn<[string], string>(identifier =>
      flavor === "mysql" ? `\`${identifier}\`` : `"${identifier}"`
    ) as unknown as Driver["quoteIdent"],
    nowExpression: vi.fn<[], string>(() => {
      if (flavor === "mysql") return "CURRENT_TIMESTAMP(3)";
      if (flavor === "sqlite") return "CURRENT_TIMESTAMP";
      return "NOW()";
    }) as unknown as Driver["nowExpression"],
    mapError: vi.fn<[unknown], Error>(error => (error instanceof Error ? error : new Error(String(error)))) as unknown as Driver["mapError"],
    probeConnection: vi.fn<[], Promise<void>>().mockResolvedValue(undefined) as unknown as Driver["probeConnection"],
    enqueueConnection(overrides: Partial<DriverConnectionMock> = {}) {
      const conn = createConnectionMock(overrides);
      connectionQueue.push(conn);
      return conn;
    }
  } as DriverMock;

  return driver;
}
