export interface PoolLike {
  connect(): Promise<{
    query(sql: string, params?: unknown[]): Promise<any>;
    release(): Promise<void> | void;
  }>;
  query(sql: string, params?: unknown[]): Promise<any>;
  end(): Promise<void> | void;
}

export interface DriverOptions {
  url: string;
  table: string;
  schema?: string;
  connectTimeoutMs?: number;
  pool?: PoolLike;
}

export interface AppliedMigrationRow {
  version: bigint;
  name: string;
  checksum: string;
  appliedAt: Date | null;
  rolledBackAt: Date | null;
}

export interface DriverConnection {
  ensureMigrationsTable(): Promise<void>;
  fetchAppliedMigrations(): Promise<AppliedMigrationRow[]>;
  markMigrationApplied(input: { version: bigint; name: string; checksum: string }): Promise<void>;
  markMigrationRolledBack(version: bigint): Promise<void>;
  acquireLock(lockKey: string, timeoutMs: number): Promise<boolean>;
  releaseLock(lockKey: string): Promise<void>;
  beginTransaction(): Promise<void>;
  commitTransaction(): Promise<void>;
  rollbackTransaction(): Promise<void>;
  query<T = unknown>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
  runStatement(sql: string): Promise<void>;
  dispose(): Promise<void>;
}

export interface Driver {
  connect(): Promise<DriverConnection>;
  close(): Promise<void>;
  quoteIdent(identifier: string): string;
  nowExpression(): string;
  supportsTransactionalDDL: boolean;
  mapError(error: unknown): Error;
  probeConnection(): Promise<void>;
}
