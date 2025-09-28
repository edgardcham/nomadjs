import type { Pool } from "pg";
export interface DriverOptions {
  url: string;
  table: string;
  schema?: string;
  connectTimeoutMs?: number;
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
  runStatement(sql: string): Promise<void>;
  dispose(): Promise<void>;
}

export interface Driver {
  getPool(): Pool;
  connect(): Promise<DriverConnection>;
  close(): Promise<void>;
  quoteIdent(identifier: string): string;
  nowExpression(): string;
  supportsTransactionalDDL: boolean;
  mapError(error: unknown): Error;
}
