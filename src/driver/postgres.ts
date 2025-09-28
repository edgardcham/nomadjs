import { Pool } from "pg";
import { ConnectionError, SqlError } from "../core/errors.js";
import type { Driver, DriverOptions, DriverConnection, AppliedMigrationRow } from "./types.js";

function quoteIdent(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

function qualifyTable(schema: string | undefined, table: string): string {
  const tableIdent = quoteIdent(table);
  if (!schema) {
    return tableIdent;
  }
  return `${quoteIdent(schema)}.${tableIdent}`;
}

function nowExpression(): string {
  return "NOW()";
}

function deriveLockKey(lockKeyHex: string): number {
  const buf = Buffer.from(lockKeyHex, "hex");
  const first = buf.subarray(0, 4);
  const num = first.readUInt32BE(0);
  return (num % 2147483647) + 1;
}

class PostgresConnection implements DriverConnection {
  constructor(
    private readonly client: any,
    private readonly table: string,
    private readonly schema?: string
  ) {}

  private qualifiedTable(): string {
    return qualifyTable(this.schema, this.table);
  }

  async ensureMigrationsTable(): Promise<void> {
    const tableName = this.qualifiedTable();
    await this.client.query(
      `CREATE TABLE IF NOT EXISTS ${tableName} (
        version     BIGINT PRIMARY KEY,
        name        TEXT NOT NULL,
        checksum    TEXT NOT NULL,
        applied_at  TIMESTAMPTZ,
        rolled_back_at TIMESTAMPTZ
      )`,
      []
    );
  }

  async fetchAppliedMigrations(): Promise<AppliedMigrationRow[]> {
    const tableName = this.qualifiedTable();
    const result = await this.client.query(
      `SELECT version, name, checksum, applied_at, rolled_back_at
       FROM ${tableName}
       WHERE applied_at IS NOT NULL
       ORDER BY version ASC`,
      []
    );

    const rows: AppliedMigrationRow[] = (result.rows ?? []).map((row: { version: string | number; name: string; checksum: string; applied_at: string | Date | null; rolled_back_at: string | Date | null }): AppliedMigrationRow => ({
      version: BigInt(row.version),
      name: row.name,
      checksum: row.checksum,
      appliedAt: row.applied_at ? new Date(row.applied_at) : null,
      rolledBackAt: row.rolled_back_at ? new Date(row.rolled_back_at) : null
    }));

    rows.sort((a, b) => (a.version < b.version ? -1 : a.version > b.version ? 1 : 0));
    return rows;
  }

  async markMigrationApplied(input: { version: bigint; name: string; checksum: string }): Promise<void> {
    const tableName = this.qualifiedTable();
    await this.client.query(
      `INSERT INTO ${tableName} (version, name, checksum, applied_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (version) DO UPDATE
       SET applied_at = NOW(), rolled_back_at = NULL`,
      [input.version.toString(), input.name, input.checksum]
    );
  }

  async markMigrationRolledBack(version: bigint): Promise<void> {
    const tableName = this.qualifiedTable();
    await this.client.query(
      `UPDATE ${tableName} SET rolled_back_at = NOW() WHERE version = $1`,
      [version.toString()]
    );
  }

  async acquireLock(lockKey: string, _timeoutMs: number): Promise<boolean> {
    const key = deriveLockKey(lockKey);
    const result = await this.client.query("SELECT pg_try_advisory_lock($1)", [key]);
    return result.rows?.[0]?.pg_try_advisory_lock === true;
  }

  async releaseLock(lockKey: string): Promise<void> {
    const key = deriveLockKey(lockKey);
    await this.client.query("SELECT pg_advisory_unlock($1)", [key]);
  }

  async beginTransaction(): Promise<void> {
    await this.client.query("BEGIN");
  }

  async commitTransaction(): Promise<void> {
    await this.client.query("COMMIT");
  }

  async rollbackTransaction(): Promise<void> {
    await this.client.query("ROLLBACK");
  }

  async runStatement(sql: string): Promise<void> {
    await this.client.query(sql);
  }

  async dispose(): Promise<void> {
    await this.client.release();
  }
}

class PostgresDriver implements Driver {
  private readonly pool: Pool;
  readonly supportsTransactionalDDL = true;

  constructor(private readonly options: DriverOptions) {
    const poolConfig: any = { connectionString: options.url };
    if (options.connectTimeoutMs) {
      poolConfig.connectionTimeoutMillis = options.connectTimeoutMs;
    }
    this.pool = new Pool(poolConfig);
  }

  quoteIdent(identifier: string): string {
    return quoteIdent(identifier);
  }

  nowExpression(): string {
    return nowExpression();
  }

  getPool(): Pool {
    return this.pool;
  }

  async connect(): Promise<DriverConnection> {
    const client = await this.pool.connect();
    return new PostgresConnection(client, this.options.table, this.options.schema);
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  mapError(error: unknown): Error {
    if (error instanceof ConnectionError || error instanceof SqlError) {
      return error;
    }

    const err = error as any;
    const message: string = err?.message || "Unknown database error";
    const code: string | undefined = err?.code;

    if (code && ["ECONNREFUSED", "ENOTFOUND", "ETIMEDOUT", "ENETUNREACH", "ECONNRESET"].includes(code)) {
      return new ConnectionError(`Connection failed: ${message}`);
    }

    if (code && ["28P01", "28000"].includes(code)) {
      return new ConnectionError(`Authentication failed: ${message}`);
    }

    if (code === "3D000" || message.includes("does not exist")) {
      return new ConnectionError(`Database error: ${message}`);
    }

    if (typeof message === "string" && (message.includes("password") || message.includes("authentication"))) {
      return new ConnectionError(`Authentication failed: ${message}`);
    }

    return new SqlError(message);
  }
}

export function createPostgresDriver(options: DriverOptions): Driver {
  return new PostgresDriver(options);
}
