import { createPool, type Pool, type PoolConnection } from "mysql2/promise";
import { ConnectionError, SqlError } from "../core/errors.js";
import type { Driver, DriverConnection, DriverOptions, AppliedMigrationRow } from "./types.js";

function quoteIdent(identifier: string): string {
  return `\`${identifier.replace(/`/g, '``')}\``;
}

function qualifyTable(schema: string | undefined, table: string): string {
  const tableIdent = quoteIdent(table);
  if (!schema) {
    return tableIdent;
  }
  return `${quoteIdent(schema)}.${tableIdent}`;
}

function extractScalar(row: Record<string, any> | undefined): any {
  if (!row) return undefined;
  const keys = Object.keys(row);
  if (keys.length === 0) return undefined;
  const key = keys[0];
  if (key === undefined) return undefined;
  return row[key];
}

class MySqlConnection implements DriverConnection {
  constructor(
    private readonly connection: PoolConnection,
    private readonly table: string,
    private readonly schema?: string
  ) {}

  private qualifiedTable(): string {
    return qualifyTable(this.schema, this.table);
  }

  async ensureMigrationsTable(): Promise<void> {
    const tableName = this.qualifiedTable();
    const sql = `CREATE TABLE IF NOT EXISTS ${tableName} (
      version BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      checksum CHAR(64) NOT NULL,
      applied_at DATETIME(3),
      rolled_back_at DATETIME(3)
    ) ENGINE=InnoDB`;
    await this.connection.execute(sql);
  }

  async fetchAppliedMigrations(): Promise<AppliedMigrationRow[]> {
    const tableName = this.qualifiedTable();
    const [rows] = await this.connection.execute(
      `SELECT version, name, checksum, applied_at, rolled_back_at
       FROM ${tableName}
       WHERE applied_at IS NOT NULL
       ORDER BY version ASC`
    );

    const result = (Array.isArray(rows) ? rows : []) as Array<{
      version: string | number;
      name: string;
      checksum: string;
      applied_at: Date | string | null;
      rolled_back_at: Date | string | null;
    }>;

    const mapped = result.map(row => ({
      version: BigInt(row.version),
      name: row.name,
      checksum: row.checksum,
      appliedAt: row.applied_at ? new Date(row.applied_at) : null,
      rolledBackAt: row.rolled_back_at ? new Date(row.rolled_back_at) : null
    }));

    mapped.sort((a, b) => (a.version < b.version ? -1 : a.version > b.version ? 1 : 0));
    return mapped;
  }

  async markMigrationApplied(input: { version: bigint; name: string; checksum: string }): Promise<void> {
    const tableName = this.qualifiedTable();
    const sql = `INSERT INTO ${tableName} (version, name, checksum, applied_at)
      VALUES (?, ?, ?, CURRENT_TIMESTAMP(3))
      ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        checksum = VALUES(checksum),
        applied_at = CURRENT_TIMESTAMP(3),
        rolled_back_at = NULL`;
    await this.connection.execute(sql, [input.version.toString(), input.name, input.checksum]);
  }

  async markMigrationRolledBack(version: bigint): Promise<void> {
    const tableName = this.qualifiedTable();
    const sql = `UPDATE ${tableName}
      SET rolled_back_at = CURRENT_TIMESTAMP(3)
      WHERE version = ?`;
    await this.connection.execute(sql, [version.toString()]);
  }

  async acquireLock(lockKey: string, timeoutMs: number): Promise<boolean> {
    const seconds = Math.max(0, Math.ceil(timeoutMs / 1000));
    const [rows] = await this.connection.execute(`SELECT GET_LOCK(?, ?)`, [lockKey, seconds]);
    const firstRow = (Array.isArray(rows) ? rows[0] : undefined) as Record<string, any> | undefined;
    const value = extractScalar(firstRow);
    return value === 1;
  }

  async releaseLock(lockKey: string): Promise<void> {
    await this.connection.execute(`SELECT RELEASE_LOCK(?)`, [lockKey]);
  }

  async beginTransaction(): Promise<void> {
    await this.connection.beginTransaction();
  }

  async commitTransaction(): Promise<void> {
    await this.connection.commit();
  }

  async rollbackTransaction(): Promise<void> {
    await this.connection.rollback();
  }

  async query<T = unknown>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> {
    const [rows] = await this.connection.execute(sql, params);
    return { rows: (Array.isArray(rows) ? rows : []) as T[] };
  }

  async runStatement(sql: string): Promise<void> {
    await this.connection.execute(sql);
  }

  async dispose(): Promise<void> {
    await this.connection.release();
  }
}

class MySqlDriver implements Driver {
  readonly supportsTransactionalDDL = false;
  private readonly pool: Pool;
  private readonly ownsPool: boolean;

  constructor(private readonly options: DriverOptions) {
    if (options.pool) {
      this.pool = options.pool as unknown as Pool;
      this.ownsPool = false;
    } else {
      const pool = createPool({
        uri: options.url,
        waitForConnections: true,
        multipleStatements: false,
        charset: "UTF8MB4",
        timezone: "Z",
        connectTimeout: options.connectTimeoutMs,
        namedPlaceholders: true,
        supportBigNumbers: true,
        bigNumberStrings: true
      });
      this.pool = pool;
      this.ownsPool = true;
    }
  }

  quoteIdent(identifier: string): string {
    return quoteIdent(identifier);
  }

  nowExpression(): string {
    return "CURRENT_TIMESTAMP(3)";
  }

  async probeConnection(): Promise<void> {
    try {
      const connection = await this.pool.getConnection();
      try {
        await connection.execute("SELECT 1");
      } finally {
        connection.release();
      }
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async connect(): Promise<DriverConnection> {
    const connection = await this.pool.getConnection();
    return new MySqlConnection(connection, this.options.table, this.options.schema);
  }

  async close(): Promise<void> {
    if (this.ownsPool) {
      await this.pool.end();
    }
  }

  mapError(error: unknown): Error {
    if (error instanceof ConnectionError || error instanceof SqlError) {
      return error;
    }

    const err = error as any;
    const code: string | undefined = err?.code;
    const message: string = err?.message || "Unknown database error";

    const connectionCodes = new Set([
      "ECONNREFUSED",
      "ENOTFOUND",
      "ETIMEDOUT",
      "ECONNRESET",
      "PROTOCOL_CONNECTION_LOST",
      "ER_ACCESS_DENIED_ERROR",
      "ER_BAD_DB_ERROR"
    ]);

    if (code && connectionCodes.has(code)) {
      return new ConnectionError(message);
    }

    const lower = message.toLowerCase();
    if (lower.includes("access denied") || lower.includes("cannot connect") || lower.includes("connect")) {
      return new ConnectionError(message);
    }

    const sqlCodes = new Set([
      "ER_PARSE_ERROR",
      "ER_DUP_ENTRY",
      "ER_TRUNCATED_WRONG_VALUE",
      "ER_NO_SUCH_TABLE"
    ]);

    if (code && sqlCodes.has(code)) {
      return new SqlError(message);
    }

    return new SqlError(message);
  }
}

export function createMySqlDriver(options: DriverOptions): Driver {
  return new MySqlDriver(options);
}
