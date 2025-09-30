import { Pool } from "pg";
import { ConnectionError, ParseConfigError, SqlError } from "../core/errors.js";
import type { Driver, DriverOptions, DriverConnection, AppliedMigrationRow, PoolLike } from "./types.js";

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

type PoolClientLike = {
  query(sql: string, params?: unknown[]): Promise<any>;
  release(): Promise<void> | void;
};

class PostgresConnection implements DriverConnection {
  constructor(
    private readonly client: PoolClientLike,
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

    const rows: AppliedMigrationRow[] = (result.rows ?? []).map((row: {
      version: string | number;
      name: string;
      checksum: string;
      applied_at: string | Date | null;
      rolled_back_at: string | Date | null;
    }): AppliedMigrationRow => ({
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

  async query<T = unknown>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> {
    const result = await this.client.query(sql, params);
    return { rows: (result?.rows ?? []) as T[] };
  }

  async runStatement(sql: string): Promise<void> {
    await this.client.query(sql);
  }

  async dispose(): Promise<void> {
    await this.client.release();
  }
}

class PostgresDriver implements Driver {
  private readonly pool: PoolLike;
  readonly supportsTransactionalDDL = true;

  private readonly ownsPool: boolean;

  constructor(private readonly options: DriverOptions) {
    if (options.pool) {
      this.pool = options.pool;
      this.ownsPool = false;
    } else {
      const poolConfig: Record<string, unknown> = { connectionString: options.url };
      if (options.connectTimeoutMs) {
        poolConfig.connectionTimeoutMillis = options.connectTimeoutMs;
      }
      try {
        const pool = new Pool(poolConfig);
        this.pool = pool as unknown as PoolLike;
        this.ownsPool = true;
      } catch (error) {
        throw this.mapError(error);
      }
    }
  }

  quoteIdent(identifier: string): string {
    return quoteIdent(identifier);
  }

  nowExpression(): string {
    return nowExpression();
  }

  async probeConnection(): Promise<void> {
    try {
      await this.pool.query("SELECT 1");
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async connect(): Promise<DriverConnection> {
    try {
      const client = await this.pool.connect();
      return new PostgresConnection(client, this.options.table, this.options.schema);
    } catch (error) {
      throw this.mapError(error);
    }
  }

  async close(): Promise<void> {
    if (this.ownsPool) {
      await this.pool.end();
    }
  }

  mapError(error: unknown): Error {
    if (error instanceof ConnectionError || error instanceof SqlError || error instanceof ParseConfigError) {
      return error;
    }

    const err = error as any;
    const message = typeof err?.message === "string" ? err.message : "Unknown database error";
    const lowerMessage = message.toLowerCase();

    const codes = new Set<string>();
    const addCode = (value: unknown) => {
      if (typeof value === "string" && value.length > 0) {
        codes.add(value);
      }
    };

    addCode(err?.code);
    addCode(err?.errno);
    addCode(err?.original?.code);
    addCode(err?.original?.errno);
    addCode(err?.cause?.code);
    addCode(err?.cause?.errno);

    const connectionCodes = new Set([
      "ECONNREFUSED",
      "ENOTFOUND",
      "EAI_AGAIN",
      "ETIMEDOUT",
      "ENETUNREACH",
      "EHOSTUNREACH",
      "ECONNRESET",
      "ECONNABORTED",
      "EPIPE",
      "ERR_SOCKET_BAD_PORT",
      "EPERM",
      "EACCES",
      "57P03",
      "08001",
      "08004",
      "08006"
    ]);

    const hasConnectionCode = [...codes].some(code => connectionCodes.has(code));

    const indicatesBadPort =
      codes.has("ERR_SOCKET_BAD_PORT") ||
      lowerMessage.includes("invalid port") ||
      (lowerMessage.includes("port number") && (lowerMessage.includes("range") || lowerMessage.includes("out of range"))) ||
      lowerMessage.includes("port should be") ||
      lowerMessage.includes("port must be") ||
      lowerMessage.includes("searchparams") ||
      lowerMessage.includes("operation not permitted") ||
      lowerMessage.includes("permission denied");

    const connectionByMessage =
      lowerMessage.includes("getaddrinfo") ||
      lowerMessage.includes("connection refused") ||
      lowerMessage.includes("could not connect to server") ||
      (lowerMessage.includes("timeout") && (lowerMessage.includes("connect") || lowerMessage.includes("connection"))) ||
      lowerMessage.includes("no such host") ||
      lowerMessage.includes("failed to resolve") ||
      lowerMessage.includes("server closed the connection") ||
      lowerMessage.includes("connection terminated unexpectedly");

    if (hasConnectionCode || indicatesBadPort || connectionByMessage) {
      return new ConnectionError(`Connection failed: ${message}`);
    }

    if (codes.has("28P01") || codes.has("28000")) {
      return new ConnectionError(`Authentication failed: ${message}`);
    }

    if (codes.has("3D000") || lowerMessage.includes("does not exist")) {
      return new ConnectionError(`Database error: ${message}`);
    }

    if (lowerMessage.includes("password") || lowerMessage.includes("authentication")) {
      return new ConnectionError(`Authentication failed: ${message}`);
    }

    const parseConfigIndicators =
      err instanceof SyntaxError ||
      lowerMessage.includes("invalid uri") ||
      lowerMessage.includes("invalid url") ||
      lowerMessage.includes("invalid connection string") ||
      lowerMessage.includes("malformed");

    if (parseConfigIndicators) {
      return new ParseConfigError(`Invalid connection URL: ${message}`);
    }

    return new SqlError(message);
  }
}

export function createPostgresDriver(options: DriverOptions): Driver {
  return new PostgresDriver(options);
}
