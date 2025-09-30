import { mkdirSync } from "node:fs";
import { dirname, resolve as resolvePath } from "node:path";
import { fileURLToPath } from "node:url";
import { ConnectionError, SqlError, NomadError } from "../core/errors.js";
import type { AppliedMigrationRow, Driver, DriverConnection } from "./types.js";

interface SqliteDriverOptions {
  url: string;
  table: string;
  connectTimeoutMs?: number;
}

const ISO_TIMESTAMP_EXPR = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";
const LOCK_TABLE_SQL =
  "CREATE TABLE IF NOT EXISTS nomad_lock (lock_name TEXT PRIMARY KEY, acquired_at TEXT NOT NULL)";
const LOCK_INSERT_SQL =
  "INSERT OR IGNORE INTO nomad_lock(lock_name, acquired_at) VALUES (?, " + ISO_TIMESTAMP_EXPR + ")";
const LOCK_DELETE_SQL = "DELETE FROM nomad_lock WHERE lock_name = ?";
const DEFAULT_BUSY_TIMEOUT = 5000;

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

export function resolveSqliteFilename(url?: string): { filename: string; isMemory: boolean } {
  if (!url) {
    return { filename: ":memory:", isMemory: true };
  }

  const trimmed = url.trim();
  if (!trimmed || trimmed === ":memory:" || trimmed === "sqlite::memory:" || trimmed === "sqlite::memory") {
    return { filename: ":memory:", isMemory: true };
  }

  if (trimmed.startsWith("sqlite://")) {
    try {
      return { filename: fileURLToPath(trimmed.replace(/^sqlite:/, "file:")), isMemory: false };
    } catch {
      // fall through
    }
  }

  if (trimmed.startsWith("file:")) {
    try {
      return { filename: fileURLToPath(trimmed), isMemory: false };
    } catch {
      // fall through
    }
  }

  if (trimmed.startsWith("sqlite:")) {
    const rest = trimmed.slice("sqlite:".length);
    if (!rest || rest === ":memory:" || rest === "memory") {
      return { filename: ":memory:", isMemory: true };
    }
    if (rest.startsWith("//")) {
      try {
        return { filename: fileURLToPath("file:" + rest), isMemory: false };
      } catch {
        // fall through
      }
    }
    return { filename: resolvePath(process.cwd(), rest), isMemory: false };
  }

  if (!trimmed.includes("://")) {
    return { filename: resolvePath(process.cwd(), trimmed), isMemory: false };
  }

  // Unknown scheme; treat as file path relative to cwd
  return { filename: resolvePath(process.cwd(), trimmed), isMemory: false };
}

function mapSqliteRow(row: any): AppliedMigrationRow {
  return {
    version: BigInt(row.version),
    name: row.name,
    checksum: row.checksum,
    appliedAt: row.applied_at ? new Date(row.applied_at) : null,
    rolledBackAt: row.rolled_back_at ? new Date(row.rolled_back_at) : null
  };
}


type SqliteDatabase = {
  exec(sql: string): unknown;
  prepare(sql: string): {
    all: (...params: any[]) => any[];
    run: (...params: any[]) => { changes?: number };
  };
  close: () => void;
};

type SqliteDatabaseConstructor = new (filename: string) => SqliteDatabase;


type BetterSqliteModule = {
  default: SqliteDatabaseConstructor;
};

type NodeSqliteModule = {
  DatabaseSync: SqliteDatabaseConstructor;
};

let sqliteCtorPromise: Promise<SqliteDatabaseConstructor> | undefined;

async function loadSqliteConstructor(): Promise<SqliteDatabaseConstructor> {
  if (!sqliteCtorPromise) {
    sqliteCtorPromise = (async () => {
      try {
        const betterSqliteModule = (await import("better-sqlite3")) as unknown as BetterSqliteModule;
        if (betterSqliteModule && typeof betterSqliteModule.default === "function") {
          return betterSqliteModule.default;
        }
      } catch {
        // Intentionally fall through to node:sqlite fallback
      }

      try {
        const nodeSqliteModule = (await import("node:sqlite")) as unknown as NodeSqliteModule;
        if (nodeSqliteModule && typeof nodeSqliteModule.DatabaseSync === "function") {
          return nodeSqliteModule.DatabaseSync;
        }
      } catch {
        // Ignore and surface a unified error below
      }

      throw new ConnectionError(
        "SQLite driver requires the better-sqlite3 package (fallback to node:sqlite failed)"
      );
    })();
  }

  return sqliteCtorPromise;
}


export function createSqliteDriver(options: SqliteDriverOptions): Driver {
  const { filename, isMemory } = resolveSqliteFilename(options.url);
  const busyTimeout = options.connectTimeoutMs ?? DEFAULT_BUSY_TIMEOUT;
  const quotedTable = quoteIdentifier(options.table);
  const ensureMigrationsSql =
    `CREATE TABLE IF NOT EXISTS ${quotedTable} (` +
    "version INTEGER PRIMARY KEY, " +
    "name TEXT NOT NULL, " +
    "checksum TEXT NOT NULL, " +
    "applied_at TEXT, " +
    "rolled_back_at TEXT" +
    ")";
  const selectAppliedSql = `SELECT version, name, checksum, applied_at, rolled_back_at FROM ${quotedTable} ORDER BY version ASC`;
  const insertAppliedSql =
    `INSERT INTO ${quotedTable} (version, name, checksum, applied_at, rolled_back_at) ` +
    `VALUES (?, ?, ?, ${ISO_TIMESTAMP_EXPR}, NULL) ` +
    `ON CONFLICT(version) DO UPDATE SET name = excluded.name, checksum = excluded.checksum, applied_at = ${ISO_TIMESTAMP_EXPR}, rolled_back_at = NULL`;
  const markRolledBackSql = `UPDATE ${quotedTable} SET rolled_back_at = ${ISO_TIMESTAMP_EXPR} WHERE version = ?`;

  const connectionErrorCodes = new Set([
    "SQLITE_CANTOPEN",
    "SQLITE_IOERR",
    "SQLITE_BUSY",
    "SQLITE_LOCKED",
    "SQLITE_AUTH",
    "SQLITE_PERM",
    "SQLITE_NOTADB"
  ]);

  const mapError = (error: unknown): Error => {
    if (error instanceof NomadError) return error;
    if (error instanceof ConnectionError || error instanceof SqlError) return error;
    const err = error as { code?: string; message?: string } | undefined;
    const message = err?.message ?? String(error);
    const code = err?.code;
    if (code && connectionErrorCodes.has(code)) {
      return new ConnectionError(message);
    }
    if (/unable to open database/i.test(message) || /database is locked/i.test(message)) {
      return new ConnectionError(message);
    }
    return new SqlError(message);
  };

  const createConnection = async (): Promise<DriverConnection> => {
    try {
      if (!isMemory) {
        mkdirSync(dirname(filename), { recursive: true });
      }
      const DatabaseCtor = await loadSqliteConstructor();
      const db = new DatabaseCtor(filename);
      db.exec(`PRAGMA busy_timeout = ${busyTimeout}`);
      db.exec("PRAGMA foreign_keys = ON");
      if (!isMemory) {
        try {
          db.exec("PRAGMA journal_mode = WAL");
        } catch {
          // Ignore failures to change journal mode
        }
      }
      db.exec(LOCK_TABLE_SQL);

      let migrationsEnsured = false;
      let inTransaction = false;
      const heldLocks = new Set<string>();

      const ensureMigrations = () => {
        if (!migrationsEnsured) {
          db.exec(ensureMigrationsSql);
          migrationsEnsured = true;
        }
      };

      const connection: DriverConnection = {
        async ensureMigrationsTable(): Promise<void> {
          ensureMigrations();
        },

        async fetchAppliedMigrations(): Promise<AppliedMigrationRow[]> {
          ensureMigrations();
          const rows = db.prepare(selectAppliedSql).all();
          return rows.map(mapSqliteRow);
        },

        async markMigrationApplied(input: { version: bigint; name: string; checksum: string }): Promise<void> {
          ensureMigrations();
          db.prepare(insertAppliedSql).run(Number(input.version), input.name, input.checksum);
        },

        async markMigrationRolledBack(version: bigint): Promise<void> {
          ensureMigrations();
          db.prepare(markRolledBackSql).run(Number(version));
        },

        async acquireLock(lockKey: string, _timeoutMs: number): Promise<boolean> {
          if (heldLocks.has(lockKey)) {
            return true;
          }
          const info = db.prepare(LOCK_INSERT_SQL).run(lockKey);
          if (info.changes && info.changes > 0) {
            heldLocks.add(lockKey);
            return true;
          }
          return false;
        },

        async releaseLock(lockKey: string): Promise<void> {
          if (!heldLocks.has(lockKey)) {
            return;
          }
          db.prepare(LOCK_DELETE_SQL).run(lockKey);
          heldLocks.delete(lockKey);
        },

        async beginTransaction(): Promise<void> {
          if (inTransaction) return;
          db.exec("BEGIN IMMEDIATE");
          inTransaction = true;
        },

        async commitTransaction(): Promise<void> {
          if (!inTransaction) return;
          db.exec("COMMIT");
          inTransaction = false;
        },

        async rollbackTransaction(): Promise<void> {
          if (!inTransaction) return;
          db.exec("ROLLBACK");
          inTransaction = false;
        },

        async query<T = unknown>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> {
          const statement = db.prepare(sql);
          const rows = params.length > 0 ? statement.all(...(params as any[])) : statement.all();
          return { rows: rows as T[] };
        },

        async runStatement(sql: string): Promise<void> {
          db.exec(sql);
        },

        async dispose(): Promise<void> {
          if (inTransaction) {
            try {
              db.exec("ROLLBACK");
            } catch {
              // ignore rollback failure during cleanup
            }
            inTransaction = false;
          }
          if (heldLocks.size > 0) {
            const release = db.prepare(LOCK_DELETE_SQL);
            for (const lock of heldLocks) {
              release.run(lock);
            }
            heldLocks.clear();
          }
          db.close();
        }
      };

      return connection;
    } catch (error) {
      throw mapError(error);
    }
  };

  const driver: Driver = {
    supportsTransactionalDDL: false, // SQLite supports transactional DDL, but we align with MySQL to avoid mixed behaviour

    async connect(): Promise<DriverConnection> {
      return createConnection();
    },

    async close(): Promise<void> {
      // Connections are closed via DriverConnection.dispose()
    },

    quoteIdent(identifier: string): string {
      return quoteIdentifier(identifier);
    },

    nowExpression(): string {
      return "CURRENT_TIMESTAMP";
    },

    mapError,

    async probeConnection(): Promise<void> {
      let connection: DriverConnection | undefined;
      try {
        connection = await createConnection();
        await connection.query("SELECT 1");
      } catch (error) {
        throw mapError(error);
      } finally {
        if (connection) {
          await connection.dispose();
        }
      }
    }
  };

  return driver;
}
