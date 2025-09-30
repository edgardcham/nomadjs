import { createHash } from "crypto";
import type { Config } from "../config.js";
import type { Driver } from "../driver/types.js";

export type DoctorStatus = "pass" | "warn" | "fail";

export interface DoctorCheck {
  id: string;
  title: string;
  status: DoctorStatus;
  message: string;
  suggestions?: string[];
}

export interface DoctorReportSummary {
  pass: number;
  warn: number;
  fail: number;
}

export interface DoctorReport {
  ok: boolean;
  summary: DoctorReportSummary;
  checks: DoctorCheck[];
  config?: {
    url?: string;
    schema: string;
    table: string;
    dir: string;
  };
  environment?: {
    server?: {
      version?: string;
      database?: string;
      user?: string;
      timezone?: string;
      encoding?: string;
    };
  };
}

export interface DoctorOptions {
  fix?: boolean;
}

function makeSummary(): DoctorReportSummary {
  return { pass: 0, warn: 0, fail: 0 };
}

function record(checks: DoctorCheck[], summary: DoctorReportSummary, check: DoctorCheck): void {
  checks.push(check);
  summary[check.status] += 1;
}

function computeLockKey(config: Config): string {
  const parts = [
    config.url || "",
    config.schema || "public",
    config.table || "nomad_migrations",
    config.dir || "migrations"
  ];
  const combined = parts.join("|");
  return createHash("sha256").update(combined).digest("hex");
}

export async function runDoctor(config: Config, driver: Driver, options: DoctorOptions = {}): Promise<DoctorReport> {
  const summary = makeSummary();
  const checks: DoctorCheck[] = [];
  const schemaName = config.schema || "public";
  const tableName = config.table || "nomad_migrations";
  const report: DoctorReport = {
    ok: true,
    summary,
    checks,
    config: {
      url: config.url,
      schema: schemaName,
      table: tableName,
      dir: config.dir || "migrations"
    }
  };

  const quote = (value: string) => driver.quoteIdent(value);
  const lockTimeout = config.lockTimeout ?? 30000;
  const connection = await driver.connect();

  try {
    // Connectivity check with server info collection
    let serverInfo: any;
    try {
      const result = await connection.query(
        "SELECT version(), current_database(), current_user, current_setting('TimeZone') AS timezone, current_setting('server_encoding') AS encoding"
      );
      serverInfo = result.rows?.[0];
      record(checks, summary, {
        id: "connect",
        title: "Database connection",
        status: "pass",
        message: `Connected to ${serverInfo?.current_database ?? "database"} as ${serverInfo?.current_user ?? "unknown"}`
      });
      report.environment = {
        server: {
          version: serverInfo?.version,
          database: serverInfo?.current_database,
          user: serverInfo?.current_user,
          timezone: serverInfo?.timezone,
          encoding: serverInfo?.encoding
        }
      };
    } catch (error) {
      record(checks, summary, {
        id: "connect",
        title: "Database connection",
        status: "fail",
        message: `Failed to connect: ${(error as Error).message}`
      });
      report.ok = false;
      return report;
    }

    // Schema exists check
    try {
      const schemaResult = await connection.query(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
        [schemaName]
      );

      if (!schemaResult.rows || schemaResult.rows.length === 0) {
        if (options.fix) {
          await connection.runStatement(`CREATE SCHEMA IF NOT EXISTS ${quote(schemaName)}`);
          const recheck = await connection.query(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = $1",
            [schemaName]
          );
          if (!recheck.rows || recheck.rows.length === 0) {
            record(checks, summary, {
              id: "schema",
              title: "Schema availability",
              status: "fail",
              message: `Schema "${schemaName}" does not exist and could not be created`
            });
            report.ok = false;
          } else {
            record(checks, summary, {
              id: "schema",
              title: "Schema availability",
              status: "pass",
              message: `Schema "${schemaName}" created`
            });
          }
        } else {
          record(checks, summary, {
            id: "schema",
            title: "Schema availability",
            status: "fail",
            message: `Schema "${schemaName}" does not exist`
          });
          report.ok = false;
        }
      } else {
        record(checks, summary, {
          id: "schema",
          title: "Schema availability",
          status: "pass",
          message: `Schema "${schemaName}" exists`
        });
      }
    } catch (error) {
      record(checks, summary, {
        id: "schema",
        title: "Schema availability",
        status: "fail",
        message: `Failed to verify schema "${schemaName}": ${(error as Error).message}`
      });
      report.ok = false;
    }

    // Migrations table check (warn if missing)
    try {
      const tableResult = await connection.query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
        [schemaName, tableName]
      );

      if (!tableResult.rows || tableResult.rows.length === 0) {
        if (options.fix) {
          await connection.runStatement(`CREATE TABLE IF NOT EXISTS ${quote(schemaName)}.${quote(tableName)} (
  version     BIGINT PRIMARY KEY,
  name        TEXT NOT NULL,
  checksum    TEXT NOT NULL,
  applied_at  TIMESTAMPTZ,
  rolled_back_at TIMESTAMPTZ
)`);
          const recheck = await connection.query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
            [schemaName, tableName]
          );
          if (!recheck.rows || recheck.rows.length === 0) {
            record(checks, summary, {
              id: "migrations-table",
              title: "Migrations table",
              status: "fail",
              message: `Version table "${schemaName}"."${tableName}" does not exist and could not be created`
            });
            report.ok = false;
          } else {
            record(checks, summary, {
              id: "migrations-table",
              title: "Migrations table",
              status: "pass",
              message: `Version table "${schemaName}"."${tableName}" created`
            });
          }
        } else {
          record(checks, summary, {
            id: "migrations-table",
            title: "Migrations table",
            status: "warn",
            message: `Version table "${schemaName}"."${tableName}" does not exist`
          });
        }
      } else {
        record(checks, summary, {
          id: "migrations-table",
          title: "Migrations table",
          status: "pass",
          message: `Version table "${schemaName}"."${tableName}" exists`
        });
      }
    } catch (error) {
      record(checks, summary, {
        id: "migrations-table",
        title: "Migrations table",
        status: "fail",
        message: `Failed to verify version table: ${(error as Error).message}`
      });
      report.ok = false;
    }

    // Advisory lock check
    try {
      const lockKey = computeLockKey(config);
      const acquired = await connection.acquireLock(lockKey, lockTimeout);
      if (!acquired) {
        record(checks, summary, {
          id: "advisory-lock",
          title: "Advisory lock",
          status: "fail",
          message: "Could not acquire migration advisory lock"
        });
        report.ok = false;
      } else {
        await connection.releaseLock(lockKey);
        record(checks, summary, {
          id: "advisory-lock",
          title: "Advisory lock",
          status: "pass",
          message: "Advisory lock acquired and released successfully"
        });
      }
    } catch (error) {
      record(checks, summary, {
        id: "advisory-lock",
        title: "Advisory lock",
        status: "fail",
        message: `Failed to verify advisory lock: ${(error as Error).message}`
      });
      report.ok = false;
    }
  } finally {
    await connection.dispose();
  }

  return report;
}
