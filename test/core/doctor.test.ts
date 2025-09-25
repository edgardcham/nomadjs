import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Pool } from "pg";
import type { Config } from "../../src/config.js";
import { runDoctor } from "../../src/core/doctor.js";

const baseConfig: Config = {
  driver: "postgres",
  url: "postgres://postgres@localhost/nomaddb",
  dir: "migrations",
  table: "nomad_migrations",
  schema: "public"
};

function createPool(handler: (sql: string, params?: unknown[]) => Promise<any>): Pool {
  return {
    query: vi.fn(handler)
  } as unknown as Pool;
}

describe("runDoctor", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("flags connection failures", async () => {
    const pool = createPool(async () => {
      throw new Error("ECONNREFUSED");
    });

    const report = await runDoctor(baseConfig, pool, {});

    expect(report.ok).toBe(false);
    const connectCheck = report.checks.find(check => check.id === "connect");
    expect(connectCheck?.status).toBe("fail");
  });

  it("fails when schema is missing without fix", async () => {
    const pool = createPool(async (sql) => {
      if (sql.includes("SELECT version()")) {
        return {
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        };
      }
      if (sql.includes("information_schema.schemata")) {
        return { rows: [] };
      }
      if (sql.includes("information_schema.tables")) {
        return { rows: [] };
      }
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ acquired: true }] };
      }
      if (sql.includes("pg_advisory_unlock")) {
        return { rows: [{ pg_advisory_unlock: true }] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    });

    const report = await runDoctor({ ...baseConfig, schema: "app" }, pool, {});

    expect(report.ok).toBe(false);
    const schemaCheck = report.checks.find(check => check.id === "schema");
    expect(schemaCheck?.status).toBe("fail");
    expect(schemaCheck?.message).toMatch(/does not exist/i);
  });

  it("creates missing schema when fix option enabled", async () => {
    const queries: string[] = [];
    const pool = createPool(async (sql) => {
      queries.push(sql);
      if (sql.includes("SELECT version()")) {
        return {
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        };
      }
      if (sql.includes("information_schema.schemata")) {
        // First check returns empty, second check after fix returns row
        const invocation = queries.filter(q => q.includes("information_schema.schemata")).length;
        if (invocation === 1) {
          return { rows: [] };
        }
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("information_schema.tables")) {
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ acquired: true }] };
      }
      if (sql.includes("pg_advisory_unlock")) {
        return { rows: [{ pg_advisory_unlock: true }] };
      }
      if (sql.startsWith("CREATE SCHEMA")) {
        return { rows: [] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    });

    const report = await runDoctor({ ...baseConfig, schema: "app" }, pool, { fix: true });

    const schemaCheck = report.checks.find(check => check.id === "schema");
    expect(schemaCheck?.status).toBe("pass");
    expect(queries.some(sql => sql.startsWith("CREATE SCHEMA"))).toBe(true);
    expect(report.ok).toBe(true);
  });

  it("warns when migrations table is missing", async () => {
    const pool = createPool(async (sql) => {
      if (sql.includes("SELECT version()")) {
        return {
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        };
      }
      if (sql.includes("information_schema.schemata")) {
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("information_schema.tables")) {
        return { rows: [] };
      }
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ acquired: true }] };
      }
      if (sql.includes("pg_advisory_unlock")) {
        return { rows: [{ pg_advisory_unlock: true }] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    });

    const report = await runDoctor(baseConfig, pool, {});

    const tableCheck = report.checks.find(check => check.id === "migrations-table");
    expect(tableCheck?.status).toBe("warn");
    expect(tableCheck?.message).toMatch(/does not exist/i);
    expect(report.ok).toBe(true);
  });

  it("creates migrations table when fix option enabled", async () => {
    const queries: string[] = [];
    const pool = createPool(async (sql) => {
      queries.push(sql);
      if (sql.includes("SELECT version()")) {
        return {
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        };
      }
      if (sql.includes("information_schema.schemata")) {
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("information_schema.tables")) {
        const invocation = queries.filter(q => q.includes("information_schema.tables")).length;
        if (invocation === 1) {
          return { rows: [] };
        }
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ acquired: true }] };
      }
      if (sql.includes("pg_advisory_unlock")) {
        return { rows: [{ pg_advisory_unlock: true }] };
      }
      if (sql.startsWith("CREATE TABLE")) {
        return { rows: [] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    });

    const report = await runDoctor(baseConfig, pool, { fix: true });

    const tableCheck = report.checks.find(check => check.id === "migrations-table");
    expect(tableCheck?.status).toBe("pass");
    expect(queries.some(sql => sql.startsWith("CREATE TABLE"))).toBe(true);
    expect(report.ok).toBe(true);
  });

  it("fails when advisory lock cannot be acquired", async () => {
    const pool = createPool(async (sql) => {
      if (sql.includes("SELECT version()")) {
        return {
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        };
      }
      if (sql.includes("information_schema.schemata")) {
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("information_schema.tables")) {
        return { rows: [{ exists: 1 }] };
      }
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ pg_try_advisory_lock: false }] };
      }
      if (sql.includes("pg_advisory_unlock")) {
        return { rows: [{ pg_advisory_unlock: true }] };
      }
      if (sql.includes("BEGIN")) {
        return { rows: [] };
      }
      if (sql.includes("ROLLBACK")) {
        return { rows: [] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    });

    const report = await runDoctor(baseConfig, pool, {});

    const lockCheck = report.checks.find(check => check.id === "advisory-lock");
    expect(lockCheck?.status).toBe("fail");
    expect(lockCheck?.message).toMatch(/another migration may be in progress|could not acquire/i);
    expect(report.ok).toBe(false);
  });
});
