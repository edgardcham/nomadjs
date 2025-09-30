import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Config } from "../../src/config.js";
import { runDoctor } from "../../src/core/doctor.js";
import { createDriverMock, type DriverMock, type DriverConnectionMock } from "../helpers/driver-mock.js";

const baseConfig: Config = {
  driver: "postgres",
  url: "postgres://postgres@localhost/nomaddb",
  dir: "migrations",
  table: "nomad_migrations",
  schema: "public"
};

describe("runDoctor", () => {
  let driver: DriverMock;

  beforeEach(() => {
    vi.restoreAllMocks();
    driver = createDriverMock();
  });

  function setupConnection(overrides: Partial<DriverConnectionMock>): DriverConnectionMock {
    return driver.enqueueConnection(overrides);
  }

  it("flags connection failures", async () => {
    setupConnection({
      query: vi.fn().mockRejectedValue(new Error("ECONNREFUSED"))
    });

    const report = await runDoctor(baseConfig, driver, {});

    expect(report.ok).toBe(false);
    const connectCheck = report.checks.find(check => check.id === "connect");
    expect(connectCheck?.status).toBe("fail");
  });

  it("fails when schema is missing without fix", async () => {
    const queryMock = vi.fn().mockImplementation((sql: string) => {
      if (sql.includes("SELECT version()")) {
        return Promise.resolve({
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        });
      }
      if (sql.includes("information_schema.schemata")) {
        return Promise.resolve({ rows: [] });
      }
      if (sql.includes("information_schema.tables")) {
        return Promise.resolve({ rows: [] });
      }
      return Promise.resolve({ rows: [] });
    });

    setupConnection({ query: queryMock });

    const report = await runDoctor({ ...baseConfig, schema: "app" }, driver, {});

    expect(report.ok).toBe(false);
    const schemaCheck = report.checks.find(check => check.id === "schema");
    expect(schemaCheck?.status).toBe("fail");
    expect(schemaCheck?.message).toMatch(/does not exist/i);
  });

  it("creates missing schema when fix option enabled", async () => {
    const queries: string[] = [];
    const queryMock = vi.fn().mockImplementation((sql: string) => {
      queries.push(sql);
      if (sql.includes("SELECT version()")) {
        return Promise.resolve({
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        });
      }
      if (sql.includes("information_schema.schemata")) {
        const invocation = queries.filter(q => q.includes("information_schema.schemata")).length;
        if (invocation === 1) {
          return Promise.resolve({ rows: [] });
        }
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      if (sql.includes("information_schema.tables")) {
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      return Promise.resolve({ rows: [] });
    });

    const runStatementMock = vi.fn().mockImplementation((sql: string) => {
      queries.push(sql);
      return Promise.resolve();
    });

    setupConnection({ query: queryMock, runStatement: runStatementMock });

    const report = await runDoctor({ ...baseConfig, schema: "app" }, driver, { fix: true });

    const schemaCheck = report.checks.find(check => check.id === "schema");
    expect(schemaCheck?.status).toBe("pass");
    expect(queries.some(sql => sql.startsWith("CREATE SCHEMA"))).toBe(true);
    expect(report.ok).toBe(true);
  });

  it("warns when migrations table is missing", async () => {
    const queryMock = vi.fn().mockImplementation((sql: string) => {
      if (sql.includes("SELECT version()")) {
        return Promise.resolve({
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        });
      }
      if (sql.includes("information_schema.schemata")) {
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      if (sql.includes("information_schema.tables")) {
        return Promise.resolve({ rows: [] });
      }
      return Promise.resolve({ rows: [] });
    });

    setupConnection({ query: queryMock });

    const report = await runDoctor(baseConfig, driver, {});

    const tableCheck = report.checks.find(check => check.id === "migrations-table");
    expect(tableCheck?.status).toBe("warn");
    expect(tableCheck?.message).toMatch(/does not exist/i);
    expect(report.ok).toBe(true);
  });

  it("creates migrations table when fix option enabled", async () => {
    const queries: string[] = [];
    const queryMock = vi.fn().mockImplementation((sql: string) => {
      queries.push(sql);
      if (sql.includes("SELECT version()")) {
        return Promise.resolve({
          rows: [{
            version: "PostgreSQL 15.4",
            current_database: "nomaddb",
            current_user: "postgres",
            timezone: "UTC",
            encoding: "UTF8"
          }]
        });
      }
      if (sql.includes("information_schema.schemata")) {
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      if (sql.includes("information_schema.tables")) {
        const invocation = queries.filter(q => q.includes("information_schema.tables")).length;
        if (invocation === 1) {
          return Promise.resolve({ rows: [] });
        }
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      return Promise.resolve({ rows: [] });
    });

    const runStatementMock = vi.fn().mockImplementation((sql: string) => {
      queries.push(sql);
      return Promise.resolve();
    });

    setupConnection({ query: queryMock, runStatement: runStatementMock });

    const report = await runDoctor(baseConfig, driver, { fix: true });

    const tableCheck = report.checks.find(check => check.id === "migrations-table");
    expect(tableCheck?.status).toBe("pass");
    expect(queries.some(sql => sql.startsWith("CREATE TABLE"))).toBe(true);
    expect(report.ok).toBe(true);
  });

  it("fails advisory lock when acquire returns false", async () => {
    const queryMock = vi.fn().mockImplementation((sql: string) => {
      if (sql.includes("SELECT version()")) {
        return Promise.resolve({ rows: [{ current_database: "nomaddb", current_user: "postgres" }] });
      }
      if (sql.includes("information_schema")) {
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      return Promise.resolve({ rows: [{ exists: 1 }] });
    });

    setupConnection({
      query: queryMock,
      acquireLock: vi.fn().mockResolvedValue(false)
    });

    const report = await runDoctor(baseConfig, driver, {});

    const lockCheck = report.checks.find(check => check.id === "advisory-lock");
    expect(lockCheck?.status).toBe("fail");
    expect(report.ok).toBe(false);
  });

  it("passes advisory lock when acquired", async () => {
    const queryMock = vi.fn().mockImplementation((sql: string) => {
      if (sql.includes("SELECT version()")) {
        return Promise.resolve({ rows: [{ current_database: "nomaddb", current_user: "postgres" }] });
      }
      if (sql.includes("information_schema")) {
        return Promise.resolve({ rows: [{ exists: 1 }] });
      }
      return Promise.resolve({ rows: [{ exists: 1 }] });
    });

    const releaseMock = vi.fn().mockResolvedValue(undefined);

    setupConnection({
      query: queryMock,
      acquireLock: vi.fn().mockResolvedValue(true),
      releaseLock: releaseMock
    });

    const report = await runDoctor(baseConfig, driver, {});

    const lockCheck = report.checks.find(check => check.id === "advisory-lock");
    expect(lockCheck?.status).toBe("pass");
    expect(releaseMock).toHaveBeenCalled();
    expect(report.ok).toBe(true);
  });
});
