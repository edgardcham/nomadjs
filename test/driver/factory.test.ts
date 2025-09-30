import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Config } from "../../src/config.js";
import { createDriver } from "../../src/driver/factory.js";

const createPostgresDriverMock = vi.fn();
const createMySqlDriverMock = vi.fn();
const createSqliteDriverMock = vi.fn();

vi.mock("../../src/driver/postgres.js", () => ({
  createPostgresDriver: (...args: unknown[]) => createPostgresDriverMock(...args)
}));

vi.mock("../../src/driver/mysql.js", () => ({
  createMySqlDriver: (...args: unknown[]) => createMySqlDriverMock(...args)
}));

vi.mock("../../src/driver/sqlite.js", () => ({
  createSqliteDriver: (...args: unknown[]) => createSqliteDriverMock(...args)
}));

describe("driver factory", () => {
  beforeEach(() => {
    createPostgresDriverMock.mockReset();
    createMySqlDriverMock.mockReset();
    createSqliteDriverMock.mockReset();
  });

  const baseConfig: Config = {
    driver: "postgres",
    url: "postgres://user:pass@localhost:5432/db",
    dir: "migrations",
    table: "nomad_migrations",
    schema: "public",
    allowDrift: false,
    autoNotx: false,
    lockTimeout: 30000,
    verbose: false,
    eventsJson: false
  };

  it("returns postgres driver by default", () => {
    const driverStub = { probeConnection: vi.fn() } as any;
    createPostgresDriverMock.mockReturnValue(driverStub);

    const driver = createDriver(baseConfig);

    expect(driver).toBe(driverStub);
    expect(createPostgresDriverMock).toHaveBeenCalledWith({
      url: baseConfig.url,
      table: baseConfig.table,
      schema: baseConfig.schema,
      connectTimeoutMs: undefined
    });
  });

  it("passes through connect timeout option", () => {
    const driverStub = { probeConnection: vi.fn() } as any;
    createPostgresDriverMock.mockReturnValue(driverStub);

    const driver = createDriver(baseConfig, { connectTimeoutMs: 1234 });

    expect(driver).toBe(driverStub);
    expect(createPostgresDriverMock).toHaveBeenCalledWith({
      url: baseConfig.url,
      table: baseConfig.table,
      schema: baseConfig.schema,
      connectTimeoutMs: 1234
    });
  });

  it("falls back to default table and schema when missing", () => {
    const driverStub = { probeConnection: vi.fn() } as any;
    createPostgresDriverMock.mockReturnValue(driverStub);

    const config: Config = {
      ...baseConfig,
      table: undefined,
      schema: undefined
    };

    createDriver(config);

    expect(createPostgresDriverMock).toHaveBeenCalledWith({
      url: config.url,
      table: "nomad_migrations",
      schema: "public",
      connectTimeoutMs: undefined
    });
  });

  it("builds mysql driver when configured", () => {
    const driverStub = { probeConnection: vi.fn() } as any;
    createMySqlDriverMock.mockReturnValue(driverStub);

    const config: Config = {
      ...baseConfig,
      driver: "mysql",
      schema: undefined
    };

    const driver = createDriver(config, { connectTimeoutMs: 456 });

    expect(driver).toBe(driverStub);
    expect(createMySqlDriverMock).toHaveBeenCalledWith({
      url: config.url,
      table: config.table,
      schema: config.schema,
      connectTimeoutMs: 456
    });
    expect(createPostgresDriverMock).not.toHaveBeenCalled();
  });

  it("builds sqlite driver when configured", () => {
    const driverStub = { probeConnection: vi.fn() } as any;
    createSqliteDriverMock.mockReturnValue(driverStub);

    const config: Config = {
      ...baseConfig,
      driver: "sqlite",
      schema: undefined
    };

    const driver = createDriver(config, { connectTimeoutMs: 789 });

    expect(driver).toBe(driverStub);
    expect(createSqliteDriverMock).toHaveBeenCalledWith({
      url: config.url,
      table: config.table,
      connectTimeoutMs: 789
    });
    expect(createPostgresDriverMock).not.toHaveBeenCalled();
    expect(createMySqlDriverMock).not.toHaveBeenCalled();
  });
});
