import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Config } from "../../src/config.js";
import { createDriver } from "../../src/driver/factory.js";

const createPostgresDriverMock = vi.fn();

vi.mock("../../src/driver/postgres.js", () => ({
  createPostgresDriver: (...args: unknown[]) => createPostgresDriverMock(...args)
}));

describe("driver factory", () => {
  beforeEach(() => {
    createPostgresDriverMock.mockReset();
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
    const driverStub = { getPool: vi.fn() } as any;
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
    const driverStub = { getPool: vi.fn() } as any;
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
    const driverStub = { getPool: vi.fn() } as any;
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
});
