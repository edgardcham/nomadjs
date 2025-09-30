import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { Config } from "../../src/config.js";
import { Migrator } from "../../src/core/migrator.js";
import { createDriverMock, type DriverMock } from "../helpers/driver-mock.js";
import type { DriverConnectionMock } from "../helpers/driver-mock.js";

const baseConfig: Config = {
  driver: "postgres",
  url: "postgresql://localhost:5432/test",
  dir: "migrations",
  table: "nomad_migrations",
  allowDrift: false,
  autoNotx: false,
  lockTimeout: 1000
};

describe("Migrator signal handling", () => {
  let driver: DriverMock;
  let connection: DriverConnectionMock;
  let migrator: Migrator;
  let onSpy: ReturnType<typeof vi.spyOn>;
  let offSpy: ReturnType<typeof vi.spyOn>;
  let killSpy: ReturnType<typeof vi.spyOn>;
  let exitSpy: ReturnType<typeof vi.spyOn>;
  let handlers: Record<string, () => Promise<void> | void>;

  beforeEach(() => {
    handlers = {};

    onSpy = vi.spyOn(process, "on");
    offSpy = vi.spyOn(process, "off");
    killSpy = vi.spyOn(process, "kill");
    exitSpy = vi.spyOn(process, "exit");

    onSpy.mockImplementation((event: string | symbol, handler: any) => {
      if (typeof event === "string") {
        handlers[event] = handler;
      }
      return process;
    });

    offSpy.mockImplementation((event: string | symbol, handler: any) => {
      if (typeof event === "string" && handlers[event] === handler) {
        delete handlers[event];
      }
      return process;
    });

    killSpy.mockImplementation(() => true);
    exitSpy.mockImplementation(() => undefined as never);

    driver = createDriverMock();
    connection = driver.enqueueConnection({
      acquireLock: vi.fn().mockResolvedValue(true),
      releaseLock: vi.fn().mockResolvedValue(undefined)
    });

    migrator = new Migrator(baseConfig, driver);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("removes signal handlers and releases lock during cleanup", async () => {
    const cleanup = await (migrator as any).acquireLock(connection);

    expect(onSpy).toHaveBeenCalledWith("SIGINT", expect.any(Function));
    expect(onSpy).toHaveBeenCalledWith("SIGTERM", expect.any(Function));

    await cleanup();

    expect(connection.releaseLock).toHaveBeenCalledTimes(1);
    expect(killSpy).not.toHaveBeenCalled();
    expect(exitSpy).not.toHaveBeenCalled();
    expect(handlers).toEqual({});
  });

  it("releases the lock and re-sends the signal on interrupt", async () => {
    const cleanup = await (migrator as any).acquireLock(connection);

    const sigintHandler = handlers["SIGINT"];
    expect(typeof sigintHandler).toBe("function");

    await sigintHandler();

    expect(connection.releaseLock).toHaveBeenCalledTimes(1);
    expect(killSpy).toHaveBeenCalledWith(process.pid, "SIGINT");
    expect(exitSpy).not.toHaveBeenCalled();
    expect(handlers).toEqual({});

    await cleanup();
    expect(connection.releaseLock).toHaveBeenCalledTimes(1);
    expect(killSpy).toHaveBeenCalledTimes(1);
  });
});
