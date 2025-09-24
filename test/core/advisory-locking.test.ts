import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { AdvisoryLock, LockConfig, LockTimeoutError } from "../../src/core/advisory-lock";
import type { PoolClient } from "pg";

describe("Advisory Locking", () => {
  let mockClient: any;
  let originalConsoleLog: any;

  beforeEach(() => {
    mockClient = {
      query: vi.fn(),
      release: vi.fn()
    };

    originalConsoleLog = console.log;
    console.log = vi.fn();
  });

  afterEach(() => {
    vi.clearAllMocks();
    console.log = originalConsoleLog;
  });

  describe("Lock Key Generation", () => {
    it("should generate consistent hash for same config", () => {
      const config1 = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const config2 = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock1 = new AdvisoryLock(config1);
      const lock2 = new AdvisoryLock(config2);

      expect(lock1.getLockKey()).toBe(lock2.getLockKey());
    });

    it("should generate different hash for different configs", () => {
      const config1 = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const config2 = {
        url: "postgres://localhost/otherdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock1 = new AdvisoryLock(config1);
      const lock2 = new AdvisoryLock(config2);

      expect(lock1.getLockKey()).not.toBe(lock2.getLockKey());
    });

    it("should handle special characters in config", () => {
      const config = {
        url: "postgres://user:p@ss!word@localhost:5432/my-db?ssl=true",
        schema: "my-schema",
        table: "migrations_table",
        dir: "./db/migrations/"
      };

      const lock = new AdvisoryLock(config);
      const key = lock.getLockKey();

      expect(key).toBeTypeOf("number");
      expect(key).toBeGreaterThan(0);
    });

    it("should generate positive lock keys", () => {
      const configs = [
        { url: "a", schema: "b", table: "c", dir: "d" },
        { url: "postgres://localhost", schema: "public", table: "mig", dir: "." },
        { url: "very-long-url-with-many-characters", schema: "schema", table: "table", dir: "dir" }
      ];

      configs.forEach(config => {
        const lock = new AdvisoryLock(config);
        const key = lock.getLockKey();
        expect(key).toBeGreaterThan(0);
        expect(key).toBeLessThanOrEqual(2147483647); // Max 32-bit signed int
      });
    });
  });

  describe("Lock Acquisition", () => {
    it("should acquire lock successfully when available", async () => {
      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const acquired = await lock.acquire(mockClient);

      expect(acquired).toBe(true);
      expect(mockClient.query).toHaveBeenCalledWith(
        "SELECT pg_try_advisory_lock($1)",
        [expect.any(Number)]
      );
    });

    it("should fail to acquire lock when unavailable", async () => {
      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: false }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const acquired = await lock.acquire(mockClient);

      expect(acquired).toBe(false);
    });

    it("should retry with exponential backoff", async () => {
      const startTime = Date.now();

      // First 3 attempts fail, 4th succeeds
      mockClient.query
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: false }] })
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: false }] })
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: false }] })
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const acquired = await lock.acquireWithRetry(mockClient, {
        timeout: 5000,
        retryDelay: 100,
        maxRetryDelay: 1000
      });

      const elapsed = Date.now() - startTime;

      expect(acquired).toBe(true);
      expect(mockClient.query).toHaveBeenCalledTimes(4);

      // Should have delays: 100ms, 200ms, 400ms = 700ms minimum
      expect(elapsed).toBeGreaterThanOrEqual(600); // Allow some margin
    });

    it("should timeout after specified duration", async () => {
      // Always return false (lock never available)
      mockClient.query.mockResolvedValue({ rows: [{ pg_try_advisory_lock: false }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);

      await expect(
        lock.acquireWithRetry(mockClient, {
          timeout: 500,
          retryDelay: 100
        })
      ).rejects.toThrow(LockTimeoutError);

      // Should have tried multiple times within 500ms
      expect(mockClient.query.mock.calls.length).toBeGreaterThan(1);
      expect(mockClient.query.mock.calls.length).toBeLessThan(10);
    });

    it("should respect max retry delay", async () => {
      const attempts: number[] = [];
      let lastAttempt = Date.now();

      mockClient.query.mockImplementation(async () => {
        const now = Date.now();
        attempts.push(now - lastAttempt);
        lastAttempt = now;
        return { rows: [{ pg_try_advisory_lock: false }] };
      });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);

      try {
        await lock.acquireWithRetry(mockClient, {
          timeout: 2000,
          retryDelay: 100,
          maxRetryDelay: 300
        });
      } catch (e) {
        // Expected to timeout
      }

      // After initial attempts, delays should be capped at maxRetryDelay
      const laterDelays = attempts.slice(4); // Skip first few
      laterDelays.forEach(delay => {
        expect(delay).toBeLessThanOrEqual(350); // Allow some margin
      });
    });
  });

  describe("Lock Release", () => {
    it("should release lock successfully", async () => {
      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_advisory_unlock: true }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const released = await lock.release(mockClient);

      expect(released).toBe(true);
      expect(mockClient.query).toHaveBeenCalledWith(
        "SELECT pg_advisory_unlock($1)",
        [expect.any(Number)]
      );
    });

    it("should handle release when lock not held", async () => {
      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_advisory_unlock: false }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const released = await lock.release(mockClient);

      expect(released).toBe(false);
    });

    it("should handle release errors gracefully", async () => {
      mockClient.query.mockRejectedValueOnce(new Error("Connection lost"));

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);

      // Should not throw, just return false
      const released = await lock.release(mockClient);
      expect(released).toBe(false);
    });
  });

  describe("Signal Handling", () => {
    it("should register cleanup on lock acquisition", async () => {
      const processOnSpy = vi.spyOn(process, "on");

      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      await lock.acquireWithCleanup(mockClient);

      expect(processOnSpy).toHaveBeenCalledWith("SIGINT", expect.any(Function));
      expect(processOnSpy).toHaveBeenCalledWith("SIGTERM", expect.any(Function));

      processOnSpy.mockRestore();
    });

    it("should release lock on SIGINT", async () => {
      let sigintHandler: any;
      const processOnSpy = vi.spyOn(process, "on").mockImplementation((event, handler) => {
        if (event === "SIGINT") sigintHandler = handler;
        return process;
      });

      const processExitSpy = vi.spyOn(process, "exit").mockImplementation((() => {}) as any);

      mockClient.query
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] })
        .mockResolvedValueOnce({ rows: [{ pg_advisory_unlock: true }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      await lock.acquireWithCleanup(mockClient);

      // Trigger SIGINT
      await sigintHandler();

      // Should have released the lock
      expect(mockClient.query).toHaveBeenCalledWith(
        "SELECT pg_advisory_unlock($1)",
        [expect.any(Number)]
      );

      // Should have called process.exit
      expect(processExitSpy).toHaveBeenCalledWith(130);

      processOnSpy.mockRestore();
      processExitSpy.mockRestore();
    });

    it("should handle cleanup errors gracefully", async () => {
      let sigtermHandler: any;
      const processOnSpy = vi.spyOn(process, "on").mockImplementation((event, handler) => {
        if (event === "SIGTERM") sigtermHandler = handler;
        return process;
      });

      const processExitSpy = vi.spyOn(process, "exit").mockImplementation((() => {}) as any);

      mockClient.query
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] })
        .mockRejectedValueOnce(new Error("Connection lost during cleanup"));

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      await lock.acquireWithCleanup(mockClient);

      // Should still exit even if cleanup fails
      await sigtermHandler();

      // Should have called process.exit
      expect(processExitSpy).toHaveBeenCalledWith(130);

      processOnSpy.mockRestore();
      processExitSpy.mockRestore();
    });

    it("should unregister handlers after release", async () => {
      const processOffSpy = vi.spyOn(process, "off");

      mockClient.query
        .mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] })
        .mockResolvedValueOnce({ rows: [{ pg_advisory_unlock: true }] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const cleanup = await lock.acquireWithCleanup(mockClient);

      // Manually call cleanup
      await cleanup();

      expect(processOffSpy).toHaveBeenCalledWith("SIGINT", expect.any(Function));
      expect(processOffSpy).toHaveBeenCalledWith("SIGTERM", expect.any(Function));

      processOffSpy.mockRestore();
    });
  });

  describe("Lock Status", () => {
    it("should check if lock is held by another connection", async () => {
      mockClient.query.mockResolvedValueOnce({
        rows: [{ locked: true, pid: 12345 }]
      });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const status = await lock.getStatus(mockClient);

      expect(status.isLocked).toBe(true);
      expect(status.lockedBy).toBe(12345);
      expect(mockClient.query).toHaveBeenCalledWith(
        expect.stringContaining("pg_locks"),
        [expect.any(Number)]
      );
    });

    it("should report unlocked status", async () => {
      mockClient.query.mockResolvedValueOnce({
        rows: []
      });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const status = await lock.getStatus(mockClient);

      expect(status.isLocked).toBe(false);
      expect(status.lockedBy).toBeUndefined();
    });
  });

  describe("Edge Cases", () => {
    it("should handle database connection errors", async () => {
      mockClient.query.mockRejectedValueOnce(new Error("Connection refused"));

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);

      await expect(lock.acquire(mockClient)).rejects.toThrow("Connection refused");
    });

    it("should handle null/undefined in lock results", async () => {
      mockClient.query.mockResolvedValueOnce({ rows: [{}] });

      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock = new AdvisoryLock(config);
      const acquired = await lock.acquire(mockClient);

      expect(acquired).toBe(false);
    });

    it("should handle concurrent lock attempts", async () => {
      const config = {
        url: "postgres://localhost/testdb",
        schema: "public",
        table: "nomad_migrations",
        dir: "./migrations"
      };

      const lock1 = new AdvisoryLock(config);
      const lock2 = new AdvisoryLock(config);

      // First lock succeeds
      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: true }] });
      const acquired1 = await lock1.acquire(mockClient);
      expect(acquired1).toBe(true);

      // Second lock fails (same key)
      mockClient.query.mockResolvedValueOnce({ rows: [{ pg_try_advisory_lock: false }] });
      const acquired2 = await lock2.acquire(mockClient);
      expect(acquired2).toBe(false);
    });

    it("should handle very long config strings", () => {
      const config = {
        url: "postgres://".padEnd(1000, "x"),
        schema: "public".padEnd(500, "y"),
        table: "migrations".padEnd(500, "z"),
        dir: "./migrations".padEnd(500, "w")
      };

      const lock = new AdvisoryLock(config);
      const key = lock.getLockKey();

      expect(key).toBeTypeOf("number");
      expect(key).toBeGreaterThan(0);
      expect(key).toBeLessThanOrEqual(2147483647);
    });
  });
});