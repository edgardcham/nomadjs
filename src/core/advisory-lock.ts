import { createHash } from "crypto";
import type { PoolClient } from "pg";
import { LockTimeoutError } from "./errors.js";
import { logger } from "../utils/logger.js";

export interface LockConfig {
  url: string;
  schema?: string;
  table?: string;
  dir: string;
}

export interface RetryConfig {
  timeout?: number;      // Total timeout in ms (default: 30000)
  retryDelay?: number;   // Initial retry delay in ms (default: 100)
  maxRetryDelay?: number; // Max retry delay in ms (default: 5000)
}

export interface LockStatus {
  isLocked: boolean;
  lockedBy?: number; // Process ID holding the lock
}


export class AdvisoryLock {
  private readonly lockKey: number;
  private cleanupHandlers: Array<() => void> = [];

  constructor(private readonly config: LockConfig) {
    this.lockKey = this.generateLockKey();
  }

  /**
   * Generate a consistent lock key from configuration
   * Uses CRC32 to ensure we get a positive 32-bit integer
   */
  private generateLockKey(): number {
    const parts = [
      this.config.url,
      this.config.schema || "public",
      this.config.table || "nomad_migrations",
      this.config.dir
    ];

    const combined = parts.join("|");
    const hash = createHash("sha256").update(combined).digest();

    // Use first 4 bytes as unsigned 32-bit integer, then ensure positive
    const num = hash.readUInt32BE(0);

    // PostgreSQL advisory locks use bigint, but we'll use positive int32 range
    // to ensure compatibility and avoid issues
    return (num % 2147483647) + 1;
  }

  /**
   * Get the generated lock key
   */
  getLockKey(): number {
    return this.lockKey;
  }

  /**
   * Try to acquire the advisory lock
   */
  async acquire(client: PoolClient): Promise<boolean> {
    try {
      const result = await client.query(
        "SELECT pg_try_advisory_lock($1)",
        [this.lockKey]
      );

      return result.rows[0]?.pg_try_advisory_lock === true;
    } catch (error) {
      // Re-throw connection errors
      throw error;
    }
  }

  /**
   * Try to acquire lock with retry and exponential backoff
   */
  async acquireWithRetry(
    client: PoolClient,
    config: RetryConfig = {}
  ): Promise<boolean> {
    const {
      timeout = 30000,
      retryDelay = 100,
      maxRetryDelay = 5000
    } = config;

    const startTime = Date.now();
    let currentDelay = retryDelay;

    while (true) {
      // Try to acquire lock
      const acquired = await this.acquire(client);
      if (acquired) {
        return true;
      }

      // Check timeout
      const elapsed = Date.now() - startTime;
      if (elapsed >= timeout) {
        throw new LockTimeoutError(timeout);
      }

      // Wait before retry with exponential backoff
      await this.sleep(currentDelay);

      // Increase delay for next attempt (exponential backoff)
      currentDelay = Math.min(currentDelay * 2, maxRetryDelay);
    }
  }

  /**
   * Acquire lock with signal cleanup handlers
   */
  async acquireWithCleanup(
    client: PoolClient,
    config: RetryConfig = {}
  ): Promise<() => Promise<void>> {
    // Acquire the lock first
    const acquired = await this.acquireWithRetry(client, config);
    if (!acquired) {
      throw new Error("Failed to acquire lock");
    }

    // Setup signal handlers for cleanup
    const cleanupHandler = async () => {
      logger.warn("\nReceived interrupt signal, releasing migration lock...");
      try {
        await this.release(client);
      } catch (error) {
        logger.error(`Error releasing lock: ${(error as Error).message}`);
      }
      process.exit(130); // Standard exit code for SIGINT
    };

    // Register signal handlers
    process.on("SIGINT", cleanupHandler);
    process.on("SIGTERM", cleanupHandler);

    // Store handlers for later removal
    this.cleanupHandlers.push(() => {
      process.off("SIGINT", cleanupHandler);
      process.off("SIGTERM", cleanupHandler);
    });

    // Return cleanup function
    return async () => {
      await this.release(client);
      this.cleanupHandlers.forEach(handler => handler());
      this.cleanupHandlers = [];
    };
  }

  /**
   * Release the advisory lock
   */
  async release(client: PoolClient): Promise<boolean> {
    try {
      const result = await client.query(
        "SELECT pg_advisory_unlock($1)",
        [this.lockKey]
      );

      return result.rows[0]?.pg_advisory_unlock === true;
    } catch (error) {
      // Log error but don't throw - connection might be closed
      logger.error(`Error releasing advisory lock: ${(error as Error).message}`);
      return false;
    }
  }

  /**
   * Check lock status
   */
  async getStatus(client: PoolClient): Promise<LockStatus> {
    const result = await client.query(
      `SELECT
        EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1) as locked,
        pid FROM pg_locks WHERE locktype = 'advisory' AND objid = $1 LIMIT 1`,
      [this.lockKey]
    );

    const row = result.rows[0];
    if (!row || !row.locked) {
      return { isLocked: false };
    }

    return {
      isLocked: true,
      lockedBy: row.pid as number
    };
  }

  /**
   * Sleep helper for retry delays
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
