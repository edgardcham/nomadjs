import { basename } from "node:path";
import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { listMigrationFiles, filenameToVersion } from "./files.js";
import { parseNomadSqlFile, type ParsedMigration } from "../parser/enhanced-parser.js";
import { calculateChecksum, verifyChecksum } from "./checksum.js";
import { detectHazards, validateHazards } from "./hazards.js";
import { DriftError, MissingFileError, SqlError, ConnectionError, ChecksumMismatchError, LockTimeoutError, NomadError } from "./errors.js";
import { Planner, type PlanOptions, type MigrationPlan } from "./planner.js";
import { matchesFilter, type TagFilter } from "./tags.js";
import type { Config } from "../config.js";
import type { Driver, DriverConnection, PoolLike } from "../driver/types.js";
import { createPostgresDriver } from "../driver/postgres.js";
import { logger } from "../utils/logger.js";
import { emitEvent, previewSql } from "../utils/events.js";

export interface MigrationFile {
  version: bigint;
  name: string;
  filepath: string;
  content: string;
  checksum: string;
  parsed: ParsedMigration;
}

export interface AppliedMigration {
  version: bigint;
  name: string;
  checksum: string;
  appliedAt: Date;
  rolledBackAt?: Date | null;
}

export interface MigrationStatus {
  version: bigint;
  name: string;
  applied: boolean;
  appliedAt?: Date;
  hasDrift?: boolean;
  isMissing?: boolean;
  hasLegacyChecksum?: boolean;
  tags?: string[];
}

export interface VerifyResult {
  valid: boolean;
  driftCount: number;
  missingCount: number;
  driftedMigrations: Array<{
    version: bigint;
    name: string;
    expectedChecksum: string;
    actualChecksum: string;
  }>;
  missingMigrations: Array<{
    version: bigint;
    name: string;
  }>;
}

function isDriver(value: Driver | PoolLike): value is Driver {
  return typeof (value as Driver).connect === "function" && typeof (value as Driver).close === "function" && typeof (value as Driver).probeConnection === "function";
}

export class Migrator {
  private readonly config: Config;
  private readonly driver: Driver;
  private readonly lockKey: string;
  private migrationFileCache: Map<string, MigrationFile> = new Map();
  private cacheLastModified: Map<string, number> = new Map();
  private cacheLastSize: Map<string, number> = new Map();

  constructor(config: Config, driverOrPool: Driver | PoolLike) {
    this.config = config;

    if (isDriver(driverOrPool)) {
      this.driver = driverOrPool;
    } else {
      const table = config.table || "nomad_migrations";
      const schema = config.schema;
      const url = config.url || "";
      this.driver = createPostgresDriver({
        url,
        table,
        schema,
        pool: driverOrPool
      });
    }

    this.lockKey = this.computeLockKey();
  }

  private async withConnection<T>(fn: (connection: DriverConnection) => Promise<T>): Promise<T> {
    let connection: DriverConnection | undefined;
    try {
      connection = await this.driver.connect();
      return await fn(connection);
    } catch (error) {
      if (error instanceof NomadError) {
        throw error;
      }
      throw this.driver.mapError(error);
    } finally {
      if (connection) {
        await connection.dispose();
      }
    }
  }

  private computeLockKey(): string {
    const url = this.config.url || "";
    const dir = this.config.dir || "";
    const table = this.config.table || "nomad_migrations";
    const schema = this.config.schema || "public";
    const data = `${url}|${dir}|${schema}|${table}`;
    return createHash("sha256").update(data).digest("hex");
  }

  private async acquireLock(connection: DriverConnection): Promise<() => Promise<void>> {
    const lockKey = this.lockKey;
    const timeout = this.config.lockTimeout || 30000;
    const start = Date.now();
    let delay = 100;
    const maxDelay = 5000;

    while (true) {
      const acquired = await connection.acquireLock(lockKey, timeout);
      if (acquired) break;
      if (Date.now() - start >= timeout) {
        throw new LockTimeoutError(timeout);
      }
      await this.sleep(delay);
      delay = Math.min(delay * 2, maxDelay);
    }

    let released = false;
    const release = async () => {
      if (released) return;
      released = true;
      try {
        await connection.releaseLock(lockKey);
      } catch (error) {
        logger.error(`Error releasing advisory lock: ${(error as Error).message}`);
      }
    };

    const signalHandlers: Array<{ signal: NodeJS.Signals; handler: () => Promise<void> }> = [];

    const removeSignalHandlers = () => {
      for (const { signal, handler } of signalHandlers) {
        process.off(signal, handler);
      }
      signalHandlers.length = 0;
    };

    const registerSignalHandler = (signal: NodeJS.Signals) => {
      const handler = async () => {
        logger.warn("\nReceived interrupt signal, releasing migration lock...");
        removeSignalHandlers();
        await release();
        if (typeof process.kill === "function") {
          process.kill(process.pid, signal);
        }
      };
      process.on(signal, handler);
      signalHandlers.push({ signal, handler });
    };

    registerSignalHandler("SIGINT");
    registerSignalHandler("SIGTERM");

    return async () => {
      removeSignalHandlers();
      await release();
    };
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Ensure the migrations table exists with v2 schema
   */
  async ensureTable(): Promise<void> {
    await this.withConnection(connection => connection.ensureMigrationsTable());
  }

  /**
   * Load migration files from disk with caching
   */
  private async loadMigrationFiles(): Promise<MigrationFile[]> {
    const dir = this.config.dir || "./migrations";
    let files: string[] = [];

    try {
      files = listMigrationFiles(dir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return [];
      }
      throw error;
    }

    return files.map(filepath => {
      // Try to use cache if available
      const cached = this.migrationFileCache.get(filepath);

      // Check if file is in cache and hasn't been modified (skip in tests)
      if (cached && process.env.NODE_ENV !== 'test') {
        try {
          const stats = statSync(filepath);
          const mtime = stats.mtimeMs;
          const lastModified = this.cacheLastModified.get(filepath);
          const lastSize = this.cacheLastSize.get(filepath);

          if (lastModified === mtime && lastSize === stats.size) {
            if (process.env.NOMAD_CACHE_HASH_GUARD === 'true') {
              const content = readFileSync(filepath, "utf8");
              const checksum = calculateChecksum(content);
              if (checksum !== cached.checksum) {
                this.migrationFileCache.delete(filepath);
                this.cacheLastModified.delete(filepath);
                this.cacheLastSize.delete(filepath);
              } else {
                return cached;
              }
            } else {
              return cached;
            }
          }
        } catch {
          // File stats not available, proceed without cache
        }
      }

      // Load the file
      const content = readFileSync(filepath, "utf8");
      const checksum = calculateChecksum(content);
      const parsed = parseNomadSqlFile(filepath);
      const filename = basename(filepath);
      const version = BigInt(filenameToVersion(filepath));
      const name = filename.replace(/^\d+[-_]/, "").replace(/\.sql$/i, "");

      const migrationFile: MigrationFile = {
        version,
        name,
        filepath,
        content,
        checksum,
        parsed
      };

      // Update cache (if not in test mode)
      if (process.env.NODE_ENV !== 'test') {
        this.migrationFileCache.set(filepath, migrationFile);
        try {
          const stats = statSync(filepath);
          this.cacheLastModified.set(filepath, stats.mtimeMs);
          this.cacheLastSize.set(filepath, stats.size);
        } catch {
          // Can't get file stats, skip cache update
        }
      }

      return migrationFile;
    });
  }

  /**
   * Clear the migration file cache (useful for testing)
   */
  clearCache(): void {
    this.migrationFileCache.clear();
    this.cacheLastModified.clear();
    this.cacheLastSize.clear();
  }

  /**
   * Get applied migrations from database
   */
  async getAppliedMigrations(): Promise<AppliedMigration[]> {
    const rows = await this.withConnection(connection => connection.fetchAppliedMigrations());

    return rows
      .filter(row => row.appliedAt !== null)
      .map(row => ({
        version: row.version,
        name: row.name,
        checksum: row.checksum,
        appliedAt: row.appliedAt as Date,
        rolledBackAt: row.rolledBackAt
      }));
  }

  /**
   * Verify checksum for a migration
   */
  async verifyChecksum(migration: MigrationFile): Promise<void> {
    if (this.config.allowDrift) {
      // Just log warning but continue
      const applied = await this.getAppliedMigrations();
      const appliedMig = applied.find(m => m.version === migration.version);

      if (appliedMig && appliedMig.checksum !== migration.checksum) {
        logger.warn(
          `Checksum mismatch for migration ${migration.version} (${migration.name})\n` +
          `  Expected: ${appliedMig.checksum}\n` +
          `  Actual: ${migration.checksum}\n` +
          `  Continuing due to --allow-drift flag`
        );
      }
      return;
    }

    const applied = await this.getAppliedMigrations();
    const appliedMig = applied.find(m => m.version === migration.version);

    if (appliedMig && appliedMig.checksum !== migration.checksum) {
      throw new ChecksumMismatchError({
        version: migration.version,
        name: migration.name,
        expectedChecksum: appliedMig.checksum,
        actualChecksum: migration.checksum,
        filepath: migration.filepath
      });
    }
  }

  /**
   * Get migration status with drift detection
   */
  async status(filter?: TagFilter): Promise<MigrationStatus[]> {
    await this.ensureTable();

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();

    const fileMap = new Map(files.map(f => [f.version, f]));
    const appliedMap = new Map(applied.map(a => [a.version, a]));

    const results: MigrationStatus[] = [];
    let hasDrift = false;
    let hasMissing = false;

    // Check files on disk (respect optional filter)
    const iterFiles = filter ? files.filter(f => matchesFilter(f.parsed.tags, filter)) : files;
    for (const file of iterFiles) {
      const appliedMig = appliedMap.get(file.version);
      const status: MigrationStatus = {
        version: file.version,
        name: file.name,
        applied: !!appliedMig && !appliedMig.rolledBackAt,
        appliedAt: appliedMig?.appliedAt,
        tags: file.parsed.tags
      };

      if (appliedMig) {
        if (!appliedMig.checksum) {
          status.hasLegacyChecksum = true;
        } else if (appliedMig.checksum !== file.checksum) {
          status.hasDrift = true;
          hasDrift = true;
        }
      }

      results.push(status);
    }

    // Check for missing files (skip when filter is active since tags are unknown)
    if (!filter) {
      for (const appliedMig of applied) {
        if (!appliedMig.rolledBackAt && !fileMap.has(appliedMig.version)) {
          const missingStatus: MigrationStatus = {
            version: appliedMig.version,
            name: appliedMig.name,
            applied: true,
            appliedAt: appliedMig.appliedAt,
            isMissing: true
          };

          // Check if it's a legacy migration without checksum
          if (!appliedMig.checksum) {
            missingStatus.hasLegacyChecksum = true;
          }

          results.push(missingStatus);
          hasMissing = true;
        }
      }
    }

    // Sort by version
    results.sort((a, b) => {
      if (a.version < b.version) return -1;
      if (a.version > b.version) return 1;
      return 0;
    });

    // Check if we should throw errors
    if (!this.config.allowDrift) {
      if (hasDrift) {
        const driftedVersions = results
          .filter(r => r.hasDrift)
          .map(r => r.version.toString());
        throw new DriftError(driftedVersions);
      }
      if (hasMissing) {
        const missingVersions = results
          .filter(r => r.isMissing)
          .map(r => r.version.toString());
        throw new MissingFileError(missingVersions);
      }
    }

    return results;
  }

  /**
   * Verify all migration checksums
   */
  async verify(): Promise<VerifyResult> {
    await this.ensureTable();
    emitEvent(this.config.eventsJson, { event: "verify-start", ts: new Date().toISOString() });

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();

    const fileMap = new Map(files.map(f => [f.version, f]));
    const driftedMigrations: VerifyResult["driftedMigrations"] = [];
    const missingMigrations: VerifyResult["missingMigrations"] = [];

    for (const appliedMig of applied) {
      if (appliedMig.rolledBackAt) continue;

      const file = fileMap.get(appliedMig.version);
      if (!file) {
        missingMigrations.push({
          version: appliedMig.version,
          name: appliedMig.name
        });
      } else if (appliedMig.checksum && appliedMig.checksum !== file.checksum) {
        driftedMigrations.push({
          version: appliedMig.version,
          name: appliedMig.name,
          expectedChecksum: appliedMig.checksum,
          actualChecksum: file.checksum
        });
      }
    }

    const result: VerifyResult = {
      valid: driftedMigrations.length === 0 && missingMigrations.length === 0,
      driftCount: driftedMigrations.length,
      missingCount: missingMigrations.length,
      driftedMigrations,
      missingMigrations
    };

    emitEvent(this.config.eventsJson, {
      event: "verify-end",
      ts: new Date().toISOString(),
      valid: result.valid,
      driftCount: result.driftCount,
      missingCount: result.missingCount
    });

    return result;
  }

  /**
   * Plan migrations up (preview without applying)
   */
  async planUp(options: PlanOptions & { filter?: TagFilter; includeAncestors?: boolean } = {}): Promise<MigrationPlan> {
    await this.ensureTable();

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();
    const appliedVersions = new Set(
      applied
        .filter(a => !a.rolledBackAt)
        .map(a => a.version.toString())
    );

    const isPending = (f: MigrationFile) => !appliedVersions.has(f.version.toString());
    let pendingAll = files.filter(isPending);
    let pending: MigrationFile[];
    let warnings: string[] = [];
    if (options.filter) {
      const selected = pendingAll.filter(f => matchesFilter(f.parsed.tags, options.filter!));
      if (options.includeAncestors) {
        if (selected.length === 0) {
          pending = [];
        } else {
          const earliest = selected[0]!.version;
          pending = files.filter(f => isPending(f) && (f.version <= earliest || matchesFilter(f.parsed.tags, options.filter!)));
          warnings.push(`Including ancestors up to ${earliest}`);
        }
      } else {
        pending = selected;
        // Warn if earlier pending exist before the first selected
        if (selected.length > 0) {
          const minSel = selected[0]!.version;
          const earlierPending = pendingAll.some(f => f.version < minSel);
          if (earlierPending) {
            warnings.push("Tag filter excludes earlier pending migrations; use --include-ancestors to include prerequisites.");
          }
        }
      }
    } else {
      pending = pendingAll;
    }

    // Check for checksum mismatches
    for (const file of files) {
      const appliedMig = applied.find(a => a.version === file.version);
      if (appliedMig && appliedMig.checksum !== file.checksum) {
        warnings.push(`Checksum mismatch for version ${file.version}`);
      }
    }

    const planner = new Planner(this.config.autoNotx === true, this.driver.supportsTransactionalDDL);
    const plan = planner.planUp(pending, options);
    if (warnings.length > 0 && plan.summary.warnings) {
      plan.summary.warnings.push(...warnings);
    } else if (warnings.length > 0) {
      plan.summary.warnings = warnings;
    }

    return plan;
  }

  /**
   * Plan migrations down (preview rollback)
   */
  async planDown(options: PlanOptions & { filter?: TagFilter } = {}): Promise<MigrationPlan> {
    await this.ensureTable();

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();

    const fileMap = new Map(files.map(f => [f.version, f]));
    const activeApplied = applied
      .filter(a => !a.rolledBackAt)
      .sort((a, b) => {
        if (b.version < a.version) return -1;
        if (b.version > a.version) return 1;
        return 0;
      });

    // Determine contiguous head based on filter (if any)
    const toRollback: MigrationFile[] = [];
    if (options.filter) {
      for (const a of activeApplied) {
        const file = fileMap.get(a.version);
        if (!file) continue;
        if (matchesFilter(file.parsed.tags, options.filter)) {
          toRollback.push(file);
        } else {
          break; // stop at first non-matching
        }
      }
    } else {
      for (const a of activeApplied) {
        const file = fileMap.get(a.version);
        if (file) toRollback.push(file);
      }
    }
    const errors: string[] = [];
    const warnings: string[] = [];

    for (const appliedMig of activeApplied) {
      const file = fileMap.get(appliedMig.version);
      if (!file) {
        errors.push(`Migration file not found for version ${appliedMig.version} (${appliedMig.name})`);
      } else {
        // Check checksum
        if (appliedMig.checksum && appliedMig.checksum !== file.checksum && !this.config.allowDrift) {
          warnings.push(`Checksum mismatch detected for version ${appliedMig.version}`);
        }
      }
    }

    const planner = new Planner(this.config.autoNotx === true, this.driver.supportsTransactionalDDL);
    const limited = typeof options.count === "number" ? toRollback.slice(0, Math.max(options.count, 0)) : toRollback;
    const plan = planner.planDown(limited, options);

    if (errors.length > 0) {
      plan.errors = errors;
      plan.migrations = [];
    }

    if (warnings.length > 0) {
      if (plan.summary.warnings) {
        plan.summary.warnings.push(...warnings);
      } else {
        plan.summary.warnings = warnings;
      }
    }

    return plan;
  }

  /**
   * Plan migration to specific version
   */
  async planTo(options: PlanOptions & { version: bigint }): Promise<MigrationPlan> {
    await this.ensureTable();

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();
    const appliedVersions = new Set(
      applied
        .filter(a => !a.rolledBackAt)
        .map(a => a.version.toString())
    );

    const planner = new Planner(this.config.autoNotx === true, this.driver.supportsTransactionalDDL);
    return planner.planTo(files, appliedVersions, options.version, options);
  }

  /**
   * Migrate to a specific target version (apply or rollback as needed)
   */
  async to(targetVersion: bigint): Promise<void> {
    const connection = await this.driver.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      cleanup = await this.acquireLock(connection);
      emitEvent(this.config.eventsJson, { event: "lock-acquired", ts: new Date().toISOString() });

      await connection.ensureMigrationsTable();

      const files = await this.loadMigrationFiles();
      const applied = await this.getAppliedMigrations();
      const activeApplied = applied.filter(a => !a.rolledBackAt);
      const appliedVersions = new Set(activeApplied.map(a => a.version.toString()));
      const fileMap = new Map(files.map(f => [f.version.toString(), f]));

      let currentMax = 0n;
      for (const a of activeApplied) {
        if (a.version > currentMax) currentMax = a.version;
      }

      if (targetVersion === currentMax) {
        logger.info("Already at target version");
        return;
      }

      if (targetVersion < currentMax) {
        const toRollback = activeApplied.filter(a => a.version > targetVersion);
        for (const appliedMig of toRollback) {
          const file = fileMap.get(appliedMig.version.toString());
          if (!file) {
            throw new MissingFileError([`${appliedMig.version}_${appliedMig.name}`]);
          }
          if (appliedMig.checksum && appliedMig.checksum !== file.checksum && !this.config.allowDrift) {
            throw new ChecksumMismatchError({
              version: file.version,
              name: file.name,
              expectedChecksum: appliedMig.checksum,
              actualChecksum: file.checksum,
              filepath: file.filepath
            });
          }
        }
      }

      const planner = new Planner(this.config.autoNotx === true, this.driver.supportsTransactionalDDL);
      const plan = planner.planTo(files, appliedVersions, targetVersion, {});

      if (plan.migrations.length === 0) {
        logger.info("Already at target version");
      } else if (plan.direction === "up") {
        logger.action(`Applying ${plan.migrations.length} migration(s) to reach ${targetVersion}`);
        for (const pm of plan.migrations) {
          const mf = fileMap.get(pm.version.toString());
          if (!mf) continue;
          emitEvent(this.config.eventsJson, { event: "apply-start", direction: "up", version: String(mf.version), name: mf.name, ts: new Date().toISOString() });
          const res = await this.applyUpWithConnection(mf, connection);
          emitEvent(this.config.eventsJson, { event: "apply-end", direction: "up", version: String(mf.version), name: mf.name, ms: res.ms, ts: new Date().toISOString() });
        }
      } else {
        logger.action(`Rolling back ${plan.migrations.length} migration(s) to reach ${targetVersion}`);
        for (const pm of plan.migrations) {
          const mf = fileMap.get(pm.version.toString());
          if (!mf) continue;
          emitEvent(this.config.eventsJson, { event: "apply-start", direction: "down", version: String(mf.version), name: mf.name, ts: new Date().toISOString() });
          const res = await this.applyDownWithConnection(mf, connection);
          emitEvent(this.config.eventsJson, { event: "apply-end", direction: "down", version: String(mf.version), name: mf.name, ms: res.ms, ts: new Date().toISOString() });
        }
      }

      const refreshedApplied = await this.getAppliedMigrations();
      let finalMax = 0n;
      for (const a of refreshedApplied.filter(a => !a.rolledBackAt)) {
        if (a.version > finalMax) finalMax = a.version;
      }
      logger.success(`Target reached. Current version: ${finalMax}`);
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
        emitEvent(this.config.eventsJson, { event: "lock-released", ts: new Date().toISOString() });
      }
      await connection.dispose();
    }
  }

  /**
   * Apply migrations up
   */
  async up(limit?: number, filter?: TagFilter, includeAncestors?: boolean): Promise<void> {
    const connection = await this.driver.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      cleanup = await this.acquireLock(connection);
      emitEvent(this.config.eventsJson, { event: "lock-acquired", ts: new Date().toISOString() });

      await connection.ensureMigrationsTable();

      const files = await this.loadMigrationFiles();
      const applied = await this.getAppliedMigrations();
      const appliedVersions = new Set(
        applied
          .filter(a => !a.rolledBackAt)
          .map(a => a.version.toString())
      );

      let pendingAll = files.filter(f => !appliedVersions.has(f.version.toString()));
      let pending: MigrationFile[];

      if (filter) {
        const selected = pendingAll.filter(f => matchesFilter(f.parsed.tags, filter));
        if (includeAncestors && selected.length > 0) {
          const earliest = selected[0]!.version;
          pending = files.filter(f => !appliedVersions.has(f.version.toString()) && (f.version <= earliest || matchesFilter(f.parsed.tags, filter)));
          logger.info(`Including ancestors up to ${earliest}`);
        } else {
          pending = selected;
          if (selected.length > 0) {
            const minSel = selected[0]!.version;
            const earlierPending = pendingAll.some(f => f.version < minSel);
            if (earlierPending) {
              logger.warn("Tag filter excludes earlier pending migrations; use --include-ancestors to include prerequisites.");
            }
          }
        }
      } else {
        pending = pendingAll;
      }

      const toApply = typeof limit === "number" ? pending.slice(0, Math.max(limit, 0)) : pending;

      logger.action(`Applying ${toApply.length} migration(s) (${pending.length} pending out of ${files.length})`);

      let totalStatements = 0;
      const totalMigrations = toApply.length;
      const wallStart = Date.now();
      for (let idx = 0; idx < toApply.length; idx++) {
        const migration = toApply[idx]!;
        if (this.config.verbose) {
          logger.action(`→ executing m${idx + 1}/${totalMigrations} ${migration.version} (${migration.name})`);
        }
        emitEvent(this.config.eventsJson, { event: "apply-start", direction: "up", version: String(migration.version), name: migration.name, ts: new Date().toISOString() });
        const res = await this.applyUpWithConnection(migration, connection);
        totalStatements += res.statements;
        emitEvent(this.config.eventsJson, { event: "apply-end", direction: "up", version: String(migration.version), name: migration.name, ms: res.ms, ts: new Date().toISOString() });
        if (this.config.verbose) {
          logger.success(`✓ done m${idx + 1}/${totalMigrations} (${res.ms}ms, ${res.statements} stmt${res.statements === 1 ? '' : 's'})`);
        }
      }
      if (this.config.verbose) {
        const wallMs = Date.now() - wallStart;
        logger.success(`✓ ${totalMigrations} migration${totalMigrations === 1 ? '' : 's'}, ${totalStatements} statement${totalStatements === 1 ? '' : 's'} in ${wallMs}ms`);
      }
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
        emitEvent(this.config.eventsJson, { event: "lock-released", ts: new Date().toISOString() });
      }
      await connection.dispose();
    }
  }

  /**
   * Rollback migrations down
   */
  async down(count = 1, filter?: TagFilter): Promise<void> {
    const connection = await this.driver.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      cleanup = await this.acquireLock(connection);
      emitEvent(this.config.eventsJson, { event: "lock-acquired", ts: new Date().toISOString() });

      await connection.ensureMigrationsTable();

      const files = await this.loadMigrationFiles();
      const applied = await this.getAppliedMigrations();

      const fileMap = new Map(files.map(f => [f.version, f]));
      const activeApplied = applied
        .filter(a => !a.rolledBackAt)
        .sort((a, b) => {
          if (b.version < a.version) return -1;
          if (b.version > a.version) return 1;
          return 0;
        });

      const toRollbackApplied: typeof activeApplied = [];
      if (filter) {
        for (const a of activeApplied) {
          const f = fileMap.get(a.version);
          if (!f) continue;
          if (matchesFilter(f.parsed.tags, filter)) {
            toRollbackApplied.push(a);
          } else {
            break;
          }
        }
      } else {
        toRollbackApplied.push(...activeApplied);
      }

      const limitedApplied = toRollbackApplied.slice(0, Math.max(count, 0));

      let totalStatements = 0;
      const totalMigrations = limitedApplied.length;
      const wallStart = Date.now();
      let idx = 0;
      for (const appliedMig of limitedApplied) {
        const file = fileMap.get(appliedMig.version);
        if (!file) {
          throw new MissingFileError([`${appliedMig.version}_${appliedMig.name}`]);
        }

        if (appliedMig.checksum && appliedMig.checksum !== file.checksum && !this.config.allowDrift) {
          throw new ChecksumMismatchError({
            version: file.version,
            name: file.name,
            expectedChecksum: appliedMig.checksum,
            actualChecksum: file.checksum,
            filepath: file.filepath
          });
        }
        if (this.config.verbose) {
          logger.action(`→ executing m${idx + 1}/${totalMigrations} ${file.version} (${file.name})`);
        }
        emitEvent(this.config.eventsJson, { event: "apply-start", direction: "down", version: String(file.version), name: file.name, ts: new Date().toISOString() });
        const res = await this.applyDownWithConnection(file, connection);
        emitEvent(this.config.eventsJson, { event: "apply-end", direction: "down", version: String(file.version), name: file.name, ms: res.ms, ts: new Date().toISOString() });
        totalStatements += res.statements;
        if (this.config.verbose) {
          logger.success(`✓ done m${idx + 1}/${totalMigrations} (${res.ms}ms, ${res.statements} stmt${res.statements === 1 ? '' : 's'})`);
        }
        idx++;
      }
      if (this.config.verbose) {
        const wallMs = Date.now() - wallStart;
        logger.success(`✓ ${totalMigrations} migration${totalMigrations === 1 ? '' : 's'} rolled back, ${totalStatements} statement${totalStatements === 1 ? '' : 's'} in ${wallMs}ms`);
      }
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
        emitEvent(this.config.eventsJson, { event: "lock-released", ts: new Date().toISOString() });
      }
      await connection.dispose();
    }
  }

  /**
   * Redo the last applied migration (rollback and reapply)
   * For safety, only the most recent migration can be redone
   */
  async redo(): Promise<void> {
    await this.ensureTable();

    const applied = await this.getAppliedMigrations();
    const files = await this.loadMigrationFiles();

    if (applied.length === 0) {
      throw new Error("No migrations to redo");
    }

    const targetMigration = applied[applied.length - 1]!;
    const file = files.find(f => f.version === targetMigration.version);
    if (!file) {
      throw new MissingFileError([
        `${targetMigration.version}_${targetMigration.name}`
      ]);
    }

    if (!this.config.allowDrift && file.checksum !== targetMigration.checksum) {
      throw new ChecksumMismatchError({
        version: targetMigration.version,
        name: targetMigration.name,
        expectedChecksum: targetMigration.checksum,
        actualChecksum: file.checksum,
        filepath: file.filepath
      });
    } else if (file.checksum !== targetMigration.checksum) {
      logger.warn(`Checksum mismatch for migration ${targetMigration.version} (${targetMigration.name})`);
    }

    const connection = await this.driver.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      cleanup = await this.acquireLock(connection);
      emitEvent(this.config.eventsJson, { event: "lock-acquired", ts: new Date().toISOString() });

      logger.action(`Rolling back ${targetMigration.version} (${targetMigration.name})`);
      emitEvent(this.config.eventsJson, { event: "apply-start", direction: "down", version: String(file.version), name: file.name, ts: new Date().toISOString() });
      const downRes = await this.applyDownWithConnection(file, connection);
      emitEvent(this.config.eventsJson, { event: "apply-end", direction: "down", version: String(file.version), name: file.name, ms: downRes.ms, ts: new Date().toISOString() });

      logger.action(`Reapplying ${targetMigration.version} (${targetMigration.name})`);
      emitEvent(this.config.eventsJson, { event: "apply-start", direction: "up", version: String(file.version), name: file.name, ts: new Date().toISOString() });
      const upRes = await this.applyUpWithConnection(file, connection);
      emitEvent(this.config.eventsJson, { event: "apply-end", direction: "up", version: String(file.version), name: file.name, ms: upRes.ms, ts: new Date().toISOString() });

      logger.success(`Redo complete: ${targetMigration.version} (${targetMigration.name})`);
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
        emitEvent(this.config.eventsJson, { event: "lock-released", ts: new Date().toISOString() });
      }
      await connection.dispose();
    }
  }

  /**
   * Apply a single migration up (legacy - gets own connection)
   */
  private async applyUp(migration: MigrationFile): Promise<void> {
    await this.withConnection(connection => this.applyUpWithConnection(migration, connection).then(() => undefined));
  }

  private async applyUpWithConnection(
    migration: MigrationFile,
    connection: DriverConnection
  ): Promise<{ statements: number; ms: number; usedTransaction: boolean }> {
    const label = `${migration.version} (${migration.name})`;

    const upSql = migration.parsed.up.statements.join("\n");
    const hazards = detectHazards(upSql);

    const validation = validateHazards(hazards, migration.parsed.up.notx || migration.parsed.noTransaction, {
      autoNotx: this.config.autoNotx,
      logger: (msg) => logger.warn(`⚠️  ${msg}`)
    });

    const shouldTx = this.driver.supportsTransactionalDDL && !validation.shouldSkipTransaction;
    const total = migration.parsed.up.statements.length;
    const startedAt = Date.now();

    if (total === 0) {
      await connection.markMigrationApplied({
        version: migration.version,
        name: migration.name,
        checksum: migration.checksum
      });
      logger.success(`↑ up ${label}`);
      const ms = Date.now() - startedAt;
      return { statements: 0, ms, usedTransaction: false };
    }

    try {
      if (shouldTx) await connection.beginTransaction();

      for (let i = 0; i < total; i++) {
        const statement = migration.parsed.up.statements[i]!;
        const start = Date.now();
        try {
          await connection.runStatement(statement);
        } catch (error) {
          throw this.createSqlError("up", migration, statement, i, error);
        }
        const ms = Date.now() - start;
        const preview = statement.length > 60 ? statement.slice(0, 57) + "..." : statement;
        if (this.config.verbose) {
          logger.info(`s${i + 1}/${total} (${ms}ms): ${preview}`);
        }
        emitEvent(this.config.eventsJson, { event: "stmt-run", direction: "up", version: String(migration.version), ms, preview: previewSql(statement) });
      }

      await connection.markMigrationApplied({
        version: migration.version,
        name: migration.name,
        checksum: migration.checksum
      });

      if (shouldTx) await connection.commitTransaction();
      logger.success(`↑ up ${label}`);
      const ms = Date.now() - startedAt;
      return { statements: total, ms, usedTransaction: shouldTx };
    } catch (error) {
      if (shouldTx) {
        try {
          await connection.rollbackTransaction();
        } catch {
        }
      }
      if (error instanceof SqlError) {
        throw error;
      }
      throw new SqlError(`Failed UP ${label}: ${(error as Error).message}`);
    }
  }

  /**
   * Apply a single migration down using provided connection
   */
  private async applyDownWithConnection(
    migration: MigrationFile,
    connection: DriverConnection
  ): Promise<{ statements: number; ms: number; usedTransaction: boolean }> {
    const label = `${migration.version} (${migration.name})`;

    const downSql = migration.parsed.down.statements.join("\n");
    const hazards = detectHazards(downSql);

    const validation = validateHazards(hazards, migration.parsed.down.notx || migration.parsed.noTransaction, {
      autoNotx: this.config.autoNotx,
      logger: (msg) => logger.warn(`⚠️  ${msg}`)
    });

    const shouldTx = this.driver.supportsTransactionalDDL && !validation.shouldSkipTransaction;
    const total = migration.parsed.down.statements.length;
    const startedAt = Date.now();

    if (total === 0) {
      await connection.markMigrationRolledBack(migration.version);
      logger.info(`↓ down ${label}`);
      const ms = Date.now() - startedAt;
      return { statements: 0, ms, usedTransaction: false };
    }

    try {
      if (shouldTx) await connection.beginTransaction();

      for (let i = 0; i < total; i++) {
        const statement = migration.parsed.down.statements[i]!;
        const start = Date.now();
        try {
          await connection.runStatement(statement);
        } catch (error) {
          throw this.createSqlError("down", migration, statement, i, error);
        }
        const ms = Date.now() - start;
        const preview = statement.length > 60 ? statement.slice(0, 57) + "..." : statement;
        if (this.config.verbose) {
          logger.info(`s${i + 1}/${total} (${ms}ms): ${preview}`);
        }
        emitEvent(this.config.eventsJson, { event: "stmt-run", direction: "down", version: String(migration.version), ms, preview: previewSql(statement) });
      }

      await connection.markMigrationRolledBack(migration.version);

      if (shouldTx) await connection.commitTransaction();
      logger.info(`↓ down ${label}`);
      const ms = Date.now() - startedAt;
      return { statements: total, ms, usedTransaction: shouldTx };
    } catch (error) {
      if (shouldTx) {
        try {
          await connection.rollbackTransaction();
        } catch {
        }
      }
      if (error instanceof SqlError) {
        throw error;
      }
      throw new SqlError(`Failed DOWN ${label}: ${(error as Error).message}`);
    }
  }

  /**
   * Apply a single migration down (legacy - gets own connection)
   */
  private async applyDown(migration: MigrationFile): Promise<void> {
    await this.withConnection(connection => this.applyDownWithConnection(migration, connection).then(() => undefined));
  }

  private createSqlError(
    direction: "up" | "down",
    migration: MigrationFile,
    statement: string,
    index: number,
    error: any
  ): SqlError {
    const metaSource = direction === "up"
      ? migration.parsed.up.statementMeta
      : migration.parsed.down.statementMeta;
    const meta = metaSource?.[index];
    const location = this.resolveErrorLocation(meta, statement, error);
    const label = `${migration.version} (${migration.name})`;
    const prefix = direction === "up" ? "Failed UP" : "Failed DOWN";

    return new SqlError(`${prefix} ${label}: ${(error as Error).message}`, {
      sql: statement,
      file: migration.filepath,
      line: location.line,
      column: location.column
    });
  }

  private resolveErrorLocation(
    meta: { line: number; column: number } | undefined,
    statement: string,
    error: any
  ): { line?: number; column?: number } {
    const rawPosition = (error && typeof error.position !== "undefined") ? error.position : undefined;
    const parsedPosition = typeof rawPosition === "string"
      ? parseInt(rawPosition, 10)
      : typeof rawPosition === "number"
        ? rawPosition
        : NaN;

    const hasMeta = Boolean(meta);
    let line = meta?.line;
    let column = meta?.column;

    if (!Number.isNaN(parsedPosition) && parsedPosition > 0) {
      const relative = this.computeRelativeLocation(statement, parsedPosition);
      if (hasMeta && meta) {
        line = meta.line + (relative.line - 1);
        column = relative.line === 1
          ? meta.column + (relative.column - 1)
          : relative.column;
      } else {
        line = relative.line;
        column = relative.column;
      }
    }

    return { line, column };
  }

  private computeRelativeLocation(statement: string, position: number): { line: number; column: number } {
    const clamped = Math.max(1, Math.min(Math.floor(position), statement.length));
    let line = 1;
    let column = 1;

    for (let i = 0; i < clamped - 1; i++) {
      const ch = statement[i];
      if (ch === "\n") {
        line++;
        column = 1;
      } else {
        column++;
      }
    }

    return { line, column };
  }
}
