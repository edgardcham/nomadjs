import { basename } from "node:path";
import { readFileSync, statSync } from "node:fs";
import { listMigrationFiles, filenameToVersion } from "./files.js";
import { parseNomadSqlFile, type ParsedMigration } from "../parser/enhanced-parser.js";
import { calculateChecksum, verifyChecksum } from "./checksum.js";
import { detectHazards, validateHazards } from "./hazards.js";
import { AdvisoryLock } from "./advisory-lock.js";
import { DriftError, MissingFileError, SqlError, ConnectionError, ChecksumMismatchError, LockTimeoutError } from "./errors.js";
import { Planner, type PlanOptions, type MigrationPlan } from "./planner.js";
import type { Config } from "../config.js";
import type { Pool } from "pg";
import { logger } from "../utils/logger.js";

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

export class Migrator {
  private pool: Pool;
  private config: Config;
  private migrationFileCache: Map<string, MigrationFile> = new Map();
  private cacheLastModified: Map<string, number> = new Map();

  constructor(config: Config, pool: Pool) {
    this.config = config;
    this.pool = pool;
  }

  /**
   * Ensure the migrations table exists with v2 schema
   */
  async ensureTable(): Promise<void> {
    const table = this.config.table || "nomad_migrations";
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${table} (
        version     BIGINT PRIMARY KEY,
        name        TEXT NOT NULL,
        checksum    TEXT NOT NULL,
        applied_at  TIMESTAMPTZ,
        rolled_back_at TIMESTAMPTZ
      )
    `);
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

          if (lastModified === mtime) {
            // Return cached version
            return cached;
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
  }

  /**
   * Get applied migrations from database
   */
  async getAppliedMigrations(): Promise<AppliedMigration[]> {
    const table = this.config.table || "nomad_migrations";
    const result = await this.pool.query(`
      SELECT version, name, checksum, applied_at, rolled_back_at
      FROM ${table}
      WHERE applied_at IS NOT NULL
      ORDER BY version ASC
    `);

    return result.rows.map(row => ({
      version: BigInt(row.version as string | number),
      name: row.name as string,
      checksum: row.checksum as string,
      appliedAt: row.applied_at as Date,
      rolledBackAt: row.rolled_back_at as Date | null
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
  async status(): Promise<MigrationStatus[]> {
    await this.ensureTable();

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();

    const fileMap = new Map(files.map(f => [f.version, f]));
    const appliedMap = new Map(applied.map(a => [a.version, a]));

    const results: MigrationStatus[] = [];
    let hasDrift = false;
    let hasMissing = false;

    // Check files on disk
    for (const file of files) {
      const appliedMig = appliedMap.get(file.version);
      const status: MigrationStatus = {
        version: file.version,
        name: file.name,
        applied: !!appliedMig && !appliedMig.rolledBackAt,
        appliedAt: appliedMig?.appliedAt
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

    // Check for missing files
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

    return {
      valid: driftedMigrations.length === 0 && missingMigrations.length === 0,
      driftCount: driftedMigrations.length,
      missingCount: missingMigrations.length,
      driftedMigrations,
      missingMigrations
    };
  }

  /**
   * Plan migrations up (preview without applying)
   */
  async planUp(options: PlanOptions = {}): Promise<MigrationPlan> {
    await this.ensureTable();

    const files = await this.loadMigrationFiles();
    const applied = await this.getAppliedMigrations();
    const appliedVersions = new Set(
      applied
        .filter(a => !a.rolledBackAt)
        .map(a => a.version.toString())
    );

    const pending = files.filter(f => !appliedVersions.has(f.version.toString()));

    // Check for checksum mismatches
    const warnings: string[] = [];
    for (const file of files) {
      const appliedMig = applied.find(a => a.version === file.version);
      if (appliedMig && appliedMig.checksum !== file.checksum) {
        warnings.push(`Checksum mismatch for version ${file.version}`);
      }
    }

    const planner = new Planner(this.config.autoNotx);
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
  async planDown(options: PlanOptions = {}): Promise<MigrationPlan> {
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

    // Map to migration files and check for missing files
    const toRollback: MigrationFile[] = [];
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
        toRollback.push(file);
      }
    }

    const planner = new Planner(this.config.autoNotx);
    const plan = planner.planDown(toRollback, options);

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

    const planner = new Planner(this.config.autoNotx);
    return planner.planTo(files, appliedVersions, options.version, options);
  }

  /**
   * Migrate to a specific target version (apply or rollback as needed)
   */
  async to(targetVersion: bigint): Promise<void> {
    // Acquire advisory lock
    const lock = new AdvisoryLock({
      url: this.config.url,
      schema: this.config.schema || "public",
      table: this.config.table,
      dir: this.config.dir
    });

    const client = await this.pool.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      const lockTimeout = this.config.lockTimeout || 30000;
      cleanup = await lock.acquireWithCleanup(client, {
        timeout: lockTimeout,
        retryDelay: 100,
        maxRetryDelay: 5000
      });

      await this.ensureTable();

      const files = await this.loadMigrationFiles();
      const applied = await this.getAppliedMigrations();
      const activeApplied = applied.filter(a => !a.rolledBackAt);
      const appliedVersions = new Set(activeApplied.map(a => a.version.toString()));
      const fileMap = new Map(files.map(f => [f.version.toString(), f]));

      // Pre-check: if rolling back, ensure files exist and checksums OK
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

      // Build plan via Planner for correct order
      const planner = new Planner(this.config.autoNotx);
      const plan = planner.planTo(files, appliedVersions, targetVersion, {});

      if (plan.migrations.length === 0) {
        logger.info("Already at target version");
      } else if (plan.direction === "up") {
        logger.action(`Applying ${plan.migrations.length} migration(s) to reach ${targetVersion}`);
        for (const pm of plan.migrations) {
          const mf = fileMap.get(pm.version.toString());
          if (!mf) continue; // Shouldn't happen
          await this.applyUpWithClient(mf, client);
        }
      } else {
        logger.action(`Rolling back ${plan.migrations.length} migration(s) to reach ${targetVersion}`);
        for (const pm of plan.migrations) {
          const mf = fileMap.get(pm.version.toString());
          if (!mf) continue; // Pre-check above should catch missing files
          await this.applyDownWithClient(mf, client);
        }
      }

      // Log final state summary
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
      }
      client.release();
    }
  }

  /**
   * Apply migrations up
   */
  async up(limit?: number): Promise<void> {
    // Acquire advisory lock
    const lock = new AdvisoryLock({
      url: this.config.url,
      schema: this.config.schema || "public",
      table: this.config.table,
      dir: this.config.dir
    });

    const client = await this.pool.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      // Try to acquire lock with timeout
      const lockTimeout = this.config.lockTimeout || 30000;
      cleanup = await lock.acquireWithCleanup(client, {
        timeout: lockTimeout,
        retryDelay: 100,
        maxRetryDelay: 5000
      });

      // Now proceed with migrations
      await this.ensureTable();

      const files = await this.loadMigrationFiles();
      const applied = await this.getAppliedMigrations();
      const appliedVersions = new Set(
        applied
          .filter(a => !a.rolledBackAt)
          .map(a => a.version.toString())
      );

      const pending = files.filter(f => !appliedVersions.has(f.version.toString()));
      const toApply = typeof limit === "number" ? pending.slice(0, Math.max(limit, 0)) : pending;

      logger.action(`Applying ${toApply.length} migration(s) (${pending.length} pending out of ${files.length})`);

      for (const migration of toApply) {
        await this.applyUpWithClient(migration, client);
      }
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      // Release lock and cleanup
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
      }
      client.release();
    }
  }

  /**
   * Rollback migrations down
   */
  async down(count = 1): Promise<void> {
    // Acquire advisory lock
    const lock = new AdvisoryLock({
      url: this.config.url,
      schema: this.config.schema || "public",
      table: this.config.table,
      dir: this.config.dir
    });

    const client = await this.pool.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      // Try to acquire lock with timeout
      const lockTimeout = this.config.lockTimeout || 30000;
      cleanup = await lock.acquireWithCleanup(client, {
        timeout: lockTimeout,
        retryDelay: 100,
        maxRetryDelay: 5000
      });

      // Now proceed with rollback
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
        })
        .slice(0, Math.max(count, 0));

      for (const appliedMig of activeApplied) {
        const file = fileMap.get(appliedMig.version);
        if (!file) {
          throw new MissingFileError([`${appliedMig.version}_${appliedMig.name}`]);
        }

        // Verify checksum before rollback
        if (appliedMig.checksum && appliedMig.checksum !== file.checksum && !this.config.allowDrift) {
          throw new ChecksumMismatchError({
            version: file.version,
            name: file.name,
            expectedChecksum: appliedMig.checksum,
            actualChecksum: file.checksum,
            filepath: file.filepath
          });
        }

        await this.applyDownWithClient(file, client);
      }
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      // Release lock and cleanup
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
      }
      client.release();
    }
  }

  /**
   * Redo the last applied migration (rollback and reapply)
   * For safety, only the most recent migration can be redone
   */
  async redo(): Promise<void> {
    await this.ensureTable();

    const table = this.config.table || "nomad_migrations";
    const applied = await this.getAppliedMigrations();
    const files = await this.loadMigrationFiles();

    if (applied.length === 0) {
      throw new Error("No migrations to redo");
    }

    // Always redo the last applied migration (no version selection allowed)
    const targetMigration = applied[applied.length - 1]!;

    // Find the migration file
    const file = files.find(f => f.version === targetMigration.version);
    if (!file) {
      throw new MissingFileError([
        `${targetMigration.version}_${targetMigration.name}`
      ]);
    }

    // Verify checksum (unless drift is allowed)
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

    // Acquire advisory lock
    const lock = new AdvisoryLock({
      url: this.config.url,
      schema: this.config.schema || "public",
      table: this.config.table,
      dir: this.config.dir
    });

    const client = await this.pool.connect();
    let cleanup: (() => Promise<void>) | undefined;

    try {
      // Acquire lock with timeout
      cleanup = await lock.acquireWithCleanup(client, {
        timeout: this.config.lockTimeout || 30000,
        retryDelay: 100,
        maxRetryDelay: 5000
      });

      logger.action(`Rolling back ${targetMigration.version} (${targetMigration.name})`);

      // Execute down migration
      await this.executeDown(file, client);

      logger.action(`Reapplying ${targetMigration.version} (${targetMigration.name})`);

      // Execute up migration
      await this.executeUp(file, client);

      logger.success(`Redo complete: ${targetMigration.version} (${targetMigration.name})`);
    } catch (error) {
      if (error instanceof LockTimeoutError) {
        logger.error("Failed to acquire migration lock - another migration may be in progress");
      }
      throw error;
    } finally {
      // Release lock and cleanup
      if (cleanup) {
        await cleanup();
        cleanup = undefined;
      }
      client.release();
    }
  }

  /**
   * Execute down migration with proper transaction handling
   */
  private async executeDown(migration: MigrationFile, client: any): Promise<void> {
    const table = this.config.table || "nomad_migrations";
    const downStatements = migration.parsed.down.statements;

    if (downStatements.length === 0) {
      // No down migration - just update the database record
      await client.query(
        `UPDATE ${table} SET rolled_back_at = now() WHERE version = $1`,
        [migration.version]
      );
      return;
    }

    // Check for hazards in down statements
    const downSql = downStatements.join("\n");
    const hazards = detectHazards(downSql);
    const validation = validateHazards(hazards, migration.parsed.down.notx, {
      autoNotx: this.config.autoNotx
    });

    if (!validation) {
      throw new Error("Hazard validation failed");
    }

    const useTransaction = !migration.parsed.down.notx &&
                          !(this.config.autoNotx && hazards.length > 0);

    try {
      if (useTransaction) {
        await client.query("BEGIN");
      }

      // Execute each down statement
      for (const statement of downStatements) {
        await client.query(statement);
      }

      // Update migration record
      await client.query(
        `UPDATE ${table} SET rolled_back_at = now() WHERE version = $1`,
        [migration.version]
      );

      if (useTransaction) {
        await client.query("COMMIT");
      }
    } catch (error) {
      if (useTransaction) {
        await client.query("ROLLBACK");
      }
      throw new SqlError(`Down migration failed for ${migration.version}: ${(error as Error).message}`);
    }
  }

  /**
   * Execute up migration with proper transaction handling
   */
  private async executeUp(migration: MigrationFile, client: any): Promise<void> {
    const table = this.config.table || "nomad_migrations";
    const upStatements = migration.parsed.up.statements;

    if (upStatements.length === 0) {
      // No up migration - just update the database record
      await client.query(
        `UPDATE ${table} SET rolled_back_at = NULL, applied_at = now() WHERE version = $1`,
        [migration.version]
      );
      return;
    }

    // Check for hazards in up statements
    const upSql = upStatements.join("\n");
    const hazards = detectHazards(upSql);
    const validation = validateHazards(hazards, migration.parsed.up.notx || migration.parsed.noTransaction, {
      autoNotx: this.config.autoNotx
    });

    if (!validation) {
      throw new Error("Hazard validation failed");
    }

    const useTransaction = !(migration.parsed.up.notx || migration.parsed.noTransaction) &&
                          !(this.config.autoNotx && hazards.length > 0);

    try {
      if (useTransaction) {
        await client.query("BEGIN");
      }

      // Execute each up statement
      for (const statement of upStatements) {
        await client.query(statement);
      }

      // Update migration record
      await client.query(
        `UPDATE ${table}
         SET rolled_back_at = NULL,
             applied_at = now(),
             checksum = $2
         WHERE version = $1`,
        [migration.version, migration.checksum]
      );

      if (useTransaction) {
        await client.query("COMMIT");
      }
    } catch (error) {
      if (useTransaction) {
        await client.query("ROLLBACK");
      }
      throw new SqlError(`Up migration failed for ${migration.version}: ${(error as Error).message}`);
    }
  }

  /**
   * Apply a single migration up using provided client
   */
  private async applyUpWithClient(migration: MigrationFile, client: any): Promise<void> {
    const table = this.config.table || "nomad_migrations";
    const label = `${migration.version} (${migration.name})`;

    // Check for hazards in up statements
    const upSql = migration.parsed.up.statements.join("\n");
    const hazards = detectHazards(upSql);

    // Validate hazards and determine if we should skip transactions
    const validation = validateHazards(hazards, migration.parsed.up.notx || migration.parsed.noTransaction, {
      autoNotx: this.config.autoNotx,
      logger: (msg) => logger.warn(`⚠️  ${msg}`)
    });

    const shouldTx = !validation.shouldSkipTransaction;

    try {
      if (shouldTx) {
        await client.query("BEGIN");
      }

      for (const statement of migration.parsed.up.statements) {
        await client.query(statement);
      }

      await client.query(
        `INSERT INTO ${table} (version, name, checksum, applied_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (version) DO UPDATE
         SET applied_at = NOW(), rolled_back_at = NULL`,
        [migration.version.toString(), migration.name, migration.checksum]
      );

      if (shouldTx) {
        await client.query("COMMIT");
      }
      logger.success(`↑ up ${label}`);
    } catch (error) {
      if (shouldTx) await client.query("ROLLBACK");
      throw new SqlError(`Failed UP ${label}: ${(error as Error).message}`);
    }
  }

  /**
   * Apply a single migration up (legacy - gets own connection)
   */
  private async applyUp(migration: MigrationFile): Promise<void> {
    const client = await this.pool.connect();
    try {
      await this.applyUpWithClient(migration, client);
    } finally {
      client.release();
    }
  }

  /**
   * Apply a single migration down using provided client
   */
  private async applyDownWithClient(migration: MigrationFile, client: any): Promise<void> {
    const table = this.config.table || "nomad_migrations";
    const label = `${migration.version} (${migration.name})`;

    // Check for hazards in down statements
    const downSql = migration.parsed.down.statements.join("\n");
    const hazards = detectHazards(downSql);

    // Validate hazards and determine if we should skip transactions
    const validation = validateHazards(hazards, migration.parsed.down.notx || migration.parsed.noTransaction, {
      autoNotx: this.config.autoNotx,
      logger: (msg) => logger.warn(`⚠️  ${msg}`)
    });

    const shouldTx = !validation.shouldSkipTransaction;

    try {
      if (shouldTx) await client.query("BEGIN");

      for (const statement of migration.parsed.down.statements) {
        await client.query(statement);
      }

      await client.query(
        `UPDATE ${table}
         SET rolled_back_at = NOW()
         WHERE version = $1`,
        [migration.version.toString()]
      );

      if (shouldTx) await client.query("COMMIT");
      logger.info(`↓ down ${label}`);
    } catch (error) {
      if (shouldTx) await client.query("ROLLBACK");
      throw new SqlError(`Failed DOWN ${label}: ${(error as Error).message}`);
    }
  }

  /**
   * Apply a single migration down (legacy - gets own connection)
   */
  private async applyDown(migration: MigrationFile): Promise<void> {
    const client = await this.pool.connect();
    try {
      await this.applyDownWithClient(migration, client);
    } finally {
      client.release();
    }
  }
}
