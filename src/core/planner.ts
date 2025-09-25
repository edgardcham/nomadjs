import { detectHazards } from "./hazards.js";
import type { Hazard } from "./hazards.js";
import type { TagFilter } from "./tags.js";
import type { MigrationFile } from "./migrator.js";

export interface PlanOptions {
  limit?: number;        // For up command
  count?: number;        // For down command
  version?: bigint;      // For to command
  format?: "human" | "json";
  dryRun?: boolean;
  // Not used by Planner; consumed in Migrator before planning
  filter?: TagFilter;
  includeAncestors?: boolean;
}

export interface PlannedMigration {
  version: bigint;
  name: string;
  filepath: string;
  transaction: boolean;
  reason?: string;       // Why transaction is disabled
  hazards: Hazard[];
  statements: string[];
  tags?: string[];
  warnings?: string[];
}

export interface MigrationPlan {
  direction: "up" | "down";
  migrations: PlannedMigration[];
  summary: {
    total: number;
    transactional: number;
    nonTransactional: number;
    hazardCount: number;
    warnings?: string[];
  };
  errors?: string[];
  dryRun?: boolean;
}

export class Planner {
  constructor(
    private autoNotx: boolean = false
  ) {}

  /**
   * Create a migration plan for UP migrations
   */
  planUp(
    pendingMigrations: MigrationFile[],
    options: PlanOptions = {}
  ): MigrationPlan {
    const migrations = options.limit !== undefined
      ? pendingMigrations.slice(0, Math.max(options.limit, 0))
      : pendingMigrations;

    const plannedMigrations = migrations.map(m => this.planMigration(m, "up"));

    return this.createPlan("up", plannedMigrations, options);
  }

  /**
   * Create a migration plan for DOWN migrations
   */
  planDown(
    appliedMigrations: MigrationFile[],
    options: PlanOptions = {}
  ): MigrationPlan {
    // Take the most recent N migrations
    const migrations = options.count !== undefined
      ? appliedMigrations.slice(0, Math.max(options.count, 0))
      : appliedMigrations;

    const plannedMigrations = migrations.map(m => this.planMigration(m, "down"));

    return this.createPlan("down", plannedMigrations, options);
  }

  /**
   * Create a migration plan to reach a specific version
   */
  planTo(
    allMigrations: MigrationFile[],
    appliedVersions: Set<string>,
    targetVersion: bigint,
    options: PlanOptions = {}
  ): MigrationPlan {
    const targetApplied = appliedVersions.has(targetVersion.toString());
    const currentMax = this.getCurrentMaxVersion(appliedVersions);

    if (targetVersion > currentMax) {
      // Moving forward - apply migrations up to and including target
      const pending = allMigrations
        .filter(m => !appliedVersions.has(m.version.toString()))
        .filter(m => m.version <= targetVersion)
        .sort((a, b) => {
          if (a.version < b.version) return -1;
          if (a.version > b.version) return 1;
          return 0;
        });

      const planned = pending.map(m => this.planMigration(m, "up"));
      return this.createPlan("up", planned, options);
    } else if (targetVersion < currentMax) {
      // Moving backward - rollback migrations after target
      const toRollback = allMigrations
        .filter(m => appliedVersions.has(m.version.toString()))
        .filter(m => m.version > targetVersion)
        .sort((a, b) => {
          if (b.version < a.version) return -1;
          if (b.version > a.version) return 1;
          return 0;
        });

      const planned = toRollback.map(m => this.planMigration(m, "down"));
      return this.createPlan("down", planned, options);
    } else {
      // Already at target version
      return this.createPlan("up", [], options);
    }
  }

  /**
   * Plan a single migration
   */
  private planMigration(
    migration: MigrationFile,
    direction: "up" | "down",
    warnings?: string[]
  ): PlannedMigration {
    const directionData = migration.parsed[direction];
    const statements = directionData.statements;
    const sql = statements.join("\n");
    const hazards = detectHazards(sql);

    // Determine if transaction should be used
    let useTransaction = true;
    let reason: string | undefined;

    if (directionData.notx || migration.parsed.noTransaction) {
      useTransaction = false;
      reason = "notx directive";
    } else if (hazards.length > 0) {
      if (this.autoNotx) {
        useTransaction = false;
        reason = "auto-notx (hazards detected)";
      } else {
        useTransaction = false;
        reason = "hazardous operations";
      }
    }

    return {
      version: migration.version,
      name: migration.name,
      filepath: migration.filepath,
      transaction: useTransaction,
      reason,
      hazards,
      statements,
      tags: migration.parsed.tags,
      warnings
    };
  }

  /**
   * Create the final plan object
   */
  private createPlan(
    direction: "up" | "down",
    migrations: PlannedMigration[],
    options: PlanOptions
  ): MigrationPlan {
    const transactional = migrations.filter(m => m.transaction).length;
    const nonTransactional = migrations.filter(m => !m.transaction).length;
    const hazardCount = migrations.reduce(
      (sum, m) => sum + m.hazards.length,
      0
    );

    const warnings: string[] = [];
    migrations.forEach(m => {
      if (m.warnings) {
        warnings.push(...m.warnings);
      }
    });

    return {
      direction,
      migrations,
      summary: {
        total: migrations.length,
        transactional,
        nonTransactional,
        hazardCount,
        warnings: warnings.length > 0 ? warnings : undefined
      },
      dryRun: options.dryRun
    };
  }

  /**
   * Get the maximum applied version
   */
  private getCurrentMaxVersion(appliedVersions: Set<string>): bigint {
    let max = 0n;
    for (const v of appliedVersions) {
      const version = BigInt(v);
      if (version > max) {
        max = version;
      }
    }
    return max;
  }

  /**
   * Format plan for human-readable output
   */
  formatPlanOutput(plan: MigrationPlan): string {
    const lines: string[] = [];

    lines.push(`Migration Plan: ${plan.direction.toUpperCase()}`);
    lines.push("═".repeat(50));
    lines.push("");

    if (plan.errors && plan.errors.length > 0) {
      lines.push("❌ ERRORS:");
      plan.errors.forEach(err => lines.push(`   ${err}`));
      lines.push("");
      return lines.join("\n");
    }

    if (plan.migrations.length === 0) {
      lines.push("No migrations to execute.");
      return lines.join("\n");
    }

    for (const migration of plan.migrations) {
      const txLabel = migration.transaction ? "[TX]" : "[NO-TX]";
      const hazardLabel = migration.hazards.length > 0 ? " ⚠️ HAZARD" : "";
      const arrow = plan.direction === "up" ? "↑" : "↓";

      lines.push(`${txLabel} ${arrow} ${migration.version}_${migration.name}.sql${hazardLabel}`);

      if (migration.reason && !migration.transaction) {
        lines.push(`     └─ Reason: ${migration.reason}`);
      }

      // Show first statement (truncated if too long)
      if (migration.statements.length > 0) {
        const stmt = migration.statements[0];
        if (stmt) {
          const preview = stmt.length > 60
            ? stmt.substring(0, 57) + "..."
            : stmt;
          lines.push(`     └─ ${preview}`);
        }
      }

      if (migration.hazards.length > 0) {
        migration.hazards.forEach(h => {
          lines.push(`     └─ ⚠️ ${h.type}`);
        });
      }

      lines.push("");
    }

    // Summary
    lines.push(`Summary: ${plan.summary.total} migration${plan.summary.total !== 1 ? 's' : ''} to ${plan.direction === 'up' ? 'apply' : 'rollback'}`);
    lines.push(`         (${plan.summary.transactional} transactional, ${plan.summary.nonTransactional} non-transactional)`);

    if (plan.summary.hazardCount > 0) {
      lines.push(`Warnings: ${plan.summary.hazardCount} hazardous operation${plan.summary.hazardCount !== 1 ? 's' : ''} detected`);
    }

    if (plan.summary.warnings && plan.summary.warnings.length > 0) {
      for (const w of plan.summary.warnings) {
        lines.push(`Note: ${w}`);
      }
    }

    if (plan.dryRun) {
      lines.push("");
      lines.push("🔍 DRY RUN MODE - No changes will be applied");
    }

    return lines.join("\n");
  }
}
