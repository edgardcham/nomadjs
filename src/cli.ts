import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { Migrator } from "./core/migrator.js";
import { Planner } from "./core/planner.js";
import { timestampedFilename, writeSqlTemplate, writeDefaultConfig, ConfigFormat } from "./core/files.js";
import { resolveRuntimeConfig } from "./config.js";
import { createDriver } from "./driver/factory.js";
import { formatExitCodesHelp, ConnectionError, ParseConfigError, DriftError, MissingFileError } from "./core/errors.js";
import type { Config } from "./config.js";
import { logger } from "./utils/logger.js";
import { formatCliError } from "./utils/format-error.js";
import { runDoctor, type DoctorReport } from "./core/doctor.js";

type BaseArgs = {
  url?: string;
  dir?: string;
  table?: string;
  schema?: string;
  config?: string;
  allowDrift?: boolean;
  autoNotx?: boolean;
  lockTimeout?: number;
};

type UpArgs = BaseArgs & { limit?: number };

type DownArgs = BaseArgs & { count?: number };

type ToArgs = BaseArgs & { version: number };

type PlanArgs = BaseArgs & {
  up?: boolean;
  down?: boolean | number;
  to?: string;
  json?: boolean;
  dryRun?: boolean;
};

async function withMigrator<T>(args: BaseArgs, fn: (migrator: Migrator) => Promise<T>): Promise<T> {
  const runtime = resolveRuntimeConfig({
    cli: {
      url: args.url,
      dir: args.dir,
      table: args.table,
      schema: args.schema
    },
    cwd: process.cwd(),
    configPath: args.config
  });

  if (!runtime.url) {
    throw new Error("DATABASE_URL is not set (provide via --url, config file, or environment variable)");
  }

  const timeoutEnv = process.env.NOMAD_PG_CONNECT_TIMEOUT_MS;
  let connectTimeout: number | undefined;
  if (timeoutEnv) {
    const ms = parseInt(timeoutEnv, 10);
    if (!Number.isNaN(ms) && ms > 0) {
      connectTimeout = ms;
    }
  }

  const config: Config = {
    driver: "postgres",
    url: runtime.url,
    dir: runtime.dir,
    table: runtime.table,
    schema: runtime.schema,
    allowDrift: args.allowDrift || process.env.NOMAD_ALLOW_DRIFT === "true",
    autoNotx: args.autoNotx || process.env.NOMAD_AUTO_NOTX === "true",
    lockTimeout: args.lockTimeout || parseInt(process.env.NOMAD_LOCK_TIMEOUT || "30000", 10),
    verbose: (args as any).verbose === true,
    eventsJson: (args as any)["events-json"] === true
  };

  const driver = createDriver(config, { connectTimeoutMs: connectTimeout });
  const pool = driver.getPool();
  const migrator = new Migrator(config, pool, driver);

  try {
    // Test connection before proceeding
    try {
      await pool.query('SELECT 1');
    } catch (error: any) {
      // Differentiate between different types of database errors
      const message = error.message || "Failed to connect to database";

      // Connection refused, network errors
      if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND' ||
          error.code === 'ETIMEDOUT' || error.code === 'ENETUNREACH') {
        throw new ConnectionError(`Connection failed: ${message}`);
      }

      // Authentication errors
      if (error.code === '28P01' || error.code === '28000' ||
          message.includes('authentication') || message.includes('password')) {
        throw new ConnectionError(`Authentication failed: ${message}`);
      }

      // Database doesn't exist
      if (error.code === '3D000' || message.includes('does not exist')) {
        throw new ConnectionError(`Database error: ${message}`);
      }

      // Invalid connection string
      if (message.includes('invalid') || message.includes('malformed')) {
        throw new ParseConfigError(`Invalid connection URL: ${message}`);
      }

      // Default to connection error for other cases
      throw new ConnectionError(message);
    }

    return await fn(migrator);
  } finally {
    await driver.close();
  }
}

(async () => {
  const cli = yargs(hideBin(process.argv))
    .scriptName("nomad")
    .strict()
    .wrap(100)
    .option("url", { type: "string", describe: "DB connection string (overrides DATABASE_URL)" })
    .option("dir", { type: "string", default: "migrations", describe: "Migrations directory" })
    .option("table", { type: "string", describe: "Version table name" })
    .option("schema", { type: "string", describe: "Database schema (default: public)" })
    .option("config", { type: "string", describe: "Path to config file (nomad.toml or nomad.json)" })
    .option("allow-drift", { type: "boolean", describe: "Allow migrations with checksum mismatches (DANGEROUS)" })
    .option("auto-notx", { type: "boolean", describe: "Auto-disable transactions for hazardous operations" })
    .option("lock-timeout", { type: "number", describe: "Timeout for acquiring migration lock in ms (default: 30000)" })
    .option("tags", { type: "string", describe: "Filter by tags (comma-separated, OR logic)" })
    .option("only-tagged", { type: "boolean", describe: "Include only migrations that have tags" })
    .option("include-ancestors", { type: "boolean", describe: "With --tags, include earlier pending migrations up to the first match" })
    .option("verbose", { type: "boolean", describe: "Verbose execution with per-statement logs and timings" })
    .option("events-json", { type: "boolean", describe: "Stream newline-delimited JSON events to stdout" })
    .epilogue(`Exit Codes:\n${formatExitCodesHelp()}`);

  cli.command(
    "status",
    "Show migration status",
    (yy) =>
      yy.option("json", {
        type: "boolean",
        describe: "Output status as JSON"
      }),
    async (argv) => {
      await withMigrator(argv as BaseArgs, async (migrator) => {
        const filter = parseTagFilter(argv.tags as string | undefined, (argv as any)["only-tagged"]);
        const rows = await migrator.status(filter);

        if ((argv as any).json) {
          // Output as JSON (convert BigInt to string for serialization)
          console.log(JSON.stringify(rows, (_, v) =>
            typeof v === 'bigint' ? v.toString() : v, 2
          ));
        } else {
          // Human-readable format
          for (const row of rows) {
            const driftMarker = row.hasDrift ? " [DRIFT]" : row.isMissing ? " [MISSING]" : "";
            console.log(
              `${row.applied ? "applied" : "pending"}\t${row.version}\t${row.name}${driftMarker}${row.appliedAt ? `\t${row.appliedAt.toISOString()}` : ""}`
            );
          }
        }
      });
    }
  );

  cli.command(
    "up [limit]",
    "Apply pending migrations (optionally first N)",
    (yy) => yy.positional("limit", { type: "number" }),
    async (argv) => {
      const limit = typeof argv.limit === "number" ? argv.limit : undefined;
      const filter = parseTagFilter(argv.tags as string | undefined, (argv as any)["only-tagged"]);
      const includeAncestors = (argv as any)["include-ancestors"] === true;
      await withMigrator(argv as UpArgs, (migrator) => migrator.up(limit, filter, includeAncestors));
    }
  );

  cli.command(
    "down [count]",
    "Rollback last N migrations (default 1)",
    (yy) => yy.positional("count", { type: "number", default: 1 }),
    async (argv) => {
      const count = typeof argv.count === "number" ? argv.count : 1;
      const filter = parseTagFilter(argv.tags as string | undefined, (argv as any)["only-tagged"]);
      await withMigrator(argv as DownArgs, (migrator) => migrator.down(count, filter));
    }
  );

  cli.command(
    "to <target>",
    "Migrate to a specific version",
    (yy) => yy.positional("target", { type: "string", demandOption: true }),
    async (argv) => {
      const vstr = String((argv as any).target || "").trim();
      if (!/^\d+$/.test(vstr)) {
        throw new ParseConfigError(`Invalid version: ${vstr}. Expected numeric timestamp (YYYYMMDDHHMMSS).`);
      }
      const version = BigInt(vstr);
      await withMigrator(argv as BaseArgs, (migrator) => migrator.to(version));
    }
  );

  cli.command(
    "redo",
    "Rollback and reapply the last migration",
    () => {},
    async (argv) => {
      await withMigrator(argv as BaseArgs, (migrator) => migrator.redo());
    }
  );

  cli.command(
    "verify",
    "Verify migration checksums",
    () => {},
    async (argv) => {
      await withMigrator(argv as BaseArgs, async (migrator) => {
        const result = await migrator.verify();
        if (result.valid) {
          console.log("✓ All migration checksums valid");
        } else {
          console.log(`✗ ${result.driftCount} migrations with drift, ${result.missingCount} missing`);
          if (result.driftedMigrations.length > 0) {
            console.log("\nDrifted migrations:");
            for (const m of result.driftedMigrations) {
              console.log(`  - ${m.version}: ${m.name}`);
            }
          }
          if (result.missingMigrations.length > 0) {
            console.log("\nMissing migrations:");
            for (const m of result.missingMigrations) {
              console.log(`  - ${m.version}: ${m.name}`);
            }
          }
          throw result.missingCount > 0
            ? new MissingFileError(result.missingMigrations.map(m => `${m.version}_${m.name}`))
            : new DriftError(result.driftedMigrations.map(m => `${m.version}_${m.name}`));
        }
      });
    }
  );

  cli.command(
    "plan [direction]",
    "Preview migration plan without executing",
    (yy) =>
      yy
        .positional("direction", {
          type: "string",
          choices: ["up", "down"],
          default: "up",
          describe: "Direction to plan (up or down)"
        })
        .option("limit", {
          type: "number",
          describe: "Limit number of up migrations to plan"
        })
        .option("count", {
          type: "number",
          describe: "Number of down migrations to plan"
        })
        .option("to", {
          type: "string",
          describe: "Target version to plan to"
        })
        .option("json", {
          type: "boolean",
          describe: "Output plan as JSON"
        })
        .option("dry-run", {
          type: "boolean",
          describe: "Execute migrations but rollback (test run)"
        }),
    async (argv) => {
      await withMigrator(argv as PlanArgs, async (migrator) => {
        const planner = new Planner((argv as BaseArgs).autoNotx);
        let plan;

        if (argv.to) {
          // Plan to specific version
          const targetVersion = BigInt(argv.to);
          plan = await migrator.planTo({
            version: targetVersion,
            format: argv.json ? "json" : "human",
            dryRun: argv.dryRun
          });
          const filter = parseTagFilter(argv.tags as string | undefined, (argv as any)["only-tagged"]);
          if (filter) {
            logger.warn("Tag filters are ignored for 'plan --to'.");
          }
        } else if (argv.direction === "down") {
          // Plan down migrations
          const count = argv.count || 1;
          const filter = parseTagFilter(argv.tags as string | undefined, (argv as any)["only-tagged"]);
          plan = await migrator.planDown({
            count,
            format: argv.json ? "json" : "human",
            dryRun: argv.dryRun,
            filter
          });
        } else {
          // Plan up migrations (default)
          const filter = parseTagFilter(argv.tags as string | undefined, (argv as any)["only-tagged"]);
          plan = await migrator.planUp({
            limit: argv.limit,
            format: argv.json ? "json" : "human",
            dryRun: argv.dryRun,
            filter,
            includeAncestors: (argv as any)["include-ancestors"] === true
          });
        }

        if (argv.json) {
          // Output JSON format (convert BigInt to string for JSON serialization)
          console.log(JSON.stringify(plan, (_, v) =>
            typeof v === 'bigint' ? v.toString() : v, 2
          ));
        } else {
          // Output human-readable format
          console.log(planner.formatPlanOutput(plan));
        }

        // Throw error if there are issues
        if (plan.errors && plan.errors.length > 0) {
          throw new Error(`Migration plan failed: ${plan.errors.join(", ")}`);
        }
      });
    }
  );

  cli.command(
    "create <name>",
    "Create a new timestamped SQL migration",
    (yy) =>
      yy
        .positional("name", { type: "string", demandOption: true })
        .option("block", {
          type: "boolean",
          describe: "Use a block template for multi-line statements"
        }),
    async (argv) => {
      const runtime = resolveRuntimeConfig({
        cli: { dir: argv.dir as string | undefined },
        cwd: process.cwd(),
        configPath: argv.config as string | undefined
      });
      const file = timestampedFilename(runtime.dir, argv.name as string);
      mkdirSync(dirname(file), { recursive: true });
      writeSqlTemplate(file, { block: (argv as any).block === true });
      console.log(file);
    }
  );

  cli.command(
    "init-config [format]",
    "Create a default config file (nomad.toml or nomad.json)",
    (yy) =>
      yy
        .positional("format", {
          type: "string",
          choices: ["toml", "json"],
          default: "toml",
          describe: "Config file format"
        })
        .option("output", {
          type: "string",
          alias: "o",
          describe: "Output filename (default: nomad.toml or nomad.json)"
        }),
    async (argv) => {
      const format = (argv.format as ConfigFormat) || "toml";
      const filename = argv.output || `nomad.${format}`;
      const isDefaultName = filename === "nomad.toml" || filename === "nomad.json";

      try {
        writeDefaultConfig(filename, format);
        console.log(`Created ${filename}`);

        if (!isDefaultName) {
          console.log(`\nNote: To use this config file, specify it with the --config flag:`);
          console.log(`  nomad --config ${filename} <command>`);
        }
      } catch (error: any) {
        if (error.code === "EEXIST") {
          throw new ParseConfigError(`${filename} already exists. Remove it first or use a different filename with --output.`);
        }
        throw error;
      }
    }
  );

  cli.command(
    "doctor",
    "Run environment diagnostics",
    (yy) =>
      yy
        .option("json", {
          type: "boolean",
          describe: "Output report as JSON"
        })
        .option("fix", {
          type: "boolean",
          describe: "Attempt safe fixes (create schema/table)"
        }),
    async (argv) => {
      const runtime = resolveRuntimeConfig({
        cli: {
          url: argv.url as string | undefined,
          dir: argv.dir as string | undefined,
          table: argv.table as string | undefined,
          schema: argv.schema as string | undefined
        },
        cwd: process.cwd(),
        configPath: argv.config as string | undefined
      });

      if (!runtime.url) {
        throw new ParseConfigError("DATABASE_URL is not set (provide via --url, config file, or environment variable)");
      }

      const pool = makePool(runtime.url);
      const config: Config = {
        driver: "postgres",
        url: runtime.url,
        dir: runtime.dir,
        table: runtime.table,
        schema: runtime.schema,
        allowDrift: argv.allowDrift || process.env.NOMAD_ALLOW_DRIFT === "true",
        autoNotx: argv.autoNotx || process.env.NOMAD_AUTO_NOTX === "true",
        lockTimeout: argv.lockTimeout || parseInt(process.env.NOMAD_LOCK_TIMEOUT || "30000", 10)
      };

      try {
        const report = await runDoctor(config, pool, { fix: argv.fix === true });
        const connectionFailure = report.checks.find(check => check.id === "connect" && check.status === "fail");

        if (argv.json) {
          const jsonReport = serializeDoctorReport(report);
          console.log(JSON.stringify(jsonReport, null, 2));
        } else {
          printDoctorReport(report);
        }

        if (connectionFailure) {
          throw new ConnectionError(connectionFailure.message);
        }
      } finally {
        await pool.end();
      }
    }
  );

  await cli
    .demandCommand(1)
    .help()
    .fail((msg, err, yargs) => {
      // If there's an error with an exitCode, use it
      if (err && typeof (err as any).exitCode === 'number') {
        const exitErr = err as any;
        const formatted = formatCliError(exitErr) || msg;
        if (formatted) {
          logger.error(formatted);
        }
        process.exit(exitErr.exitCode);
      } else {
        // Otherwise show help and exit with code 1
        const formatted = formatCliError(err) || msg || 'Unknown error';
        logger.error(formatted);
        process.exit(1);
      }
    })
    .parseAsync();
})();

function redactConnectionString(url?: string): string | undefined {
  if (!url) return undefined;
  try {
    const parsed = new URL(url);
    if (parsed.password) {
      parsed.password = "****";
    }
    return parsed.toString();
  } catch {
    return url.replace(/:(?:[^:@/]+)@/, ":****@");
  }
}

function serializeDoctorReport(report: DoctorReport) {
  return {
    ok: report.ok,
    summary: report.summary,
    config: report.config ? {
      ...report.config,
      url: redactConnectionString(report.config.url)
    } : undefined,
    environment: report.environment,
    checks: report.checks
  };
}

function printDoctorReport(report: DoctorReport): void {
  if (report.config) {
    const redactedUrl = redactConnectionString(report.config.url);
    logger.action("Target Environment");
    if (redactedUrl) {
      logger.info(`Database: ${redactedUrl}`);
    }
    logger.info(`Schema: ${report.config.schema}`);
    logger.info(`Version table: ${report.config.table}`);
    logger.info(`Migrations dir: ${report.config.dir}`);
  }

  logger.action("Diagnostics");
  for (const check of report.checks) {
    const line = `${check.title}: ${check.message}`;
    if (check.status === "pass") {
      logger.success(`PASS  ${line}`);
    } else if (check.status === "warn") {
      logger.warn(`WARN  ${line}`);
    } else {
      logger.error(`FAIL  ${line}`);
    }
    if (check.suggestions?.length) {
      for (const suggestion of check.suggestions) {
        logger.info(`  → ${suggestion}`);
      }
    }
  }

  const summaryLine = `Summary: ${report.summary.pass} pass, ${report.summary.warn} warn, ${report.summary.fail} fail`;
  if (report.summary.fail > 0) {
    logger.error(summaryLine);
  } else if (report.summary.warn > 0) {
    logger.warn(summaryLine);
  } else {
    logger.success(summaryLine);
  }
}

function parseTagFilter(tagsArg?: string, onlyTagged?: boolean) {
  const tags = (tagsArg || "")
    .split(/[,\s]+/)
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);
  const unique = Array.from(new Set(tags));
  if (unique.length === 0 && !onlyTagged) return undefined;
  return { tags: unique.length ? unique : undefined, onlyTagged: !!onlyTagged };
}
