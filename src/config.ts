import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { ParseConfigError } from "./core/errors.js";
import { logger } from "./utils/logger.js";

export type NomadConfigFile = {
  database?: {
    url?: string;
    table?: string;
    schema?: string;
  };
  migrations?: {
    dir?: string;
  };
};

export type RuntimeConfig = {
  url?: string;
  dir: string;
  table?: string;
  schema?: string;
};

export interface Config {
  driver: "postgres";
  url: string;
  dir: string;
  table?: string;
  schema?: string;
  allowDrift?: boolean;
  autoNotx?: boolean;
  lockTimeout?: number;
  verbose?: boolean;
  eventsJson?: boolean;
}

export type ResolveConfigOptions = {
  cli: {
    url?: string;
    dir?: string;
    table?: string;
    schema?: string;
  };
  cwd: string;
  configPath?: string;
};

const DEFAULT_CONFIG_FILES = ["nomad.toml", "nomad.json"];
let loadedEnvPath: string | null = null;

// For testing - reset the loaded env path
export function resetConfigCache(): void {
  loadedEnvPath = null;
}

export function resolveRuntimeConfig(opts: ResolveConfigOptions): RuntimeConfig {
  loadDotEnvIfPresent(opts.cwd);
  const fileConfig = loadConfigFile(opts);

  const envUrl = process.env.NOMAD_DATABASE_URL ?? process.env.DATABASE_URL;
  const envDir = process.env.NOMAD_MIGRATIONS_DIR;
  const envTable = process.env.NOMAD_DB_TABLE;
  const envSchema = process.env.NOMAD_DB_SCHEMA;

  // Get raw values first
  let url = opts.cli.url ?? envUrl ?? fileConfig?.database?.url;
  let dir = opts.cli.dir ?? envDir ?? fileConfig?.migrations?.dir ?? "migrations";
  let table = opts.cli.table ?? envTable ?? fileConfig?.database?.table;
  let schema = opts.cli.schema ?? envSchema ?? fileConfig?.database?.schema ?? "public";

  // Expand environment variables in all config values
  if (url) {
    url = expandEnvVars(url);
  }
  if (dir) {
    dir = expandEnvVars(dir);
  }
  if (table) {
    table = expandEnvVars(table);
  }
  if (schema) {
    schema = expandEnvVars(schema);
  }

  return {
    url,
    dir,
    table,
    schema
  };
}

function loadConfigFile(opts: ResolveConfigOptions): NomadConfigFile | undefined {
  const candidates = resolveConfigPaths(opts);
  for (const filePath of candidates) {
    if (!existsSync(filePath)) continue;
    const raw = readFileSync(filePath, "utf8");
    if (filePath.endsWith(".json")) {
      return parseJsonConfig(raw, filePath);
    }
    if (filePath.endsWith(".toml")) {
      return parseTomlConfig(raw, filePath);
    }
  }
  return undefined;
}

function resolveConfigPaths(opts: ResolveConfigOptions): string[] {
  if (opts.configPath) {
    return [resolve(opts.cwd, opts.configPath)];
  }
  return DEFAULT_CONFIG_FILES.map((name) => resolve(opts.cwd, name));
}

function parseJsonConfig(raw: string, filePath: string): NomadConfigFile {
  try {
    const data = JSON.parse(raw);
    return normaliseConfigShape(data, filePath);
  } catch (error) {
    throw new ParseConfigError(`Failed to parse ${filePath}: ${(error as Error).message}`);
  }
}

function parseTomlConfig(raw: string, filePath: string): NomadConfigFile {
  const config: NomadConfigFile = {};
  let currentSection: "database" | "migrations" | undefined;
  const lines = raw.split(/\r?\n/);

  for (const originalLine of lines) {
    const line = originalLine.trim();
    if (!line || line.startsWith("#") || line.startsWith(";")) continue;
    const sectionMatch = line.match(/^\[(.+)]$/);
    if (sectionMatch) {
      const section = sectionMatch[1];
      if (section === "database" || section === "migrations") {
        currentSection = section;
        if (!config[currentSection]) {
          config[currentSection] = {} as any;
        }
        continue;
      }
      continue;
    }

    const kvMatch = line.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$/);
    if (!kvMatch) {
      throw new ParseConfigError(`Invalid TOML line in ${filePath}: ${originalLine}`);
    }
    const key = kvMatch[1];
    const valueLiteral = kvMatch[2];
    if (!key || valueLiteral === undefined) {
      throw new ParseConfigError(`Invalid TOML assignment in ${filePath}: ${originalLine}`);
    }
    const value = parseTomlValue(valueLiteral);
    if (!currentSection) {
      continue;
    }
    const section = config[currentSection] as Record<string, unknown>;
    section[key] = value;
  }

  return normaliseConfigShape(config, filePath);
}

function parseTomlValue(literal: string): unknown {
  const trimmed = literal.trim();
  if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1);
  }
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  const number = Number(trimmed);
  if (!Number.isNaN(number)) return number;
  return trimmed;
}

function normaliseConfigShape(input: unknown, filePath: string): NomadConfigFile {
  if (!input || typeof input !== "object") {
    throw new ParseConfigError(`Config at ${filePath} must be an object`);
  }
  const out: NomadConfigFile = {};
  const database = (input as any).database;
  if (database && typeof database === "object") {
    out.database = {};
    if (typeof database.url === "string") out.database.url = database.url;
    if (typeof database.table === "string") out.database.table = database.table;
    if (typeof database.schema === "string") out.database.schema = database.schema;
  }
  const migrations = (input as any).migrations;
  if (migrations && typeof migrations === "object") {
    out.migrations = {};
    if (typeof migrations.dir === "string") out.migrations.dir = migrations.dir;
  }
  return out;
}

function loadDotEnvIfPresent(cwd: string): void {
  const envPath = resolve(cwd, ".env");
  if (loadedEnvPath === envPath) return;
  if (!existsSync(envPath)) {
    loadedEnvPath = envPath;
    return;
  }
  loadedEnvPath = envPath;
  const raw = readFileSync(envPath, "utf8");
  const lines = raw.split(/\r?\n/);
  for (const originalLine of lines) {
    const line = originalLine.trim();
    if (!line || line.startsWith("#")) continue;
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!match) continue;
    const key = match[1] ?? "";
    const value = match[2] ?? "";
    if (!key) continue;
    if (process.env[key] !== undefined) continue;
    process.env[key] = stripQuotes(value);
  }
}

function stripQuotes(value: string): string {
  const trimmed = value.trim();
  if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function expandEnvVars(value: string): string {
  // Support ${VAR_NAME} and $VAR_NAME syntax
  return value.replace(/\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (match, p1, p2) => {
    const varName = p1 || p2;
    const envValue = process.env[varName];
    if (envValue === undefined) {
      logger.warn(`Environment variable ${varName} is not defined`);
      return match; // Keep original if not found
    }
    return envValue;
  });
}
