import { readdirSync, writeFileSync } from "node:fs";
import { join, basename } from "node:path";

export function listMigrationFiles(dir: string): string[] {
  const files = readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
    .map((entry) => join(dir, entry.name));
  files.sort((a, b) => basename(a).localeCompare(basename(b)));
  return files;
}

export function filenameToVersion(filePath: string): number {
  const name = basename(filePath, ".sql");
  const match = name.match(/^(\d+)_/);
  if (!match) throw new Error(`Invalid migration filename: ${name}`);
  return Number(match[1]);
}

export function timestampedFilename(dir: string, name: string): string {
  const stamp = new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
  const file = `${stamp}_${name}.sql`;
  return join(dir, file);
}

export function writeSqlTemplate(filePath: string): void {
  const tpl = `-- +nomad Up
-- write your up migration here

-- +nomad Down
-- write your down migration here
`;
  writeFileSync(filePath, tpl, { encoding: "utf8", flag: "wx" });
}

export type ConfigFormat = "toml" | "json";

export function writeDefaultConfig(filePath: string, format: ConfigFormat = "toml"): void {
  let content: string;

  if (format === "toml") {
    content = `# NomadJS configuration file
# https://github.com/edgardcham/nomadjs

[database]
# Database connection URL - supports environment variable substitution
# Examples of different approaches:

# 1. Full URL hardcoded (not recommended for production)
# url = "postgres://user:password@localhost:5432/dbname"

# 2. Full URL from single env var
# url = "\${DATABASE_URL}"

# 3. Password only from env var
# url = "postgres://postgres:\${DB_PASSWORD}@localhost:5432/nomaddb"

# 4. Multiple env vars for different parts
# url = "postgres://\${DB_USER}:\${DB_PASSWORD}@\${DB_HOST}:\${DB_PORT}/\${DB_NAME}"

# 5. Mix of hardcoded and env vars
# url = "postgres://postgres:\${DB_PASSWORD}@\${DB_HOST}:5432/myapp"

# Database schema name (default: public)
# schema = "public"

# Version tracking table name (default: nomad_migrations)
# table = "schema_migrations"

[migrations]
# Directory containing migration files (default: migrations)
# Can also use env vars: dir = "\${MIGRATIONS_DIR}"
dir = "migrations"
`;
  } else {
    // JSON with comments (supported by many tools even though not standard)
    content = `// NomadJS configuration file
// https://github.com/edgardcham/nomadjs
{
  "database": {
    // Database connection URL - supports environment variable substitution
    // Examples:
    // "url": "\${DATABASE_URL}",  // Full URL from env var
    // "url": "postgres://\${DB_USER}:\${DB_PASSWORD}@\${DB_HOST}:\${DB_PORT}/\${DB_NAME}",
    // "url": "postgres://postgres:\${DB_PASSWORD}@localhost:5432/nomaddb",

    // Database schema name (default: public)
    // "schema": "public",

    // Version tracking table name (default: nomad_migrations)
    // "table": "schema_migrations"
  },
  "migrations": {
    // Directory containing migration files (default: migrations)
    // Can also use env vars: "dir": "\${MIGRATIONS_DIR}"
    "dir": "migrations"
  }
}
`;
  }

  writeFileSync(filePath, content, { encoding: "utf8", flag: "wx" });
}
