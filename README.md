# Nomad

[![CI](https://github.com/edgardcham/nomadjs/actions/workflows/ci.yml/badge.svg)](https://github.com/edgardcham/nomadjs/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/%40loopfox%2Fnomad.svg)](https://www.npmjs.com/package/@loopfox/nomad)
[![Node >= 20](https://img.shields.io/badge/node-%3E%3D20-brightgreen.svg)](#installation)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Production-ready SQL migration tool for Node.js with checksums, transaction control, and first-class PostgreSQL/MySQL/SQLite support.

**Key Features:**
- SHA-256 checksums for drift detection
- Automatic transaction wrapping with hazard detection
- Detects operations that can't run in transactions (CREATE INDEX CONCURRENTLY, etc.)
- Triple drivers: PostgreSQL (advisory locks), MySQL (named locks), and SQLite via better-sqlite3 (file-backed)
- Advanced PostgreSQL parsing (dollar quotes, E-strings, COPY blocks)
- TOML/JSON configuration with env var substitution
- 390+ tests across both drivers with comprehensive edge-case coverage
- Performance optimized with migration file caching
- Configurable database schema/table names
- Standardized exit codes for CI/CD integration
- JSON output for automation and monitoring
- File:line:column error reporting for instant debugging
- Color-aware CLI output respecting `NO_COLOR`/`NOMAD_NO_COLOR`

## Installation

Requirements: Node.js 20 or newer.

After publish (recommended for most users):

```bash
npm install -g @loopfox/nomad
nomad --help
```

For local development of this repository:

```bash
npm install
npm run build
npm link            # exposes the `nomad` CLI globally during development
```

To unlink later, run `npm unlink --global nomadjs` and `npm unlink nomadjs` in any project where it was linked.

## Database Prerequisites

NomadJS ships with native drivers for PostgreSQL, MySQL, and SQLite. Use the driver that matches your connection string, or set it explicitly with `--driver`, `NOMAD_DRIVER`, or in your config file.

- **PostgreSQL**: Uses advisory locks via `pg_try_advisory_lock`. Supports schemas (default `public`) and transactional DDL.
- **MySQL (8.0+)**: Uses named locks via `GET_LOCK`. DDL executes outside transactions automatically (`supportsTransactionalDDL = false`).
- **SQLite (3.39+)**: Uses a single file on disk (or `:memory:`). The driver coordinates access with an internal lock table and always executes outside transactions.

- For Postgres/MySQL, create the target database yourself (Nomad manages only the version-tracking table).
- PostgreSQL example:
  ```bash
  psql -U postgres -d postgres -c "CREATE DATABASE nomaddb;"
  export DATABASE_URL="postgres://postgres@localhost/nomaddb"
  ```
- MySQL example:
  ```bash
  mysql -uroot -pnomad -e "CREATE DATABASE nomad_test;"
  export DATABASE_URL="mysql://root:nomad@localhost:3306/nomad_test"
  ```
- SQLite example (database file is created automatically):
  ```bash
  export DATABASE_URL="sqlite:///$(pwd)/nomad.sqlite"
  ```
- Alternatively, pass `--url` to each command instead of exporting `DATABASE_URL`.

### Driver Selection

Nomad chooses the database driver in this order:
1. `--driver` CLI flag (`postgres`, `mysql`, or `sqlite`)
2. `NOMAD_DRIVER` environment variable
3. `database.driver` field in `nomad.toml` / `nomad.json`
4. URL scheme (`postgres://`, `mysql://`, `sqlite://`, `file:`, or `*.sqlite` path)
5. Default: `postgres`

Examples:

```bash
# Force MySQL for the current command
nomad status --driver mysql --url "mysql://root:nomad@localhost:3306/nomad_test"

# Set the default driver for this shell session
export NOMAD_DRIVER=mysql
nomad up --url "mysql://root:nomad@localhost:3306/nomad_test"

# Switch to SQLite with an explicit file path
nomad plan --driver sqlite --url "sqlite:///$(pwd)/nomad.sqlite"

# Override back to Postgres
nomad plan --driver postgres --url "postgres://postgres@localhost/nomaddb"
```


## Creating & Running Migrations

```bash
nomad create init_schema
nomad create seed_data --block   # scaffold COPY stdin block template
# edit migrations/<timestamp>_init_schema.sql to add real SQL
nomad up
nomad status
```

Example migration:

```sql
-- +nomad Up
CREATE TABLE users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +nomad Down
DROP TABLE users;
```

### Advanced Migration Features

**Transaction Control & Hazard Detection** - Automatically detects hazardous operations and requires explicit `notx`:
```sql
-- +nomad Up
-- +nomad notx  -- Required for CREATE INDEX CONCURRENTLY
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);

-- Without notx, you'll get an error:
-- "Hazardous operation detected that cannot run in a transaction"
```

For development, use `--auto-notx` to automatically disable transactions when hazards are detected.

**Migration Planning** - Preview migrations before execution:
```bash
# See what will be applied
nomad plan

# Output:
# Migration Plan: UP
# ══════════════════════════════════════════════════════
#
# [TX] ↑ 20240101120000_create_users.sql
#      └─ CREATE TABLE users (id INT PRIMARY KEY);
#
# [NO-TX] ↑ 20240103140000_create_index.sql ⚠️ HAZARD
#      └─ CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
#      └─ ⚠️ CREATE_INDEX_CONCURRENTLY
#
# Summary: 2 migrations to apply (1 transactional, 1 non-transactional)
# Warnings: 1 hazardous operation detected

# Plan rollback
nomad plan down --count 2

# Get JSON output for CI/automation
nomad plan --json
```

**Block Statements** - For COPY and other multi-line statements:
```sql
-- +nomad Up
-- +nomad block
COPY users (id, name) FROM stdin;
1	John Doe
2	Jane Smith
\.
-- +nomad endblock
```

Generate this template automatically with `nomad create <name> --block`.

**PostgreSQL Support** - Full support for advanced PostgreSQL features:
```sql
-- Dollar quotes for functions
CREATE FUNCTION get_user(uid INT) RETURNS TEXT AS $$
BEGIN
  RETURN (SELECT name FROM users WHERE id = uid);
END;
$$ LANGUAGE plpgsql;

-- E-strings with escape sequences
INSERT INTO logs VALUES (E'Error:\\nFile not found');
```

`nomad status` shows which migrations are applied vs pending. `nomad down 1` rolls back the most recent applied migration. If you ask for more rollbacks than exist (e.g. `nomad down 5` when only one migration has been applied) the CLI stops after clearing the available history—no error is thrown.

`nomad redo` is useful for testing migrations during development - it rolls back and immediately reapplies the last migration. This is safe because it only affects the most recent migration, avoiding dependency issues with earlier migrations. Note: redo operates on the last applied migration only (no specific version selection).

## Commands

| Command | Description |
|---------|-------------|
| `nomad init-config [format]` | Create config file template (TOML/JSON) |
| `nomad create <name>` | Create timestamped migration file (`--block` adds COPY block template) |
| `nomad status` | Show migration status with drift detection |
| `nomad plan [direction]` | Preview migration plan without executing |
| `nomad up [limit]` | Apply pending migrations |
| `nomad down [count]` | Rollback migrations (default: 1) |
| `nomad to <version>` | Migrate to specific version |
| `nomad redo` | Rollback and reapply the last migration (safe for development; last migration only) |
| `nomad verify` | Verify migration checksums |
| `nomad doctor` | Diagnose configuration and database readiness |

All commands accept `--url`, `--dir`, `--table`, `--allow-drift`, `--auto-notx`, and the database `--driver` flag (`postgres` or `mysql`).

📚 **Full Documentation:** See [CLI_DOCUMENTATION.md](./CLI_DOCUMENTATION.md) for detailed usage, directives, and examples.


## CLI Output & Colors

Nomad prints color-coded status lines (success, warnings, notes) when running in a TTY. Colors automatically turn off when piping output or when you set either `NO_COLOR` or `NOMAD_NO_COLOR=true`. Set `NOMAD_NO_COLOR=false` (or `0`) to force colors back on for automated environments that still support ANSI escape codes.


## Configuration

Nomad reads settings in this order (highest priority first):

1. CLI flags (`--url`, `--dir`, `--table`, `--driver`, `--config`).
2. Environment variables (`DATABASE_URL`, `NOMAD_DATABASE_URL`, `NOMAD_DRIVER`, `NOMAD_MIGRATIONS_DIR`, `NOMAD_DB_TABLE`).
3. Config file (`nomad.toml` or `nomad.json`).
4. Defaults (`dir` defaults to `migrations`, driver defaults to `postgres`).

Place a `nomad.toml` or `nomad.json` next to your project to avoid repeating options. Example TOML:

```toml
[database]
url = "postgres://postgres@localhost/nomaddb"
# driver = "postgres"            # optional: "postgres", "mysql", or "sqlite"
table = "nomad_db_version"

[migrations]
dir = "./migrations"
```

```toml
# MySQL example
[database]
driver = "mysql"
url = "mysql://root:password@localhost:3306/nomad_test"

[migrations]
dir = "./migrations"
```

```toml
# SQLite example
[database]
driver = "sqlite"
url = "sqlite:///\${PWD}/nomad.sqlite"

[migrations]
dir = "./migrations"
```

### Password & Environment Management

NomadJS automatically loads `.env` files and supports flexible environment variable substitution using `${VAR_NAME}` or `$VAR_NAME` syntax in config files. This means you can keep secrets in `.env` and reference them in your config!

#### Option 1: Full DATABASE_URL in .env (Simplest)
```dotenv
# .env
DATABASE_URL=postgres://postgres:secret@localhost:5432/nomaddb
```
No config file needed - NomadJS will use DATABASE_URL directly.

#### Option 2: Full URL reference in config
```toml
# nomad.toml
[database]
url = "${DATABASE_URL}"
```
```dotenv
# .env
DATABASE_URL=postgres://postgres:secret@localhost:5432/nomaddb
```

#### Option 3: Compose URL from multiple env vars (Recommended for production)
```toml
# nomad.toml - template with placeholders
[database]
url = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
```
```dotenv
# .env - actual values (automatically loaded)
DB_USER=postgres
DB_PASSWORD=secret
DB_HOST=localhost
DB_PORT=5432
DB_NAME=nomaddb
```
When you run `nomad` commands, it automatically:
1. Loads `.env` file
2. Reads `nomad.toml`
3. Replaces `${DB_USER}` etc. with values from `.env`
4. Result: `postgres://postgres:secret@localhost:5432/nomaddb`

#### Option 4: Mix hardcoded and env vars (Common for production)
```toml
# nomad.toml
[database]
url = "postgres://appuser:${DB_PASSWORD}@prod.example.com:5432/myapp"
```
```dotenv
# .env
DB_PASSWORD=super-secret-password
```

#### Option 5: Override config with environment
```toml
# nomad.toml - default connection
[database]
url = "postgres://postgres@localhost:5432/nomaddb"
```
```dotenv
# .env - override entire URL
DATABASE_URL=postgres://postgres:secret@prod-server:5432/nomaddb
```

The resolution order is: CLI flags → Environment variables → Config file (with env substitution) → Defaults

Override the config path with `--config path/to/nomad.toml` if you keep it elsewhere.

### Try It

1. Generate a config file in your project root:
   ```bash
   nomad init-config        # Creates nomad.toml
   nomad init-config json   # Creates nomad.json
   ```
   Or drop a config file manually:
   ```toml
   [database]
   url = "postgres://postgres@localhost/nomaddb"

   [migrations]
   dir = "./migrations"
   ```
2. (Optional) create a `.env` for secrets:
   ```dotenv
   DATABASE_URL=postgres://postgres:password@localhost/nomaddb
   NOMAD_MIGRATIONS_DIR=./migrations
   ```
3. Generate and edit a migration:
   ```bash
   nomad create add_users
   # edit migrations/<timestamp>_add_users.sql
   ```
4. Run it with your config:
   ```bash
   nomad up
   nomad status
   ```
5. Override per command when needed:
   ```bash
   nomad --config other.toml --dir ./alt up 1
   nomad --url postgres://other@localhost/alt status
   ```

Nomad always resolves settings as CLI > env vars > config file > defaults, so you can mix and match safely.

## Known Limitations

- **COPY FROM stdin**: Block statements with `COPY ... FROM stdin` are not supported. The PostgreSQL client library requires special streaming protocol for COPY operations. Use `INSERT` statements or `COPY FROM file` as alternatives.
- **Migration Isolation**: Each migration runs in its own database connection. State created in one migration (like temporary tables) is not available to subsequent migrations in the same `nomad up` run.
- **Schema Creation**: NomadJS will not create database schemas. Ensure your target schema exists before running migrations that reference it.

## Test Suite

Comprehensive test coverage with 300+ tests across all features:

```bash
npm test
```

To include the CLI end-to-end suites, point `DATABASE_URL` at a disposable database and opt in via `NOMAD_TEST_WITH_DB=true`:

```bash
DATABASE_URL="postgres://postgres@localhost/nomaddb" \\
NOMAD_TEST_WITH_DB=true \\
npm run test -- test/cli/exit-codes-integration.test.ts
```

**Test Coverage:**
- **Parser**: 65 tests
  - Enhanced SQL parser: 19 tests
  - Edge cases: 33 tests
  - PostgreSQL-specific: 12 tests
- **Transaction & Hazard Detection**: 51 tests
  - Hazard detection for 13 operation types
  - Transaction wrapping logic
  - Per-direction notx support
  - Auto-notx mode
- **Checksums**: 43 tests (fully integrated)
  - Core functionality: 19 tests
  - Edge cases: 24 tests (Unicode, large files, etc.)
  - Drift detection with --allow-drift flag
- **Redo Command**: 13 tests
  - Core redo functionality
  - Transaction and notx handling
  - Checksum verification with drift
  - Advisory lock management
- **Configuration**: Full coverage of env var substitution
- **Migrator**: Core transaction handling, rollbacks, checksum verification
- **CLI Integration**: Command-line interface and exit codes (opt-in with `NOMAD_TEST_WITH_DB=true`)

Unit and core integration tests run entirely in-memory; the CLI suites require a reachable PostgreSQL instance and are skipped unless you opt in.

## `nomad doctor`

Run Nomad's health check to verify filesystem, configuration, and database prerequisites:

```bash
nomad doctor

# JSON output for CI
nomad doctor --json

# Attempt to create missing schema/table automatically
nomad doctor --fix
```

Sample output:

```
▶ Target Environment
Connected to postgres://postgres@localhost/nomaddb as postgres
▶ Diagnostics
PASS  Database connection: Connected to nomaddb as postgres
PASS  Schema availability: Schema "public" exists
WARN  Migrations table: Version table "public"."nomad_migrations" does not exist
PASS  Advisory lock: Advisory lock acquired and released successfully
Summary: 3 pass, 1 warn, 0 fail
```

Warnings keep the exit code at 0 so you can surface issues without breaking CI. Use `nomad doctor --json` to capture machine-readable reports.

Optional fast connection failure / busy timeouts:
- **PostgreSQL**: set `NOMAD_PG_CONNECT_TIMEOUT_MS` (milliseconds) to abort quickly when hosts are unreachable.
  ```bash
  export NOMAD_PG_CONNECT_TIMEOUT_MS=3000
  ```
- **MySQL**: set `NOMAD_MYSQL_CONNECT_TIMEOUT_MS` to control the underlying driver connect timeout.
  ```bash
  export NOMAD_MYSQL_CONNECT_TIMEOUT_MS=5000
  ```
- **SQLite**: set `NOMAD_SQLITE_BUSY_TIMEOUT_MS` (milliseconds) to tune how long the driver waits on locked database files (default 5000).
  ```bash
  export NOMAD_SQLITE_BUSY_TIMEOUT_MS=2000
  ```

## Tag Filtering

You can tag migrations and selectively operate on them.

Add tags in SQL:

```sql
-- +nomad tags: seed, users
-- +nomad Up
INSERT INTO users VALUES (1, 'seed');
-- +nomad Down
DELETE FROM users WHERE id = 1;
```

Use filters:

```bash
nomad status --tags=seed            # show only seed migrations
nomad up --tags=seed               # apply only seed-tagged pending migrations
nomad up --tags=users --include-ancestors  # include earlier pending prerequisites
nomad up --verbose                         # per-statement timing and detailed logs
nomad down --tags=seed --count 2   # rollback only the head of seed-tagged migrations
nomad plan --tags=users            # preview only user-tagged migrations
nomad status --only-tagged         # show only migrations that have any tags
```

Notes:
- Matching is OR across tags and case-insensitive.
- Tags apply to the entire migration (both Up and Down).
- For `down`, Nomad only rolls back the contiguous head of matching migrations; it never skips over a newer, non-matching migration.
- For `up`, use `--include-ancestors` to pull in earlier pending migrations up to the first matching tag when prerequisites are needed.

## JSON Events

Nomad can stream newline-delimited JSON events for CI/observability.

```bash
# Human logs + JSON events to stdout
nomad up --events-json

# Combine with verbose for both streams
nomad up --events-json --verbose
```

Example events (mix of apply/down/verify):

```
{"event":"lock-acquired","ts":"2025-09-24T20:00:00.000Z"}
{"event":"apply-start","direction":"up","version":"20250923052647","name":"initialize_db","ts":"..."}
{"event":"stmt-run","direction":"up","version":"20250923052647","ms":5,"preview":"CREATE TABLE users ..."}
{"event":"apply-end","direction":"up","version":"20250923052647","name":"initialize_db","ms":23,"ts":"..."}
{"event":"apply-start","direction":"down","version":"20250923052647","name":"initialize_db","ts":"..."}
{"event":"apply-end","direction":"down","version":"20250923052647","name":"initialize_db","ms":12,"ts":"..."}
{"event":"verify-start","ts":"..."}
{"event":"verify-end","valid":true,"driftCount":0,"missingCount":0,"ts":"..."}
{"event":"lock-released","ts":"..."}
```

Event types:
- `lock-acquired` / `lock-released` track advisory locking across `up`, `down`, `to`, and `redo`.
- `apply-start` / `apply-end` fire for every migration with `direction` (`up` or `down`) and include duration (`ms`) on completion.
- `stmt-run` reports each SQL statement with execution time and a truncated preview.
- `verify-start` / `verify-end` wrap `nomad verify` runs; `verify-end` includes drift and missing counts.

Notes:
- Events are emitted to stdout as one JSON object per line (NDJSON).
- Human logs (including `--verbose`) remain colorized and readable.
- Multi-database roadmap: PostgreSQL, MySQL, and SQLite drivers ship today; future enhancements are tracked in [MySQL_SUPPORT.md](MySQL_SUPPORT.md) and [SQLite_SUPPORT.md](SQLite_SUPPORT.md).

## Error Reporting

Nomad pinpoints failing SQL with `path:line:column` context so you can jump straight to the offending statement. Example:

```
migrations/20240101120000_create_users.sql:42:5 - Failed UP 20240101120000 (create_users): syntax error at or near "FROM"
```

Both parser errors and PostgreSQL execution errors include location details when available.

## Publishing Notes

When you are ready to publish to npm:

```bash
npm run build
npm publish
```

The package exposes both ESM (`dist/esm`) and CJS (`dist/cjs`) entry points and registers the `nomad` CLI via `bin/nomad`.

## Troubleshooting
### Migration cache misses

Nomad caches parsed migrations using the file's mtime and size. On some networked filesystems or editors with unusual timestamp behavior, quick edits may not update those attributes. If you suspect stale parses, set `NOMAD_CACHE_HASH_GUARD=true` to add a checksum comparison at the cost of re-reading files once per command.
