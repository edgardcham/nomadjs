# NomadJS

Production-ready SQL migration tool for Node.js with checksums, transaction control, and advanced PostgreSQL support.

**v1.1 Production Ready (Grade: A+)** - All P0 features complete with 300+ tests passing (100% success rate).

**Key Features:**
- 🔒 SHA-256 checksums for drift detection
- 🎯 Automatic transaction wrapping with hazard detection
- 🚨 Detects operations that can't run in transactions (CREATE INDEX CONCURRENTLY, etc.)
- 🐘 Full PostgreSQL support (dollar quotes, E-strings, COPY)
- 📝 TOML/JSON configuration with env var substitution
- ✅ 300+ tests passing with comprehensive edge case coverage
- 🎯 Performance optimized with migration file caching
- 🗜️ Configurable database schema support
- 🚀 Standardized exit codes for CI/CD integration
- 🔐 Advisory locking prevents concurrent migrations
- 📊 JSON output for automation and monitoring
- 🌈 Color-aware CLI output respecting `NO_COLOR`/`NOMAD_NO_COLOR`

## Installation & Linking

```bash
npm install
npm run build
npm link            # exposes the `nomad` CLI globally during development
```

To unlink later, run `npm unlink --global nomadjs` and `npm unlink nomadjs` in any project where it was linked.

## Database Prerequisites
- Create the target database yourself (Nomad will create only the version-tracking table).
- For example, using the default `postgres` superuser:
  ```bash
  psql -U postgres -d postgres -c "CREATE DATABASE nomaddb;"
  export DATABASE_URL="postgres://postgres@localhost/nomaddb"
  ```
- Alternatively, pass `--url` to each command instead of exporting `DATABASE_URL`.

## Creating & Running Migrations

```bash
nomad create init_schema
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
| `nomad create <name>` | Create timestamped migration file |
| `nomad status` | Show migration status with drift detection |
| `nomad plan [direction]` | Preview migration plan without executing |
| `nomad up [limit]` | Apply pending migrations |
| `nomad down [count]` | Rollback migrations (default: 1) |
| `nomad to <version>` | Migrate to specific version |
| `nomad redo` | Rollback and reapply the last migration (safe for development; last migration only) |
| `nomad verify` | Verify migration checksums |
| `nomad doctor` | Diagnose configuration and database readiness |

All commands accept `--url`, `--dir`, `--table`, `--allow-drift`, and `--auto-notx` flags.

📚 **Full Documentation:** See [CLI_DOCUMENTATION.md](./CLI_DOCUMENTATION.md) for detailed usage, directives, and examples.


## CLI Output & Colors

Nomad prints color-coded status lines (success, warnings, notes) when running in a TTY. Colors automatically turn off when piping output or when you set either `NO_COLOR` or `NOMAD_NO_COLOR=true`. Set `NOMAD_NO_COLOR=false` (or `0`) to force colors back on for automated environments that still support ANSI escape codes.


## Configuration

Nomad reads settings in this order (highest priority first):

1. CLI flags (`--url`, `--dir`, `--table`, `--config`).
2. Environment variables (`DATABASE_URL`, `NOMAD_MIGRATIONS_DIR`, `NOMAD_DB_TABLE`, `NOMAD_DATABASE_URL`).
3. Config file (`nomad.toml` or `nomad.json`).
4. Defaults (`dir` defaults to `migrations`).

Place a `nomad.toml` or `nomad.json` next to your project to avoid repeating options. Example TOML:

```toml
[database]
url = "postgres://postgres@localhost/nomaddb"
table = "nomad_db_version"

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

## Publishing Notes

When you are ready to publish to npm:

```bash
npm run build
npm publish
```

The package exposes both ESM (`dist/esm`) and CJS (`dist/cjs`) entry points and registers the `nomad` CLI via `bin/nomad`.
