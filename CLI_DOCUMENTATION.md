# NomadJS CLI Documentation

## Overview

NomadJS is a production-ready SQL migration tool for Node.js with advanced features including checksums, transaction control, and comprehensive PostgreSQL support.

## Features

### Enhanced SQL Parser (65 tests)
   - Dollar quote support (`$$`, `$tag$...$tag$`) with nesting
   - Block statements (`-- +nomad block/endblock`) for COPY and complex procedures
   - PostgreSQL E-strings (`E'...'`) with proper backslash escapes
   - PostgreSQL special strings (U&, B, X prefixes)
   - Comment preservation (line `--`, block `/* */`)
   - CRLF/BOM normalization
   - Tags support (`-- +nomad tags: tag1,tag2`)

### Checksum System (43 tests)
   - SHA-256 checksums for all migration files
   - Automatic drift detection when files change after being applied
   - `--allow-drift` flag for emergency overrides
   - Exit code 2 for drift, exit code 5 for missing files
   - Handles all line ending styles (CRLF/CR/LF)
   - Unicode, emoji, and binary data support

### Configuration System
   - Multiple config sources (CLI > env > config file > defaults)
   - Supports TOML and JSON config files
   - Environment variable substitution (`${VAR_NAME}` syntax)
   - Auto-loads `.env` files

### Colorized Output
   - Success/info/warning messages are color-coded when stdout is a TTY
   - Colors automatically disable when piping output
   - Set `NO_COLOR` or `NOMAD_NO_COLOR=true` to force monochrome output
   - Override in CI with `NOMAD_NO_COLOR=false` if ANSI colors are desired

## CLI Commands

### `nomad init-config [format]`
Create a configuration file template.

```bash
# Create nomad.toml (default)
nomad init-config

# Create nomad.json
nomad init-config json

# Create custom filename
nomad init-config --output my-config.toml
nomad init-config json --output database.json
```

**Config Template (TOML)**:
```toml
[database]
# Database connection URL
# Supports environment variable substitution
url = "postgresql://user:password@localhost:5432/dbname"
# Or use env vars:
# url = "${DATABASE_URL}"
# url = "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Custom migration tracking table name (default: nomad_migrations)
table = "nomad_migrations"

[migrations]
# Directory containing migration files (default: migrations)
dir = "migrations"

[options]
# Allow migrations with checksum mismatches (NOT RECOMMENDED for production)
# allowDrift = false

# Auto-disable transactions for hazardous operations (NOT RECOMMENDED for production)
# autoNotx = false
```

### `nomad create <name>`
Create a new migration file with timestamp prefix.

```bash
nomad create add_users_table
# Creates: migrations/20250921123045_add_users_table.sql

nomad create seed_users --block
# Creates: migrations/<timestamp>_seed_users.sql with COPY block template
```

**Migration Template**:
```sql
-- +nomad Up
-- SQL for forward migration goes here

-- +nomad Down
-- SQL for rollback goes here
```

**Block Template (`--block`)**:
```sql
-- +nomad Up
-- +nomad block
-- Place multi-line statements here (e.g., COPY FROM stdin)
-- Example:
-- COPY my_table (col1, col2) FROM stdin;
-- 1	Alice
-- 2	Bob
-- \.
-- +nomad endblock

-- +nomad Down
-- write your down migration here
```

### `nomad status`
Show the status of all migrations.

```bash
nomad status
nomad status --tags=seed
nomad status --only-tagged

# Output:
# applied  20250921123045  add_users       2025-09-21T12:31:00.000Z
# pending  20250921123146  add_posts
# applied  20250921123247  add_comments [DRIFT]  2025-09-21T12:32:00.000Z
# applied  20250921123348  add_tags [MISSING]     2025-09-21T12:33:00.000Z

# JSON output for CI/automation
nomad status --json
```

**JSON Output Format**:
```json
[
  {
    "version": "20250921123045",
    "name": "add_users",
    "applied": true,
    "appliedAt": "2025-09-21T12:31:00.000Z"
  },
  {
    "version": "20250921123146",
    "name": "add_posts",
    "applied": false
  },
  {
    "version": "20250921123247",
    "name": "add_comments",
    "applied": true,
    "appliedAt": "2025-09-21T12:32:00.000Z",
    "hasDrift": true
  },
  {
    "version": "20250921123348",
    "name": "add_tags",
    "applied": true,
    "appliedAt": "2025-09-21T12:33:00.000Z",
    "isMissing": true
  }
]
```

**Options**:
- `--json` - Output status as JSON for CI/automation
- `--tags=tag1,tag2` - Include only migrations with any of these tags (OR)
- `--only-tagged` - Include only migrations that have tags

**Exit Codes**:
- `0` - Success
- `2` - Drift detected (file changed after being applied)
- `5` - Missing migration file

### `nomad plan [direction]`
Preview migration plan without executing.

```bash
# Preview pending migrations (default: up)
nomad plan

# Preview rollback plan for last 2 migrations
nomad plan down --count 2

# Plan to specific version
nomad plan --to 20250921123045

# Output as JSON for CI/automation
nomad plan --json

# Test run (execute but rollback)
nomad plan --dry-run
```

**Example Output**:
```
Migration Plan: UP
══════════════════════════════════════════════════════

[TX] ↑ 20240101120000_create_users.sql
     └─ CREATE TABLE users (id INT PRIMARY KEY);

[NO-TX] ↑ 20240103140000_create_index.sql ⚠️ HAZARD
     └─ Reason: hazardous operations
     └─ CREATE INDEX CONCURRENTLY idx_users_email ON users(email);
     └─ ⚠️ CREATE_INDEX_CONCURRENTLY

Summary: 2 migrations to apply (1 transactional, 1 non-transactional)
Warnings: 1 hazardous operation detected
```

**Options**:
- `--limit <n>` - Limit number of up migrations to plan
- `--count <n>` - Number of down migrations to plan
- `--to <version>` - Target version to plan to
- `--json` - Output as JSON for CI/automation
- `--dry-run` - Execute migrations but rollback (test run)
- `--tags=tag1,tag2` - Include migrations with any of the tags (OR)
- `--only-tagged` - Include only migrations that have tags

### `nomad up [limit]`
Apply pending migrations.

```bash
# Apply all pending migrations
nomad up

# Apply only next 2 migrations
nomad up 2

# Apply only tagged migrations
nomad up --tags=seed

# Include earlier pending prerequisites up to first matching tag
nomad up --tags=users --include-ancestors

# Verbose execution (per-statement timing)
nomad up --verbose

# Stream JSON events (NDJSON)
nomad up --events-json
```

When `--events-json` is enabled Nomad emits newline-delimited events:
- `lock-acquired` / `lock-released` mark advisory lock lifecycle across `up`, `down`, `to`, and `redo`.
- `apply-start` / `apply-end` surround each migration with `direction` (`up` or `down`) and include the run time (`ms`) on completion.
- `stmt-run` reports per-statement execution timing with a truncated SQL preview.
- `verify-start` / `verify-end` wrap `nomad verify`, with the end event summarising drift and missing counts.

### `nomad down [count]`
Rollback applied migrations.

```bash
# Rollback last migration (default: 1)
nomad down

# Rollback last 3 migrations
nomad down 3

# Rollback only tagged migrations at the head of the stack
nomad down --tags=seed --count 2

# Stream JSON events (NDJSON)
nomad down 1 --events-json
```

### `nomad redo`
Rollback and reapply the last migration (useful for testing during development).

```bash
nomad redo

# Redo with drift allowed (if file changed)
nomad redo --allow-drift
```

Note: redo always operates on the last applied migration (no specific version selection).

**Use Cases:**
- Testing migration changes during development
- Verifying idempotency of migrations
- Quickly reapplying a migration after modifications

### `nomad verify`
Verify checksums of all applied migrations.

```bash
nomad verify

# Emit NDJSON summary while verifying
nomad verify --events-json

# Output:
# ✓ All migration checksums valid
# or
# ✗ 2 migrations have drift:
#   - 20250921123045: checksum mismatch
#   - 20250921123146: file missing
```

### `nomad doctor`
Run readiness diagnostics against your configuration and the target PostgreSQL instance.

```bash
nomad doctor
nomad doctor --json            # machine-readable report
nomad doctor --fix             # create schema/table if missing
```

Checks performed:
- Database connectivity (current user, database, timezone, encoding)
- Schema availability (optional creation with `--fix`)
- Presence of the Nomad migrations table (`--fix` can bootstrap it safely)
- Advisory lock acquisition (verifies concurrency safeguards)

Warnings keep the exit code at 0 so you can surface issues without breaking pipelines. Connection failures exit with code 7. Use `--json` in CI to capture the full report.

<!-- Removed outdated 'Coming Soon' section; 'plan' is implemented above. -->

## Migration Directives

### Transaction Control & Hazard Detection

#### Default Transaction Wrapping
By default, each migration runs in a transaction for safety. This ensures all-or-nothing execution.

#### Hazardous Operations
NomadJS automatically detects operations that cannot run in transactions:
- `CREATE INDEX CONCURRENTLY` / `DROP INDEX CONCURRENTLY`
- `VACUUM`, `REINDEX`, `CLUSTER`
- `REFRESH MATERIALIZED VIEW CONCURRENTLY`
- `ALTER TYPE` (adding enum values)
- `ALTER SYSTEM`, `CREATE/DROP DATABASE`, `CREATE/DROP TABLESPACE`

If hazards are detected without the `notx` directive, the migration will fail with a clear error.

#### Disabling Transactions
Use `-- +nomad notx` to disable transactions for operations that require it:

```sql
-- +nomad Up
-- +nomad notx
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);

-- +nomad Down
-- +nomad notx
DROP INDEX CONCURRENTLY idx_users_email;
```

#### Auto-notx Mode
For development environments, use `--auto-notx` to automatically disable transactions when hazards are detected:

```bash
nomad up --auto-notx
# Or via environment variable
export NOMAD_AUTO_NOTX=true
```

**Warning**: Auto-notx should NOT be used in production as it bypasses safety checks.

### Block Statements

For statements that can't be split by semicolons (like COPY):

```sql
-- +nomad Up

-- +nomad block
COPY users (id, name, email) FROM stdin;
1	John Doe	john@example.com
2	Jane Smith	jane@example.com
\.
-- +nomad endblock

-- Regular statements continue here
UPDATE users SET verified = true;
```

### Tags

Tag migrations for filtering:

```sql
-- +nomad tags: seed, test, development
-- +nomad Up
INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');

-- +nomad Down
DELETE FROM users WHERE email = 'test@example.com';
```

## Configuration

### Priority Order
1. CLI flags (`--url`, `--dir`, `--table`)
2. Environment variables (`NOMAD_*`, `DATABASE_URL`)
3. Config file (`nomad.toml` or `nomad.json`)
4. Defaults

### Environment Variables

```bash
# Database connection
export DATABASE_URL="postgresql://user:pass@localhost/db"
# or
export NOMAD_DATABASE_URL="postgresql://user:pass@localhost/db"

# Migration directory
export NOMAD_MIGRATIONS_DIR="db/migrations"

# Version table name
export NOMAD_DB_TABLE="schema_migrations"

# Allow drift (emergencies only)
export NOMAD_ALLOW_DRIFT=true

# Auto-disable transactions for hazardous operations (dev only)
export NOMAD_AUTO_NOTX=true
```

### Environment Variable Substitution

Config files support `${VAR}` and `$VAR` syntax:

```toml
[database]
# Full URL from env
url = "${DATABASE_URL}"

# Or compose from parts
url = "postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Mix hardcoded and env values
url = "postgresql://app:${DB_PASSWORD}@prod.example.com:5432/myapp"
```

## Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | Operation completed successfully |
| 1 | SQL Error | Migration SQL failed to execute |
| 2 | Drift Detected | Migration file changed after being applied |
| 3 | Lock Timeout | Could not acquire migration lock |
| 4 | Parse/Config Error | Invalid configuration or migration file |
| 5 | Missing File | Applied migration file no longer exists |
| 6 | Checksum Mismatch | File checksum doesn't match expected |
| 7 | Connection Error | Could not connect to database |

## Database Schema

NomadJS uses this table structure:

```sql
CREATE TABLE nomad_migrations (
  version     BIGINT PRIMARY KEY,
  name        TEXT NOT NULL,
  checksum    TEXT NOT NULL,
  applied_at  TIMESTAMPTZ,
  rolled_back_at TIMESTAMPTZ
);
```

## Examples

### Basic Workflow

```bash
# 1. Initialize config
nomad init-config

# 2. Create your first migration
nomad create initial_schema

# 3. Edit the migration file
# Edit migrations/20250921123045_initial_schema.sql

# 4. Apply migrations
nomad up

# 5. Check status
nomad status

# 6. Rollback if needed
nomad down
```

### CI/CD Integration

```bash
#!/bin/bash
set -e

# Verify checksums haven't drifted
nomad verify

# Run migrations
nomad up

# Confirm all migrations applied
nomad status --json | jq '.pending | length' | grep -q '^0$'
```

### Docker Compose Setup

```yaml
services:
  migrate:
    image: node:18
    environment:
      DATABASE_URL: postgresql://user:pass@db:5432/myapp
    volumes:
      - ./migrations:/app/migrations
    command: |
      sh -c "
        npm install -g nomadjs
        nomad up
      "
```

## Safety Features

1. **Checksum Verification**: Every operation verifies file checksums to detect changes
2. **Advisory Locking**: Prevents concurrent migration execution (PostgreSQL)
3. **Transaction Wrapping**: Migrations run in transactions by default
4. **Hazard Detection**: Warns about operations that can't run in transactions
5. **Drift Detection**: Alerts when migration files change after being applied

## Troubleshooting

Nomad prepends CLI errors with `file:line:column` so you can locate failing statements instantly. Example:

```
migrations/20240101120000_create_users.sql:42:5 - Failed UP 20240101120000 (create_users): syntax error at or near "FROM"
```

### Checksum Mismatch Error

```bash
# Error: Checksum mismatch for migration 20250921123045
# The file has been modified after being applied

# Solutions:
# 1. Restore the original file from version control
git checkout migrations/20250921123045_add_users.sql

# 2. Emergency override (NOT RECOMMENDED)
nomad up --allow-drift
```

### Missing Migration File

```bash
# Error: Migration file not found: 20250921123045_add_users.sql

# The file was applied but is now missing. Either:
# 1. Restore the file from backups
# 2. Manually create a placeholder with the same checksum
```

### Lock Timeout

```bash
# Error: Could not acquire migration lock (timeout: 30s)

# Another migration is running. Either:
# 1. Wait for it to complete
# 2. Check for stuck migrations:
psql -c "SELECT * FROM pg_locks WHERE locktype = 'advisory';"
```

## Exit Codes

NomadJS uses standardized exit codes for different error conditions:

| Code | Description | Scenario |
|------|-------------|----------|
| 0 | Success | Command completed successfully |
| 1 | SQL Error | SQL execution failed during migration |
| 2 | Drift Detected | Migration file modified after being applied |
| 3 | Lock Timeout | Failed to acquire advisory lock within timeout |
| 4 | Parse/Config Error | Invalid configuration or SQL parse error |
| 5 | Missing File | Applied migration file not found on disk |
| 6 | Checksum Mismatch | Migration file checksum doesn't match database |
| 7 | Connection Error | Unable to connect to database |

### Using Exit Codes in CI/CD

```bash
#!/bin/bash
nomad up || {
  case $? in
    2) echo "ERROR: Drift detected! Migration files were modified." ;;
    3) echo "ERROR: Another migration is running. Try again later." ;;
    5) echo "ERROR: Missing migration files. Check your repository." ;;
    7) echo "ERROR: Cannot connect to database." ;;
    *) echo "ERROR: Migration failed with code $?" ;;
  esac
  exit 1
}
```

## Best Practices

1. **Never modify applied migrations** - Create new migrations for changes
2. **Test rollbacks** - Ensure Down migrations work before production
3. **Use transactions** - Unless you have operations that can't run in transactions
4. **Version control migrations** - Track all migration files in git
5. **Review before applying** - Use `nomad plan` to preview changes
6. **Monitor drift** - Run `nomad verify` in CI to catch changes early
7. **Use tags** - Tag test/seed data migrations for easy filtering
8. **Handle exit codes** - Check exit codes in automation scripts
