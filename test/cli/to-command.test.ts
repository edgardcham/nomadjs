import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { execSync } from "child_process";
import { mkdirSync, rmSync, existsSync, writeFileSync } from "fs";
import { join } from "path";

const nomadCmd = "node dist/esm/cli.js";
const testDir = join(process.cwd(), "test-migrations-to");
const testDbUrl = process.env.DATABASE_URL || "postgresql://postgres@localhost/nomaddb";

function run(cmd: string) {
  try {
    const stdout = execSync(cmd, { encoding: "utf8", stdio: "pipe", env: { ...process.env, NODE_ENV: "test" } });
    return { exitCode: 0, stdout };
  } catch (err: any) {
    return { exitCode: err.status ?? 1, stdout: err.stdout?.toString() ?? "", stderr: err.stderr?.toString() ?? "" };
  }
}

describe("CLI: nomad to command", () => {
  beforeEach(() => {
    mkdirSync(testDir, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  it("applies migrations up to target version", () => {
    const health = run(`${nomadCmd} status --url "${testDbUrl}" --dir ${testDir}`);
    if (health.exitCode === 7) {
      console.warn('Skipping CLI to test: database unavailable');
      return;
    }
    const migration1 = join(testDir, "20240101120000_create_users.sql");
    const migration2 = join(testDir, "20240102120000_add_user.sql");
    writeFileSync(migration1, `-- +nomad Up
CREATE TABLE users (id INT);
-- +nomad Down
DROP TABLE users;
`);
    writeFileSync(migration2, `-- +nomad Up
INSERT INTO users VALUES (1);
-- +nomad Down
DELETE FROM users WHERE id = 1;
`);

    const result = run(`${nomadCmd} to 20240102120000 --url "${testDbUrl}" --dir ${testDir}`);
    expect(result.exitCode).toBe(0);
  });
});
