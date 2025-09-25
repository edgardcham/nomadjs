import { describe, it, expect } from "vitest";
import { execSync } from "child_process";
import { mkdirSync, rmSync } from "fs";
import { join } from "path";
import { isDatabaseAvailable } from "../utils/db";

const nomadCmd = "node dist/esm/cli.js";
const testDbUrl = process.env.DATABASE_URL || "postgresql://postgres@localhost/nomaddb";
const shouldRun = process.env.NOMAD_TEST_WITH_DB === "true" &&
  isDatabaseAvailable(testDbUrl, nomadCmd);

if (!shouldRun) {
  console.warn("Skipping CLI doctor tests: database unavailable or NOMAD_TEST_WITH_DB not set");
}

const describeIfDb = shouldRun ? describe : describe.skip;

describeIfDb("CLI: nomad doctor command", () => {
  const testDir = join(process.cwd(), "test-migrations-doctor");
  const tableName = `nomad_test_doctor_${Date.now()}`;

  it("generates a JSON report", () => {
    mkdirSync(testDir, { recursive: true });
    try {
      const output = execSync(
        `${nomadCmd} doctor --json --url "${testDbUrl}" --dir ${testDir} --table ${tableName}`,
        {
          encoding: "utf8",
          env: { ...process.env, NODE_ENV: "test", NOMAD_TEST_WITH_DB: "true" }
        }
      );
      const parsed = JSON.parse(output.trim());
      expect(parsed).toHaveProperty("checks");
      expect(Array.isArray(parsed.checks)).toBe(true);
    } finally {
      rmSync(testDir, { recursive: true, force: true });
    }
  });
});
