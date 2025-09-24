import { execSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export function isDatabaseAvailable(url: string, nomadCmd: string): boolean {
  const tempDirRoot = join(tmpdir(), "nomad-cli-db-check-");
  const tempDir = mkdtempSync(tempDirRoot);
  const tableName = `nomad_probe_${Date.now()}`;
  try {
    execSync(
      `${nomadCmd} status --url "${url}" --dir "${tempDir}" --table ${tableName}`,
      {
        stdio: "ignore",
        env: { ...process.env, NODE_ENV: "test", DATABASE_URL: url }
      }
    );
    return true;
  } catch (error) {
    return false;
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}
