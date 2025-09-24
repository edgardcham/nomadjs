import { readFileSync } from "node:fs";

interface MigrationSection {
  statements: string[];
}

interface ParsedMigration {
  version: number;
  path: string;
  up: MigrationSection;
  down: MigrationSection;
  noTransaction: boolean;
}

const UP = /^--\s*\+nomad\s+Up\s*$/i;
const DOWN = /^--\s*\+nomad\s+Down\s*$/i;
const STMT_BEGIN = /^--\s*\+nomad\s+StatementBegin\s*$/i;
const STMT_END = /^--\s*\+nomad\s+StatementEnd\s*$/i;
const NO_TX = /^--\s*\+nomad\s+NO\s+TRANSACTION\s*$/i;

export function parseNomadSql(path: string, version: number): ParsedMigration {
  const raw = readFileSync(path, "utf8");
  const lines = raw.split(/\r?\n/);

  let mode: "up" | "down" | null = null;
  let currentStmt: string[] = [];
  let inBlock = false;

  const up: MigrationSection = { statements: [] };
  const down: MigrationSection = { statements: [] };
  let noTransaction = false;

  const flush = () => {
    const stmt = currentStmt.join("\n").trim();
    if (!stmt) return;
    (mode === "up" ? up.statements : down.statements).push(stmt);
    currentStmt = [];
  };

  for (const line of lines) {
    if (UP.test(line)) {
      if (mode && currentStmt.length) flush();
      mode = "up";
      continue;
    }
    if (DOWN.test(line)) {
      if (mode && currentStmt.length) flush();
      mode = "down";
      continue;
    }
    if (STMT_BEGIN.test(line)) {
      inBlock = true;
      continue;
    }
    if (STMT_END.test(line)) {
      inBlock = false;
      flush();
      continue;
    }
    if (NO_TX.test(line)) {
      noTransaction = true;
      continue;
    }
    if (!mode) continue;
    if (inBlock) {
      currentStmt.push(line);
    } else {
      currentStmt.push(line);
      if (line.trim().endsWith(";")) flush();
    }
  }
  if (currentStmt.length) flush();

  return { version, path, up, down, noTransaction };
}
