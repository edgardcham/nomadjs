import { readFileSync } from "node:fs";

export interface StatementMeta {
  sql: string;
  line: number;
  column: number;
}

/**
 * Normalize line endings and remove BOM
 */
function normalizeContent(content: string): string {
  // Remove BOM if present
  if (content.charCodeAt(0) === 0xFEFF) {
    content = content.slice(1);
  }
  // Normalize CRLF to LF
  return content.replace(/\r\n/g, "\n");
}

function buildLineOffsets(lines: string[]): number[] {
  const offsets: number[] = [];
  let running = 0;
  for (let i = 0; i < lines.length; i++) {
    offsets.push(running);
    running += lines[i]?.length ?? 0;
    if (i < lines.length - 1) {
      running += 1; // account for newline removed by split
    }
  }
  return offsets;
}

function findLineIndex(lineOffsets: number[], index: number): number {
  let low = 0;
  let high = lineOffsets.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const midOffset = lineOffsets[mid] ?? 0;
    const nextOffset = lineOffsets[mid + 1] ?? Number.POSITIVE_INFINITY;
    if (index < midOffset) {
      high = mid - 1;
    } else if (index >= nextOffset) {
      low = mid + 1;
    } else {
      return mid;
    }
  }
  return Math.max(0, Math.min(lineOffsets.length - 1, low));
}

function toLineColumn(lineOffsets: number[], index: number): { line: number; column: number } {
  const lineIdx = findLineIndex(lineOffsets, index);
  const lineStart = lineOffsets[lineIdx] ?? 0;
  return {
    line: lineIdx + 1,
    column: index - lineStart + 1
  };
}

/**
 * Split SQL into statements, respecting:
 * - Single and double quotes
 * - Dollar quotes (PostgreSQL)
 * - Line and block comments
 * - Blocks (between block/endblock directives)
 */
export function splitSqlStatements(sql: string): string[] {
  sql = normalizeContent(sql);
  const statements: string[] = [];
  let current = "";
  let i = 0;

  while (i < sql.length) {
    // Check for line comment
    if (sql[i] === "-" && sql[i + 1] === "-") {
      const lineEnd = sql.indexOf("\n", i);
      if (lineEnd === -1) {
        // Comment goes to end of file
        current += sql.slice(i);
        i = sql.length;
      } else {
        // Skip the comment, keep the newline
        i = lineEnd + 1;
      }
      continue;
    }

    // Check for block comment
    if (sql[i] === "/" && sql[i + 1] === "*") {
      let depth = 1;
      let j = i + 2;
      while (j < sql.length && depth > 0) {
        if (sql[j] === "/" && sql[j + 1] === "*") {
          depth++;
          j += 2;
        } else if (sql[j] === "*" && sql[j + 1] === "/") {
          depth--;
          j += 2;
        } else {
          j++;
        }
      }
      // Only include comment if we're in the middle of a statement
      if (current.trim()) {
        current += sql.slice(i, j);
      }
      i = j;
      continue;
    }

    // Check for E-strings (PostgreSQL escape strings)
    if ((sql[i] === 'E' || sql[i] === 'e') && sql[i + 1] === "'") {
      let j = i + 2; // Start after E'
      while (j < sql.length) {
        if (sql[j] === "\\") {
          // Skip escaped character
          j += 2;
        } else if (sql[j] === "'") {
          j++;
          break;
        } else {
          j++;
        }
      }
      current += sql.slice(i, j);
      i = j;
      continue;
    }

    // Check for other PostgreSQL string prefixes (U&, B, X)
    if ((sql[i] === 'U' && sql[i + 1] === '&' && sql[i + 2] === "'") ||
        ((sql[i] === 'B' || sql[i] === 'b' || sql[i] === 'X' || sql[i] === 'x') && sql[i + 1] === "'")) {
      const prefixLen = (sql[i] === 'U') ? 2 : 1;
      let j = i + prefixLen + 1; // Start after prefix and '
      while (j < sql.length) {
        if (sql[j] === "'") {
          if (sql[j + 1] === "'") {
            j += 2; // Skip escaped quote
          } else {
            j++;
            break;
          }
        } else {
          j++;
        }
      }
      current += sql.slice(i, j);
      i = j;
      continue;
    }

    // Check for single quotes (regular strings)
    if (sql[i] === "'") {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === "'") {
          if (sql[j + 1] === "'") {
            j += 2; // Skip escaped quote
          } else {
            j++;
            break;
          }
        } else {
          j++;
        }
      }
      current += sql.slice(i, j);
      i = j;
      continue;
    }

    // Check for double quotes
    if (sql[i] === '"') {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === '"') {
          if (sql[j + 1] === '"') {
            j += 2; // Skip escaped quote
          } else {
            j++;
            break;
          }
        } else {
          j++;
        }
      }
      current += sql.slice(i, j);
      i = j;
      continue;
    }

    // Check for dollar quotes
    if (sql[i] === "$") {
      // Try to parse dollar quote tag
      let j = i + 1;
      let tag = "";

      // Read tag (can be empty for $$)
      while (j < sql.length && sql[j] !== "$") {
        const ch = sql[j];
        if (ch && /[A-Za-z0-9_]/.test(ch)) {
          tag += ch;
          j++;
        } else {
          break;
        }
      }

      // Check if this is a valid dollar quote start
      if (j < sql.length && sql[j] === "$") {
        const fullTag = "$" + tag + "$";
        j++; // Move past the second $

        // Find the closing dollar quote
        const closeTag = fullTag;
        let endPos = sql.indexOf(closeTag, j);

        if (endPos !== -1) {
          endPos += closeTag.length;
          current += sql.slice(i, endPos);
          i = endPos;
          continue;
        }
      }
    }

    // Check for semicolon (statement separator)
    if (sql[i] === ";") {
      // Special case: check if this completes a COPY ... FROM stdin statement
      const trimmed = current.trim();
      if (trimmed.toUpperCase().includes("COPY") &&
          trimmed.toUpperCase().includes("FROM STDIN")) {
        // Look for \. after the semicolon
        const afterSemi = sql.slice(i + 1);
        const copyEndMatch = afterSemi.match(/^[^\\]*\\\./);
        if (copyEndMatch) {
          // Include everything up to and including \.
          current += ";" + copyEndMatch[0];
          i += 1 + copyEndMatch[0].length;
          statements.push(current.trim());
          current = "";
          continue;
        }
      }

      // Normal statement end
      if (trimmed) {
        statements.push(trimmed);
      }
      current = "";
      i++;
      continue;
    }

    // Regular character
    current += sql[i];
    i++;
  }

  // Add remaining content if any
  const trimmed = current.trim();
  if (trimmed) {
    statements.push(trimmed);
  }

  return statements;
}

function computeSectionMetadata(
  content: string,
  statements: string[],
  startIndex: number,
  endIndex: number,
  lineOffsets: number[]
): StatementMeta[] {
  const metas: StatementMeta[] = [];
  const contentLength = content.length;
  let searchStart = Math.max(0, Math.min(startIndex, contentLength));
  const searchLimit = Math.max(0, Math.min(endIndex, contentLength));

  for (const statement of statements) {
    let matchIndex = -1;
    if (statement.length > 0) {
      matchIndex = content.indexOf(statement, searchStart);
      if (matchIndex >= 0 && matchIndex >= searchLimit) {
        matchIndex = -1;
      }
    }

    if (matchIndex >= 0) {
      const { line, column } = toLineColumn(lineOffsets, matchIndex);
      metas.push({ sql: statement, line, column });
      searchStart = matchIndex + statement.length;
    } else {
      const fallbackIndex = Math.min(contentLength > 0 ? contentLength - 1 : 0, searchStart);
      const { line, column } = contentLength === 0 ? { line: 1, column: 1 } : toLineColumn(lineOffsets, fallbackIndex);
      metas.push({ sql: statement, line, column });
      searchStart = Math.min(contentLength, searchStart + statement.length);
    }
  }

  return metas;
}

export interface ParsedMigration {
  up: {
    statements: string[];
    statementMeta: StatementMeta[];
    notx: boolean;
  };
  down: {
    statements: string[];
    statementMeta: StatementMeta[];
    notx: boolean;
  };
  noTransaction: boolean; // Legacy support
  tags?: string[];
}

/**
 * Parse a Nomad SQL file with directives
 */
// Alias for compatibility
export function parseSQL(content: string, filename?: string): ParsedMigration {
  return parseNomadSql(content, filename || "migration.sql");
}

export function parseNomadSql(content: string, filename: string): ParsedMigration {
  content = normalizeContent(content);

  const result: ParsedMigration = {
    up: { statements: [], statementMeta: [], notx: false },
    down: { statements: [], statementMeta: [], notx: false },
    noTransaction: false,
    tags: undefined
  };

  // Extract directives
  const lines = content.split("\n");
  const lineOffsets = buildLineOffsets(lines);
  let inUp = false;
  let inDown = false;
  let blockDepth = 0;
  let blockContent = "";
  let currentSection: string[] = [];
  let upSearchStart = 0;
  let downSearchStart = -1;
  let downDirectiveOffset = content.length;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const trimmed = line.trim().toLowerCase();

    // Check for directives
    if (trimmed.match(/^--\s*\+\s*nomad/i)) {
      const directive = trimmed.replace(/^--\s*\+\s*nomad\s*/i, "").trim();

      if (directive === "up") {
        inUp = true;
        inDown = false;
        currentSection = [];
        const nextStart = (lineOffsets[i] ?? 0) + lines[i]!.length + 1;
        upSearchStart = Math.min(content.length, Math.max(0, nextStart));
      } else if (directive === "down") {
        if (inUp && currentSection.length > 0) {
          // Save up section
          const upSql = currentSection.join("\n");
          const upStatements = splitSqlStatements(upSql);
          result.up.statements.push(...upStatements);
        }
        inUp = false;
        inDown = true;
        currentSection = [];
        downDirectiveOffset = Math.min(downDirectiveOffset, lineOffsets[i] ?? content.length);
        const nextStart = (lineOffsets[i] ?? 0) + lines[i]!.length + 1;
        downSearchStart = Math.min(content.length, Math.max(0, nextStart));
      } else if (directive === "notx" || directive === "no transaction") {
        // Set notx for the current section
        if (inUp) {
          result.up.notx = true;
        } else if (inDown) {
          result.down.notx = true;
        }
        result.noTransaction = true; // Legacy support
      } else if (directive === "block") {
        // Process any pending non-block statements first
        if (blockDepth === 0 && currentSection.length > 0) {
          const sql = currentSection.join("\n").trim();
          if (sql) {
            const stmts = splitSqlStatements(sql);
            if (inUp) {
              result.up.statements.push(...stmts);
            } else if (inDown) {
              result.down.statements.push(...stmts);
            }
          }
          currentSection = [];
        }

        if (blockDepth === 0) {
          blockContent = "";
        } else {
          // Nested block - include as content
          if (blockContent) blockContent += "\n";
          blockContent += line;
        }
        blockDepth++;
      } else if (directive === "endblock") {
        blockDepth--;
        if (blockDepth === 0) {
          // End of outermost block
          if (blockContent.trim()) {
            if (inUp) {
              result.up.statements.push(blockContent.trim());
            } else if (inDown) {
              result.down.statements.push(blockContent.trim());
            }
          }
          blockContent = "";
        } else if (blockDepth > 0) {
          // Still inside a block - include endblock as content
          if (blockContent) blockContent += "\n";
          blockContent += line;
        }
      } else if (directive.startsWith("tags:")) {
        const tagStr = directive.substring(5).trim();
        result.tags = tagStr
          .split(/[,\s]+/)
          .map(t => t.trim())
          .filter(t => t.length > 0);
      }
    } else {
      // Regular SQL line
      if (blockDepth > 0) {
        if (blockContent) blockContent += "\n";
        blockContent += line;
      } else if ((inUp || inDown) && line) {
        currentSection.push(line);
      }
    }
  }

  // Handle remaining content
  if (inUp && currentSection.length > 0) {
    const upSql = currentSection.join("\n");
    const upStatements = splitSqlStatements(upSql);
    result.up.statements.push(...upStatements);
  } else if (inDown && currentSection.length > 0) {
    const downSql = currentSection.join("\n");
    const downStatements = splitSqlStatements(downSql);
    result.down.statements.push(...downStatements);
  }

  // Handle block content at end
  if (blockDepth > 0 && blockContent.trim()) {
    if (inUp) {
      result.up.statements.push(blockContent.trim());
    } else if (inDown) {
      result.down.statements.push(blockContent.trim());
    }
  }

  const upEnd = Math.max(0, Math.min(downDirectiveOffset, content.length));
  result.up.statementMeta = computeSectionMetadata(
    content,
    result.up.statements,
    upSearchStart,
    upEnd,
    lineOffsets
  );

  const downStart = downSearchStart >= 0 ? downSearchStart : downDirectiveOffset;
  result.down.statementMeta = computeSectionMetadata(
    content,
    result.down.statements,
    Math.max(0, downStart),
    content.length,
    lineOffsets
  );

  return result;
}

/**
 * Read and parse a Nomad SQL file
 */
export function parseNomadSqlFile(filepath: string): ParsedMigration {
  const content = readFileSync(filepath, "utf8");
  return parseNomadSql(content, filepath);
}
