import { readFileSync } from "node:fs";

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

export interface ParsedMigration {
  up: {
    statements: string[];
    notx: boolean;
  };
  down: {
    statements: string[];
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
    up: { statements: [], notx: false },
    down: { statements: [], notx: false },
    noTransaction: false,
    tags: undefined
  };

  // Extract directives
  const lines = content.split("\n");
  let inUp = false;
  let inDown = false;
  let blockDepth = 0;
  let blockContent = "";
  let currentSection: string[] = [];

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

  return result;
}

/**
 * Read and parse a Nomad SQL file
 */
export function parseNomadSqlFile(filepath: string): ParsedMigration {
  const content = readFileSync(filepath, "utf8");
  return parseNomadSql(content, filepath);
}