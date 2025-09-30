export interface Hazard {
  type: string;
  line: number;
  column: number;
  statement: string;
}

export interface ValidationResult {
  shouldSkipTransaction: boolean;
  hazardsDetected: Hazard[];
}

interface HazardPattern {
  type: string;
  pattern: RegExp;
}

import { logger as nomadLogger } from "../utils/logger.js";

const HAZARD_PATTERNS: HazardPattern[] = [
  {
    type: "CREATE_INDEX_CONCURRENTLY",
    pattern: /\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\b/i
  },
  {
    type: "DROP_INDEX_CONCURRENTLY",
    pattern: /\bDROP\s+INDEX\s+CONCURRENTLY\b/i
  },
  {
    type: "REINDEX",
    pattern: /\bREINDEX\b/i
  },
  {
    type: "VACUUM",
    pattern: /\bVACUUM\b/i
  },
  {
    type: "CLUSTER",
    pattern: /\bCLUSTER\b/i
  },
  {
    type: "REFRESH_MATERIALIZED_VIEW_CONCURRENTLY",
    pattern: /\bREFRESH\s+MATERIALIZED\s+VIEW\s+CONCURRENTLY\b/i
  },
  {
    type: "ALTER_TYPE",
    pattern: /\bALTER\s+TYPE\b/i
  },
  {
    type: "ALTER_SYSTEM",
    pattern: /\bALTER\s+SYSTEM\b/i
  },
  {
    type: "CREATE_DATABASE",
    pattern: /\bCREATE\s+DATABASE\b/i
  },
  {
    type: "DROP_DATABASE",
    pattern: /\bDROP\s+DATABASE\b/i
  },
  {
    type: "CREATE_TABLESPACE",
    pattern: /\bCREATE\s+TABLESPACE\b/i
  },
  {
    type: "DROP_TABLESPACE",
    pattern: /\bDROP\s+TABLESPACE\b/i
  },
  {
    type: "ALTER_TABLESPACE",
    pattern: /\bALTER\s+TABLESPACE\b/i
  },
  {
    type: "LOCK_TABLES",
    pattern: /\bLOCK\s+TABLES\b/i
  },
  {
    type: "LOAD_DATA_INFILE",
    pattern: /\bLOAD\s+DATA\s+(?:LOCAL\s+)?INFILE\b/i
  },
  {
    type: "ALTER_TABLE_ALGORITHM",
    pattern: /\bALTER\s+TABLE\b(?:(?!;).)*(?:\bALGORITHM\s*=\s*\w+|\bLOCK\s*=\s*\w+)/is
  },
  {
    type: "TABLE_MAINTENANCE",
    pattern: /\b(?:OPTIMIZE|ANALYZE|REPAIR)\s+TABLE\b/i
  }
];

interface SQLSegment {
  content: string;
  isCode: boolean;
  startLine: number;
  startColumn: number;
}

function preprocessSQL(sql: string): SQLSegment[] {
  const segments: SQLSegment[] = [];
  let currentPos = 0;
  let lineNumber = 1;
  let columnNumber = 1;

  const addSegment = (content: string, isCode: boolean, startLine: number, startColumn: number) => {
    if (content) {
      segments.push({ content, isCode, startLine, startColumn });
    }
  };

  while (currentPos < sql.length) {
    const startLine = lineNumber;
    const startColumn = columnNumber;

    // Check for line comments
    if (sql[currentPos] === '-' && sql[currentPos + 1] === '-') {
      let endPos = sql.indexOf('\n', currentPos);
      if (endPos === -1) endPos = sql.length;

      const comment = sql.substring(currentPos, endPos);
      addSegment(comment, false, startLine, startColumn);

      currentPos = endPos;
      columnNumber = 1;
      continue;
    }

    // Check for block comments
    if (sql[currentPos] === '/' && sql[currentPos + 1] === '*') {
      let endPos = sql.indexOf('*/', currentPos);
      if (endPos === -1) endPos = sql.length;
      else endPos += 2;

      const comment = sql.substring(currentPos, endPos);
      addSegment(comment, false, startLine, startColumn);

      // Update line/column tracking
      for (let i = currentPos; i < endPos; i++) {
        if (sql[i] === '\n') {
          lineNumber++;
          columnNumber = 1;
        } else {
          columnNumber++;
        }
      }

      currentPos = endPos;
      continue;
    }

    // Check for dollar quotes
    if (sql[currentPos] === '$') {
      const dollarMatch = sql.substring(currentPos).match(/^(\$[^$]*\$)/);
      if (dollarMatch && dollarMatch[1]) {
        const delimiter = dollarMatch[1];
        const startPos = currentPos;
        const endPos = sql.indexOf(delimiter, currentPos + delimiter.length);

        if (endPos !== -1) {
          const fullQuote = sql.substring(startPos, endPos + delimiter.length);
          addSegment(fullQuote, false, startLine, startColumn);

          // Update line/column tracking
          for (let i = startPos; i < endPos + delimiter.length; i++) {
            if (sql[i] === '\n') {
              lineNumber++;
              columnNumber = 1;
            } else {
              columnNumber++;
            }
          }

          currentPos = endPos + delimiter.length;
          continue;
        }
      }
    }

    // Check for strings
    if (sql[currentPos] === "'" ||
        (sql[currentPos] === 'E' && sql[currentPos + 1] === "'") ||
        (sql[currentPos] === 'U' && sql[currentPos + 1] === '&' && sql[currentPos + 2] === "'") ||
        (sql[currentPos] === 'B' && sql[currentPos + 1] === "'") ||
        (sql[currentPos] === 'X' && sql[currentPos + 1] === "'")) {

      let quoteStart = currentPos;
      if (sql[currentPos] !== "'") {
        quoteStart = sql.indexOf("'", currentPos);
      }

      let i = quoteStart + 1;
      while (i < sql.length) {
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") {
            i += 2; // Skip escaped quote
          } else {
            i++;
            break;
          }
        } else if (sql[i] === '\\' && sql[currentPos] === 'E') {
          i += 2; // Skip escape sequence in E-string
        } else {
          i++;
        }
      }

      const str = sql.substring(currentPos, i);
      addSegment(str, false, startLine, startColumn);

      // Update line/column tracking
      for (let j = currentPos; j < i; j++) {
        if (sql[j] === '\n') {
          lineNumber++;
          columnNumber = 1;
        } else {
          columnNumber++;
        }
      }

      currentPos = i;
      continue;
    }

    // Check for COPY blocks - treat them as non-code
    if (sql.substring(currentPos).match(/^COPY\s+/i)) {
      const copyMatch = sql.substring(currentPos).match(/^COPY\s+[^;]+FROM\s+stdin[^;]*;?/i);
      if (copyMatch) {
        const copyStart = currentPos;
        let copyEnd = currentPos + copyMatch[0].length;

        // Find the \. terminator
        const terminatorPos = sql.indexOf('\\.', copyEnd);
        if (terminatorPos !== -1) {
          copyEnd = terminatorPos + 2;
          // Skip optional newline after \.
          if (copyEnd < sql.length && sql[copyEnd] === '\n') copyEnd++;
        }

        const copyBlock = sql.substring(copyStart, copyEnd);
        addSegment(copyBlock, false, startLine, startColumn);

        // Update line/column tracking
        for (let i = copyStart; i < copyEnd; i++) {
          if (sql[i] === '\n') {
            lineNumber++;
            columnNumber = 1;
          } else {
            columnNumber++;
          }
        }

        currentPos = copyEnd;
        continue;
      }
    }

    // Regular code
    let codeEnd = currentPos;
    while (codeEnd < sql.length) {
      const ch = sql[codeEnd];

      // Stop at potential string/comment/quote starts
      if (ch === '-' && sql[codeEnd + 1] === '-') break;
      if (ch === '/' && sql[codeEnd + 1] === '*') break;
      if (ch === '$') break;
      if (ch === "'") break;
      if (ch === 'E' && sql[codeEnd + 1] === "'") break;
      if (ch === 'U' && sql[codeEnd + 1] === '&' && sql[codeEnd + 2] === "'") break;
      if (ch === 'B' && sql[codeEnd + 1] === "'") break;
      if (ch === 'X' && sql[codeEnd + 1] === "'") break;
      if (sql.substring(codeEnd).match(/^\bCOPY\b.*\bFROM\s+stdin/i)) break;

      if (ch === '\n') {
        lineNumber++;
        columnNumber = 1;
      } else {
        columnNumber++;
      }

      codeEnd++;
    }

    if (codeEnd > currentPos) {
      const code = sql.substring(currentPos, codeEnd);
      addSegment(code, true, startLine, startColumn);
      currentPos = codeEnd;
    }
  }

  return segments;
}

export function detectHazards(sql: string): Hazard[] {
  const hazards: Hazard[] = [];
  const segments = preprocessSQL(sql);

  for (const segment of segments) {
    if (!segment.isCode) continue;

    // Collect all matches with their positions first
    const matches: Array<{
      pattern: typeof HAZARD_PATTERNS[0];
      match: RegExpExecArray;
      index: number;
    }> = [];

    for (const pattern of HAZARD_PATTERNS) {
      const regex = new RegExp(pattern.pattern.source, pattern.pattern.flags + 'g');
      let match;

      while ((match = regex.exec(segment.content)) !== null) {
        matches.push({
          pattern,
          match,
          index: match.index
        });
      }
    }

    // Sort matches by their position in the SQL
    matches.sort((a, b) => a.index - b.index);

    // Process matches in order of appearance
    for (const { pattern, match } of matches) {
      // Calculate the actual line and column of the match
      let matchLine = segment.startLine;
      let matchColumn = segment.startColumn;

      // Count newlines and adjust position up to the match
      for (let i = 0; i < match.index; i++) {
        if (segment.content[i] === '\n') {
          matchLine++;
          matchColumn = 1;
        } else {
          matchColumn++;
        }
      }

      // Try to extract the full statement around the match
      let statementEnd = match.index + match[0].length;
      let statementStart = match.index;

      // Find the end of statement (semicolon or newline)
      while (statementEnd < segment.content.length) {
        const ch = segment.content[statementEnd];
        if (ch === ';' || ch === '\n') {
          if (ch === ';') statementEnd++; // Include semicolon
          break;
        }
        statementEnd++;
      }

      // Extract and trim the statement
      let statement = segment.content.substring(statementStart, statementEnd).trim();

      hazards.push({
        type: pattern.type,
        line: matchLine,
        column: matchColumn,
        statement: statement
      });
    }
  }

  return hazards;
}

export function wrapInTransaction(sql: string, skipTransaction: boolean = false): string {
  if (skipTransaction) {
    return sql;
  }

  // Check if transaction already exists
  const transactionPattern = /\b(BEGIN|START\s+TRANSACTION)\b/i;
  if (transactionPattern.test(sql)) {
    return sql;
  }

  // Wrap in transaction
  const trimmed = sql.trim();
  return `BEGIN;\n${trimmed}\nCOMMIT;`;
}

export function validateHazards(
  hazards: Hazard[],
  hasNotx: boolean,
  options?: {
    autoNotx?: boolean;
    logger?: (msg: string) => void;
  }
): ValidationResult {
  const { autoNotx = false, logger: customLogger } = options || {};
  const logFn = customLogger ?? ((msg: string) => nomadLogger.warn(msg));

  if (hazards.length === 0) {
    return {
      shouldSkipTransaction: hasNotx,
      hazardsDetected: []
    };
  }

  if (hasNotx) {
    return {
      shouldSkipTransaction: true,
      hazardsDetected: hazards
    };
  }

  if (autoNotx) {
    const hazardTypes = hazards.map(h => h.type).join(", ");
    logFn(`Auto-notx: Disabling transaction due to hazardous operations: ${hazardTypes}`);
    return {
      shouldSkipTransaction: true,
      hazardsDetected: hazards
    };
  }

  const hazardList = hazards.map(h =>
    `  - ${h.type} at line ${h.line}, column ${h.column}`
  ).join("\n");

  throw new Error(
    `Hazardous operation detected that cannot run in a transaction:\n` +
    `${hazardList}\n\n` +
    `Use '-- +nomad notx' directive to disable transaction for this migration.`
  );
}
