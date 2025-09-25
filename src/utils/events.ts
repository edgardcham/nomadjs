import { logger } from "./logger.js";

export type EventRecord = Record<string, unknown> & { event: string };

export function emitEvent(enabled: boolean | undefined, record: EventRecord): void {
  if (!enabled) return;
  try {
    // Emit raw JSON to stdout to keep it machine-friendly
    // Use console.log directly (not colorized logger)
    // eslint-disable-next-line no-console
    console.log(JSON.stringify(record));
  } catch (e) {
    // Fallback to warn if JSON serialization fails
    logger.warn(`Failed to emit event: ${(e as Error).message}`);
  }
}

export function previewSql(sql: string, max: number = 60): string {
  return sql.length > max ? sql.slice(0, max - 3) + "..." : sql;
}

