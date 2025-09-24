import { vi } from "vitest";

export interface PgResponder {
  match: (sql: string) => boolean;
  handler: (sql: string, params?: unknown[]) => { rows: any[] } | void;
}

export function createPgQueryMock(responders: PgResponder[]): ReturnType<typeof vi.fn> {
  return vi.fn(async (sql: string, params?: unknown[]) => {
    if (typeof sql === "string") {
      if (sql.includes("pg_try_advisory_lock")) {
        return { rows: [{ pg_try_advisory_lock: true }] };
      }
      if (sql.includes("pg_advisory_unlock")) {
        return { rows: [{ pg_advisory_unlock: true }] };
      }
      if (sql.includes("pg_locks")) {
        return { rows: [] };
      }

      for (const responder of responders) {
        if (responder.match(sql)) {
          const result = responder.handler(sql, params);
          if (result) {
            return result;
          }
        }
      }
    }
    return { rows: [] };
  });
}
