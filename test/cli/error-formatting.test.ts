import { describe, it, expect } from "vitest";
import { SqlError } from "../../src/core/errors.js";
import { formatCliError } from "../../src/utils/format-error.js";

describe("CLI error formatting", () => {
  it("prints file:line:column prefix for SqlError", () => {
    const error = new SqlError("Failed UP 20240101120000 (create_users): syntax error", {
      file: "migrations/20240101120000_create_users.sql",
      line: 5,
      column: 7
    });

    const formatted = formatCliError(error);
    expect(formatted).toContain("migrations/20240101120000_create_users.sql:5:7");
    expect(formatted).toContain("syntax error");
  });

  it("falls back to error message when no location is present", () => {
    const error = new SqlError("Failed UP 20240101120000 (create_users): syntax error");
    const formatted = formatCliError(error);
    expect(formatted).toBe(error.message);
  });
});
