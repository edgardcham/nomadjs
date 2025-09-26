import { SqlError } from "../core/errors.js";

export function formatCliError(error: any): string {
  if (!error) {
    return "";
  }

  if (error instanceof SqlError && error.file && error.line) {
    const columnPart = error.column ? `:${error.column}` : "";
    return `${error.file}:${error.line}${columnPart} - ${error.message}`;
  }

  return error.message || String(error);
}
