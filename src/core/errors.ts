/**
 * Standardized exit codes for NomadJS
 */
export enum ExitCode {
  SUCCESS = 0,
  SQL_ERROR = 1,
  DRIFT_DETECTED = 2,
  LOCK_TIMEOUT = 3,
  PARSE_CONFIG_ERROR = 4,
  MISSING_FILE = 5,
  CHECKSUM_MISMATCH = 6,
  CONNECTION_ERROR = 7
}

/**
 * Base class for NomadJS errors with exit codes
 */
export class NomadError extends Error {
  public readonly exitCode: ExitCode;

  constructor(message: string, exitCode: ExitCode) {
    super(message);
    this.exitCode = exitCode;
    this.name = this.constructor.name;
  }
}

/**
 * SQL execution error
 */
export class SqlError extends NomadError {
  constructor(message: string, public readonly sql?: string) {
    super(message, ExitCode.SQL_ERROR);
  }
}

/**
 * Drift detected in applied migrations
 */
export class DriftError extends NomadError {
  constructor(versions: string[]) {
    const msg = versions.length === 0
      ? `Drift detected in 0 migrations: `
      : versions.length === 1
      ? `Drift detected in migration ${versions[0]}`
      : `Drift detected in ${versions.length} migrations: ${versions.join(", ")}`;
    super(msg, ExitCode.DRIFT_DETECTED);
  }
}

/**
 * Lock acquisition timeout
 */
export class LockTimeoutError extends NomadError {
  constructor(timeout: number) {
    super(`Failed to acquire migration lock within ${timeout}ms`, ExitCode.LOCK_TIMEOUT);
  }
}

/**
 * Parse or configuration error
 */
export class ParseConfigError extends NomadError {
  constructor(message: string) {
    super(message, ExitCode.PARSE_CONFIG_ERROR);
  }
}

/**
 * Missing migration file
 */
export class MissingFileError extends NomadError {
  constructor(versions: string[]) {
    const msg = versions.length === 0
      ? `Missing 0 migration files: `
      : versions.length === 1
      ? `Missing migration file: ${versions[0]}`
      : `Missing ${versions.length} migration files: ${versions.join(", ")}`;
    super(msg, ExitCode.MISSING_FILE);
  }
}

/**
 * Checksum mismatch error
 */
export class ChecksumMismatchError extends NomadError {
  public readonly version: bigint;
  public readonly expectedChecksum: string;
  public readonly actualChecksum: string;
  public readonly filepath: string;

  constructor(details: {
    version: bigint;
    name: string;
    expectedChecksum: string;
    actualChecksum: string;
    filepath: string;
  }) {
    const message =
      `Checksum mismatch for migration ${details.version} (${details.name}):\n` +
      `  File: ${details.filepath}\n` +
      `  Expected: ${details.expectedChecksum}\n` +
      `  Actual: ${details.actualChecksum}\n` +
      `\n` +
      `The migration file has been modified after it was applied to the database.\n` +
      `This could lead to inconsistencies. To proceed:\n` +
      `  1. Restore the original migration file, or\n` +
      `  2. Use --allow-drift flag (NOT RECOMMENDED for production)`;
    super(message, ExitCode.CHECKSUM_MISMATCH);
    this.version = details.version;
    this.expectedChecksum = details.expectedChecksum;
    this.actualChecksum = details.actualChecksum;
    this.filepath = details.filepath;
  }
}

/**
 * Database connection error
 */
export class ConnectionError extends NomadError {
  constructor(message: string) {
    super(`Database connection error: ${message}`, ExitCode.CONNECTION_ERROR);
  }
}

/**
 * Helper to get exit code description for help text
 */
export function getExitCodeDescription(code: ExitCode): string {
  switch (code) {
    case ExitCode.SUCCESS:
      return "Success";
    case ExitCode.SQL_ERROR:
      return "SQL execution error";
    case ExitCode.DRIFT_DETECTED:
      return "Drift detected in applied migrations";
    case ExitCode.LOCK_TIMEOUT:
      return "Lock acquisition timeout";
    case ExitCode.PARSE_CONFIG_ERROR:
      return "Parse or configuration error";
    case ExitCode.MISSING_FILE:
      return "Missing migration file";
    case ExitCode.CHECKSUM_MISMATCH:
      return "Checksum mismatch";
    case ExitCode.CONNECTION_ERROR:
      return "Database connection error";
    default:
      return "Unknown error";
  }
}

/**
 * Format exit codes for help text
 */
export function formatExitCodesHelp(): string {
  const codes = [
    ExitCode.SUCCESS,
    ExitCode.SQL_ERROR,
    ExitCode.DRIFT_DETECTED,
    ExitCode.LOCK_TIMEOUT,
    ExitCode.PARSE_CONFIG_ERROR,
    ExitCode.MISSING_FILE,
    ExitCode.CHECKSUM_MISMATCH,
    ExitCode.CONNECTION_ERROR
  ];

  return codes
    .map(code => `  ${code} - ${getExitCodeDescription(code)}`)
    .join("\n");
}