import { createHash } from "node:crypto";
import { ChecksumMismatchError } from "./errors.js";

/**
 * Normalize content for consistent checksum calculation.
 * - Removes BOM
 * - Normalizes all line endings (CRLF, CR, LF) to LF
 */
function normalizeForChecksum(content: string): string {
  // Remove BOM if present
  if (content.charCodeAt(0) === 0xFEFF) {
    content = content.slice(1);
  }
  // Normalize all line ending styles to LF
  // First convert CRLF to LF, then convert remaining CR to LF
  return content.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

/**
 * Calculate SHA-256 checksum of file content.
 * Content is normalized (BOM removed, CRLF -> LF) before hashing.
 */
export function calculateChecksum(content: string): string {
  const normalized = normalizeForChecksum(content);
  const hash = createHash("sha256");
  hash.update(normalized, "utf8");
  return hash.digest("hex");
}

/**
 * Verify that content matches expected checksum.
 * Comparison is case-insensitive.
 */
export function verifyChecksum(content: string, expectedChecksum: string): boolean {
  if (!expectedChecksum) {
    return false;
  }
  const actualChecksum = calculateChecksum(content);
  return actualChecksum.toLowerCase() === expectedChecksum.toLowerCase();
}

/**
 * Migration with checksum metadata
 */
export interface MigrationWithChecksum {
  version: bigint;
  name: string;
  filepath: string;
  content: string;
  checksum: string;
}


/**
 * Validate checksum format (64 hex characters)
 */
export function isValidChecksum(checksum: string): boolean {
  return /^[a-f0-9]{64}$/i.test(checksum);
}