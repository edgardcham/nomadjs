import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { MockedFunction } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
import { MissingFileError } from "../../src/core/errors.js";
import { listMigrationFiles, filenameToVersion } from "../../src/core/files.js";
import { parseNomadSqlFile } from "../../src/parser/enhanced-parser.js";
import { calculateChecksum } from "../../src/core/checksum.js";
import { readFileSync } from "node:fs";
import type { Config } from "../../src/config.js";
import { createDriverMock, type DriverMock } from "../helpers/driver-mock.js";

vi.mock("node:fs");
vi.mock("../../src/core/files.js");
vi.mock("../../src/parser/enhanced-parser.js");
vi.mock("../../src/core/checksum.js");

describe.each(["postgres", "mysql", "sqlite"] as const)("Status Command (%s)", (flavor) => {
  let config: Config;
  let migrator: Migrator;
  let driver: DriverMock;
  let listFilesMock: MockedFunction<typeof listMigrationFiles>;
  let readFileMock: MockedFunction<typeof readFileSync>;
  let parseMock: MockedFunction<typeof parseNomadSqlFile>;
  let versionMock: MockedFunction<typeof filenameToVersion>;
  let migrationsByPath: Map<string, { content: string; checksum: string }>;

  beforeEach(() => {
    config = {
      driver: flavor,
      url:
        flavor === "postgres"
          ? "postgresql://test:test@localhost:5432/testdb"
          : flavor === "mysql"
            ? "mysql://test:test@localhost:3306/testdb"
            : "sqlite:///tmp/testdb.sqlite",
      dir: "/test/migrations",
      table: "nomad_migrations",
      schema: flavor === "postgres" ? "public" : undefined,
      allowDrift: false
    };

    driver = createDriverMock({ flavor });
    migrator = new Migrator(config, driver);

    migrationsByPath = new Map();

    listFilesMock = vi.mocked(listMigrationFiles);
    readFileMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
    parseMock = vi.mocked(parseNomadSqlFile);
    versionMock = vi.mocked(filenameToVersion);

    versionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    readFileMock.mockImplementation((filepath: string) => {
      const entry = migrationsByPath.get(filepath);
      if (!entry) throw new Error(`Unexpected read for ${filepath}`);
      return entry.content;
    });

    parseMock.mockImplementation((filepath: string) => {
      const entry = migrationsByPath.get(filepath);
      if (!entry) throw new Error(`Unexpected parse for ${filepath}`);
      return {
        up: { statements: [], notx: false },
        down: { statements: [], notx: false },
        noTransaction: false,
        tags: []
      } as any;
    });

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(process, "exit").mockImplementation((() => {}) as any);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function installMigrations(defs: Array<{ version: string; name: string; content: string }>) {
    const paths: string[] = [];
    migrationsByPath.clear();
    defs.forEach(def => {
      const filepath = `${config.dir}/${def.version}_${def.name}.sql`;
      migrationsByPath.set(filepath, { content: def.content, checksum: calculateChecksum(def.content) });
      paths.push(filepath);
    });
    listFilesMock.mockReturnValue(paths);
  }

  function enqueueConnections(appliedRows: Array<{ version: bigint; name: string; checksum: string; appliedAt?: Date; rolledBackAt?: Date | null }>) {
    const ensureConn = driver.enqueueConnection({});
    ensureConn.ensureMigrationsTable.mockResolvedValue(undefined);

    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(
        appliedRows.map(row => ({
          version: row.version,
          name: row.name,
          checksum: row.checksum,
          appliedAt: row.appliedAt ?? new Date("2024-01-01T00:00:00Z"),
          rolledBackAt: row.rolledBackAt ?? null
        }))
      )
    });

    return { ensureConn, fetchConn };
  }

  describe("Basic Status", () => {
    it("shows pending migrations", async () => {
      installMigrations([
        { version: "20240101120000", name: "create_users", content: "CREATE TABLE users (id INT);" },
        { version: "20240102130000", name: "add_email", content: "ALTER TABLE users ADD email TEXT;" }
      ]);

      enqueueConnections([]);

      const status = await migrator.status();

      expect(status).toHaveLength(2);
      expect(status[0].applied).toBe(false);
      expect(status[1].applied).toBe(false);
    });

    it("shows applied migrations", async () => {
      const content = "CREATE TABLE users (id INT);";
      installMigrations([
        { version: "20240101120000", name: "create_users", content }
      ]);

      const checksum = calculateChecksum(content);
      const appliedAt = new Date("2024-01-01T12:00:00Z");
      enqueueConnections([
        { version: 20240101120000n, name: "create_users", checksum, appliedAt }
      ]);

      const status = await migrator.status();

      expect(status).toHaveLength(1);
      expect(status[0].applied).toBe(true);
      expect(status[0].appliedAt).toEqual(appliedAt);
    });

    it("shows mixed applied and pending", async () => {
      const content = "SELECT 1;";
      installMigrations([
        { version: "20240101120000", name: "one", content },
        { version: "20240102130000", name: "two", content },
        { version: "20240103140000", name: "three", content }
      ]);

      const checksum = calculateChecksum(content);
      enqueueConnections([
        { version: 20240101120000n, name: "one", checksum }
      ]);

      const status = await migrator.status();

      expect(status.map(s => s.applied)).toEqual([true, false, false]);
    });
  });

  describe("Checksum and drift", () => {
    it("marks drift when checksum differs", async () => {
      const content = "CREATE TABLE users (id INT);";
      installMigrations([
        { version: "20240101120000", name: "create_users", content }
      ]);

      enqueueConnections([
        { version: 20240101120000n, name: "create_users", checksum: "different" }
      ]);

      await expect(migrator.status()).rejects.toThrow(/drift/i);
    });

    it("allows drift when allowDrift=true", async () => {
      migrator = new Migrator({ ...config, allowDrift: true }, driver);
      const content = "CREATE TABLE users (id INT);";
      installMigrations([
        { version: "20240101120000", name: "create_users", content }
      ]);

      enqueueConnections([
        { version: 20240101120000n, name: "create_users", checksum: "different" }
      ]);

      const status = await migrator.status();
      expect(status[0].hasDrift).toBe(true);
    });
  });

  describe("Missing files", () => {
    it("marks applied migrations missing from disk", async () => {
      installMigrations([
        { version: "20240101120000", name: "create_users", content: "CREATE TABLE users (id INT);" }
      ]);

      const presentChecksum = calculateChecksum("CREATE TABLE users (id INT);");
      enqueueConnections([
        { version: 20240101120000n, name: "create_users", checksum: presentChecksum, rolledBackAt: null },
        { version: 20240102130000n, name: "missing", checksum: "chk-missing", rolledBackAt: null }
      ]);

      await expect(migrator.status()).rejects.toThrow(MissingFileError);
    });
  });
});
