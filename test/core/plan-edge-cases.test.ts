import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { MockedFunction } from "vitest";
import { Migrator } from "../../src/core/migrator.js";
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

describe("Plan Command Edge Cases", () => {
  let migrator: Migrator;
  let driver: DriverMock;
  let listFilesMock: MockedFunction<typeof listMigrationFiles>;
  let readFileMock: MockedFunction<typeof readFileSync>;
  let parseMock: MockedFunction<typeof parseNomadSqlFile>;
  let versionMock: MockedFunction<typeof filenameToVersion>;
  let migrationsByPath: Map<string, { up: string[]; down: string[]; checksum: string }>;
  let checksumByContent: Map<string, string>;

  const config: Config = {
    driver: "postgres",
    url: "postgresql://test:test@localhost:5432/testdb",
    dir: "/test/migrations",
    table: "nomad_migrations",
    allowDrift: false,
    autoNotx: false
  };

  beforeEach(() => {
    driver = createDriverMock();
    migrator = new Migrator(config, driver);

    migrationsByPath = new Map();
    checksumByContent = new Map();

    listFilesMock = vi.mocked(listMigrationFiles);
    readFileMock = vi.mocked(readFileSync as unknown as typeof readFileSync);
    parseMock = vi.mocked(parseNomadSqlFile);
    versionMock = vi.mocked(filenameToVersion);

    versionMock.mockImplementation((filepath: string) => {
      const match = filepath.match(/(\d{14})/);
      return match ? match[1] : undefined;
    });

    vi.mocked(calculateChecksum).mockImplementation((content: string) => {
      return checksumByContent.get(content) ?? `chk:${content}`;
    });

    readFileMock.mockImplementation((filepath: string) => {
      const entry = migrationsByPath.get(filepath);
      if (!entry) throw new Error(`Unexpected read for ${filepath}`);
      return ['-- up', ...entry.up, '-- down', ...entry.down].join('\n');
    });

    parseMock.mockImplementation((filepath: string) => {
      const entry = migrationsByPath.get(filepath);
      if (!entry) throw new Error(`Unexpected parse for ${filepath}`);
      return {
        up: { statements: entry.up, notx: false },
        down: { statements: entry.down, notx: false },
        noTransaction: false
      } as any;
    });

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function installMigrations(defs: Array<{ version: string; name: string; up: string[]; down: string[]; checksum?: string }>) {
    migrationsByPath.clear();
    const paths: string[] = [];
    for (const def of defs) {
      const checksum = def.checksum ?? `chk-${def.version}`;
      const filepath = `${config.dir}/${def.version}_${def.name}.sql`;
      migrationsByPath.set(filepath, { up: def.up, down: def.down, checksum });
      checksumByContent.set(['-- up', ...def.up, '-- down', ...def.down].join('\n'), checksum);
      paths.push(filepath);
    }
    listFilesMock.mockReturnValue(paths);
  }

  function enqueueConnections(appliedRows: Array<{ version: bigint; name: string; checksum: string; rolledBackAt: Date | null }>) {
    const ensureConn = driver.enqueueConnection({});
    ensureConn.ensureMigrationsTable.mockResolvedValue(undefined);

    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(
        appliedRows.map(row => ({ ...row, appliedAt: new Date("2024-01-01T00:00:00Z") }))
      )
    });

    return { ensureConn, fetchConn };
  }

  describe("Empty and Edge Cases", () => {
    it("handles missing migrations directory", async () => {
      listFilesMock.mockImplementation(() => {
        const err: any = new Error("ENOENT");
        err.code = "ENOENT";
        throw err;
      });
      enqueueConnections([]);

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(0);
      expect(plan.summary.total).toBe(0);
    });

    it("handles empty migration files", async () => {
      installMigrations([
        { version: "20240101120000", name: "empty", up: [], down: [] }
      ]);
      enqueueConnections([]);

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].statements).toHaveLength(0);
    });

    it("handles comment-only migrations", async () => {
      installMigrations([
        { version: "20240101120000", name: "comments_only", up: [], down: [] }
      ]);
      enqueueConnections([]);

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].statements).toHaveLength(0);
    });
  });

  describe("Version Edge Cases", () => {
    it("handles planning to unknown version", async () => {
      installMigrations([
        { version: "20240101120000", name: "one", up: ["SELECT 1;"], down: [] }
      ]);
      enqueueConnections([]);

      const plan = await migrator.planTo({ version: 99999999999999n });

      expect(plan.direction).toBe("up");
      expect(plan.migrations).toHaveLength(1);
    });

    it("plans rollback to version 0", async () => {
      installMigrations([
        { version: "20240101120000", name: "one", up: ["SELECT 1;"], down: ["SELECT 2;"] },
        { version: "20240102130000", name: "two", up: ["SELECT 3;"], down: ["SELECT 4;"] }
      ]);

      const applied = [
        { version: 20240101120000n, name: "one", checksum: "chk-20240101120000", rolledBackAt: null },
        { version: 20240102130000n, name: "two", checksum: "chk-20240102130000", rolledBackAt: null }
      ];
      enqueueConnections(applied);

      const plan = await migrator.planTo({ version: 0n });

      expect(plan.direction).toBe("down");
      expect(plan.migrations).toHaveLength(2);
    });
  });
});
