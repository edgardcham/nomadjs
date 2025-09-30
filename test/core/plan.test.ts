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

type MigrationDef = {
  version: bigint;
  name: string;
  up: string[];
  down: string[];
  upNotx?: boolean;
  downNotx?: boolean;
  checksum: string;
  content: string;
};

type AppliedRow = {
  version: bigint;
  name: string;
  checksum: string;
  appliedAt: Date;
  rolledBackAt: Date | null;
};

describe.each(["postgres", "mysql", "sqlite"] as const)("Planner integration (%s)", (flavor) => {
  let config: Config;
  let driver: DriverMock;
  let migrator: Migrator;
  let listFilesMock: MockedFunction<typeof listMigrationFiles>;
  let readFileMock: MockedFunction<typeof readFileSync>;
  let parseMock: MockedFunction<typeof parseNomadSqlFile>;
  let versionMock: MockedFunction<typeof filenameToVersion>;
  let migrationsByPath: Map<string, MigrationDef>;
  let checksumByContent: Map<string, string>;

  beforeEach(() => {
    driver = createDriverMock({ flavor });

    config = {
      driver: flavor,
      url:
        flavor === "postgres"
          ? "postgresql://test:test@localhost:5432/testdb"
          : flavor === "mysql"
            ? "mysql://test:test@localhost:3306/testdb"
            : "sqlite:///tmp/plan.sqlite",
      dir: "/test/migrations",
      table: "nomad_migrations",
      schema: flavor === "postgres" ? "public" : undefined,
      allowDrift: false,
      autoNotx: false
    };

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
      const def = migrationsByPath.get(filepath);
      if (!def) throw new Error(`Unexpected read for ${filepath}`);
      return def.content;
    });

    parseMock.mockImplementation((filepath: string) => {
      const def = migrationsByPath.get(filepath);
      if (!def) throw new Error(`Unexpected parse for ${filepath}`);
      const upMeta = def.up.map((sql, idx) => ({ sql, line: idx + 1, column: 1 }));
      const downMeta = def.down.map((sql, idx) => ({ sql, line: idx + 1, column: 1 }));
      return {
        up: { statements: def.up, notx: def.upNotx ?? false, statementMeta: upMeta },
        down: { statements: def.down, notx: def.downNotx ?? false, statementMeta: downMeta },
        noTransaction: false,
        tags: []
      } as any;
    });

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "info").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function createMigration(version: string, name: string, opts: {
    up: string[];
    down: string[];
    upNotx?: boolean;
    downNotx?: boolean;
    checksum?: string;
  }): MigrationDef {
    const checksum = opts.checksum ?? `chk-${version}`;
    const content = ['-- up', ...opts.up, '-- down', ...opts.down].join('\n');
    checksumByContent.set(content, checksum);
    return {
      version: BigInt(version),
      name,
      up: opts.up,
      down: opts.down,
      upNotx: opts.upNotx,
      downNotx: opts.downNotx,
      checksum,
      content
    };
  }

  function installMigrations(defs: MigrationDef[]) {
    const paths: string[] = [];
    migrationsByPath.clear();
    for (const def of defs) {
      const filepath = `${config.dir}/${def.version}_${def.name}.sql`;
      migrationsByPath.set(filepath, def);
      paths.push(filepath);
    }
    listFilesMock.mockReturnValue(paths);
  }

  function appliedRow(def: MigrationDef, overrides: Partial<AppliedRow> = {}): AppliedRow {
    return {
      version: def.version,
      name: def.name,
      checksum: def.checksum,
      appliedAt: new Date("2024-01-01T00:00:00Z"),
      rolledBackAt: null,
      ...overrides
    };
  }

  function enqueuePlanConnections(appliedRows: AppliedRow[]) {
    const ensureConn = driver.enqueueConnection({});
    ensureConn.ensureMigrationsTable.mockResolvedValue(undefined);

    const fetchConn = driver.enqueueConnection({
      fetchAppliedMigrations: vi.fn().mockResolvedValue(appliedRows)
    });

    return { ensureConn, fetchConn };
  }

  describe("plan up", () => {
    it("lists pending migrations in order", async () => {
      const mig1 = createMigration("20240101120000", "create_users", {
        up: ["CREATE TABLE users (id INT);"],
        down: ["DROP TABLE users;"]
      });
      const mig2 = createMigration("20240102130000", "add_email", {
        up: ["ALTER TABLE users ADD email TEXT;"],
        down: ["ALTER TABLE users DROP COLUMN email;"]
      });
      const mig3 = createMigration("20240103140000", "add_index", {
        up: ["CREATE INDEX idx_email ON users(email);"],
        down: ["DROP INDEX idx_email;"]
      });
      installMigrations([mig1, mig2, mig3]);

      enqueuePlanConnections([appliedRow(mig1)]);

      const plan = await migrator.planUp();

      expect(plan.direction).toBe("up");
      expect(plan.migrations.map(m => m.version)).toEqual([mig2.version, mig3.version]);
      expect(plan.summary.total).toBe(2);
      expect(plan.migrations.every(m => m.transaction === (flavor === "postgres"))).toBe(true);
    });

    it("detects hazardous operations", async () => {
      const hazardous = createMigration("20240101120000", "create_index_concurrently", {
        up: ["CREATE INDEX CONCURRENTLY idx_email ON users(email);"] ,
        down: ["DROP INDEX idx_email;"]
      });
      installMigrations([hazardous]);
      enqueuePlanConnections([]);

      const plan = await migrator.planUp();

      expect(plan.migrations).toHaveLength(1);
      expect(plan.migrations[0].hazards?.[0].type).toBe("CREATE_INDEX_CONCURRENTLY");
      expect(plan.migrations[0].transaction).toBe(false);
      expect(plan.summary.hazardCount).toBe(1);
    });

    it("respects notx directive", async () => {
      const notx = createMigration("20240101120000", "vacuum", {
        up: ["VACUUM ANALYZE users;"],
        down: [],
        upNotx: true
      });
      installMigrations([notx]);
      enqueuePlanConnections([]);

      const plan = await migrator.planUp();

      expect(plan.migrations[0].transaction).toBe(false);
      expect(plan.migrations[0].reason).toBe("notx directive");
    });

    it("obeys limit option", async () => {
      const mig1 = createMigration("20240101120000", "one", { up: ["SELECT 1;"], down: [] });
      const mig2 = createMigration("20240102130000", "two", { up: ["SELECT 2;"], down: [] });
      const mig3 = createMigration("20240103140000", "three", { up: ["SELECT 3;"], down: [] });
      installMigrations([mig1, mig2, mig3]);
      enqueuePlanConnections([]);

      const plan = await migrator.planUp({ limit: 2 });

      expect(plan.migrations.map(m => m.version)).toEqual([mig1.version, mig2.version]);
    });
  });

  describe("plan down", () => {
    it("lists rollback migrations in reverse order", async () => {
      const mig1 = createMigration("20240101120000", "create_users", {
        up: ["CREATE TABLE users (id INT);"],
        down: ["DROP TABLE users;"]
      });
      const mig2 = createMigration("20240102130000", "add_email", {
        up: ["ALTER TABLE users ADD email TEXT;"],
        down: ["ALTER TABLE users DROP COLUMN email;"]
      });
      installMigrations([mig1, mig2]);

      enqueuePlanConnections([appliedRow(mig1), appliedRow(mig2)]);

      const plan = await migrator.planDown({ count: 2 });

      expect(plan.direction).toBe("down");
      expect(plan.migrations.map(m => m.version)).toEqual([mig2.version, mig1.version]);
      expect(plan.migrations[0].statements?.[0]).toContain("DROP COLUMN email");
    });

    it("detects hazards in down direction", async () => {
      const mig = createMigration("20240101120000", "reindex", {
        up: ["CREATE INDEX idx_test ON users(email);"] ,
        down: ["REINDEX INDEX idx_test;"]
      });
      installMigrations([mig]);
      enqueuePlanConnections([appliedRow(mig)]);

      const plan = await migrator.planDown({ count: 1 });

      expect(plan.migrations[0].hazards?.[0].type).toBe("REINDEX");
      expect(plan.migrations[0].transaction).toBe(false);
    });

    it("skips migrations already rolled back", async () => {
      const mig1 = createMigration("20240101120000", "one", { up: ["SELECT 1;"], down: ["SELECT 1;" ] });
      const mig2 = createMigration("20240102130000", "two", { up: ["SELECT 2;"], down: ["SELECT 2;"] });
      installMigrations([mig1, mig2]);

      enqueuePlanConnections([
        appliedRow(mig1),
        appliedRow(mig2, { rolledBackAt: new Date("2024-01-02T00:00:00Z") })
      ]);

      const plan = await migrator.planDown({ count: 2 });

      expect(plan.migrations.map(m => m.version)).toEqual([mig1.version]);
    });
  });

  describe("plan to version", () => {
    it("plans forward migrations to reach target", async () => {
      const mig1 = createMigration("20240101120000", "one", { up: ["SELECT 1;"], down: [] });
      const mig2 = createMigration("20240102130000", "two", { up: ["SELECT 2;"], down: [] });
      const mig3 = createMigration("20240103140000", "three", { up: ["SELECT 3;"], down: [] });
      const mig4 = createMigration("20240104150000", "four", { up: ["SELECT 4;"], down: [] });
      installMigrations([mig1, mig2, mig3, mig4]);

      enqueuePlanConnections([appliedRow(mig1)]);

      const plan = await migrator.planTo({ version: mig3.version });

      expect(plan.direction).toBe("up");
      expect(plan.migrations.map(m => m.version)).toEqual([mig2.version, mig3.version]);
    });

    it("plans rollback migrations when target is behind", async () => {
      const mig1 = createMigration("20240101120000", "one", { up: ["SELECT 1;"], down: [] });
      const mig2 = createMigration("20240102130000", "two", { up: ["SELECT 2;"], down: [] });
      const mig3 = createMigration("20240103140000", "three", { up: ["SELECT 3;"], down: [] });
      installMigrations([mig1, mig2, mig3]);

      enqueuePlanConnections([appliedRow(mig1), appliedRow(mig2), appliedRow(mig3)]);

      const plan = await migrator.planTo({ version: mig1.version });

      expect(plan.direction).toBe("down");
      expect(plan.migrations.map(m => m.version)).toEqual([mig3.version, mig2.version]);
    });
  });
});
