import type { Config } from "../config.js";
import type { Driver } from "./types.js";
import { createPostgresDriver } from "./postgres.js";
import { createMySqlDriver } from "./mysql.js";

export interface DriverFactoryOptions {
  connectTimeoutMs?: number;
}

export function createDriver(config: Config, options: DriverFactoryOptions = {}): Driver {
  if (config.driver === "mysql") {
    return createMySqlDriver({
      url: config.url,
      table: config.table || "nomad_migrations",
      schema: config.schema,
      connectTimeoutMs: options.connectTimeoutMs
    });
  }

  return createPostgresDriver({
    url: config.url,
    table: config.table || "nomad_migrations",
    schema: config.schema || "public",
    connectTimeoutMs: options.connectTimeoutMs
  });
}
