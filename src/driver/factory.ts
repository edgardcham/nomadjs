import type { Config } from "../config.js";
import type { Driver } from "./types.js";
import { createPostgresDriver } from "./postgres.js";

export interface DriverFactoryOptions {
  connectTimeoutMs?: number;
}

export function createDriver(config: Config, options: DriverFactoryOptions = {}): Driver {
  // Currently only postgres is supported
  return createPostgresDriver({
    url: config.url,
    table: config.table || "nomad_migrations",
    schema: config.schema || "public",
    connectTimeoutMs: options.connectTimeoutMs
  });
}
