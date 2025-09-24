declare module "pg" {
  export interface QueryResultRow {
    [column: string]: unknown;
  }

  export interface QueryResult<R extends QueryResultRow = QueryResultRow> {
    rows: R[];
  }

  export interface PoolConfig {
    connectionString?: string;
  }

  export interface PoolClient {
    query<R extends QueryResultRow = QueryResultRow>(
      text: string,
      params?: ReadonlyArray<unknown>
    ): Promise<QueryResult<R>>;
    release(): void;
  }

  export class Pool {
    constructor(config?: PoolConfig);
    connect(): Promise<PoolClient>;
    query<R extends QueryResultRow = QueryResultRow>(
      text: string,
      params?: ReadonlyArray<unknown>
    ): Promise<QueryResult<R>>;
    end(): Promise<void>;
  }
}
