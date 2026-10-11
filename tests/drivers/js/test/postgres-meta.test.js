import path from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { PostgresMeta } from "@supabase/postgres-meta";
import {
  adminQuery,
  checkGolden,
  databaseConfig,
  databaseName,
  databaseUrl,
  dropDatabase,
  recreateDatabase,
  stableJson,
  xfailOnSereneDB,
} from "../target.js";

const FIXTURES = path.resolve(import.meta.dirname, "..", "fixtures", "postgres-meta");
const MAIN = databaseName("postgres_meta");
const ROLE = `${MAIN}_analyst`;
const PUBLIC = { includedSchemas: ["public"] };
const VOLATILE = new Set(["bytes", "size", "live_rows_estimate", "dead_rows_estimate"]);
const JSONB_AGG = "jsonb_agg is missing";

let meta;

async function execute(sql) {
  const client = new pg.Client(databaseConfig(MAIN));
  await client.connect();
  try {
    await client.query(sql);
  } finally {
    await client.end();
  }
}

function normalize(value) {
  if (Array.isArray(value)) {
    return value
      .map(normalize)
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !VOLATILE.has(key) && !/(^|_)(id|ids|oid)$/.test(key))
        .map(([key, v]) => [key, normalize(v)]),
    );
  }
  if (typeof value === "string") {
    return value.replaceAll(ROLE, "<role>").replaceAll(MAIN, "<database>");
  }
  return value;
}

async function listed(name, call) {
  const { data, error } = await call();
  if (error) throw new Error(error.message);
  checkGolden(path.join(FIXTURES, `${name}.json`), stableJson(normalize(data)));
  return data;
}

beforeAll(async () => {
  await recreateDatabase(MAIN);
  await adminQuery(`DROP ROLE IF EXISTS "${ROLE}"`);
  await adminQuery(`CREATE ROLE "${ROLE}" NOLOGIN`);
  await execute(`
    CREATE TYPE mood AS ENUM ('happy', 'sad');
    CREATE TABLE teams (
      id serial PRIMARY KEY,
      name varchar(50) NOT NULL UNIQUE,
      founded date,
      CONSTRAINT teams_name_length CHECK (char_length(name) > 1)
    );
    COMMENT ON TABLE teams IS 'esports teams';
    COMMENT ON COLUMN teams.name IS 'display name';
    CREATE TABLE players (
      id serial PRIMARY KEY,
      team_id integer REFERENCES teams (id),
      nick text NOT NULL,
      mood mood DEFAULT 'happy',
      rating numeric(6, 2),
      tags text[]
    );
    CREATE INDEX players_nick ON players (nick);
    CREATE VIEW team_rosters AS SELECT t.name, p.nick FROM teams t JOIN players p ON p.team_id = t.id;
    CREATE FUNCTION add_points(a integer, b integer) RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT a + b';
    GRANT SELECT ON teams TO "${ROLE}";
  `);
  meta = new PostgresMeta({ connectionString: databaseUrl(MAIN), max: 1 });
});

afterAll(async () => {
  if (meta) await meta.end();
  await execute(`REVOKE ALL ON teams FROM "${ROLE}"`).catch(() => {});
  await dropDatabase(MAIN);
  await adminQuery(`DROP ROLE IF EXISTS "${ROLE}"`);
});

describe("postgres-meta", () => {
  test("version and config", async () => {
    const { data: version, error } = await meta.version.retrieve();
    expect(error).toBeNull();
    expect(version.version_number).toBeGreaterThanOrEqual(180000);
    checkGolden(
      path.join(FIXTURES, "version.json"),
      stableJson({ max_connections: version.max_connections }),
    );
    const { data: config } = await meta.config.list();
    expect(config.map((c) => c.name)).toEqual(
      expect.arrayContaining(["search_path", "TimeZone", "server_version"]),
    );
  });

  test("schemas", async () => {
    await listed("schemas", () => meta.schemas.list());
  });

  test("columns", async () => {
    await listed("columns", () => meta.columns.list(PUBLIC));
  });

  test("views", async () => {
    await listed("views", () => meta.views.list({ ...PUBLIC, includeColumns: true }));
  });

  test("foreign tables, triggers and extensions", async () => {
    await listed("foreign-tables", () => meta.foreignTables.list(PUBLIC));
    await listed("triggers", () => meta.triggers.list(PUBLIC));
    const { data: extensions } = await meta.extensions.list();
    checkGolden(
      path.join(FIXTURES, "installed-extensions.json"),
      stableJson(extensions.filter((e) => e.installed_version).map((e) => e.name).sort()),
    );
  });

  test("roles", async () => {
    const { data, error } = await meta.roles.list();
    expect(error).toBeNull();
    checkGolden(
      path.join(FIXTURES, "roles.json"),
      stableJson(normalize(data.filter((r) => r.name === ROLE))),
    );
  });

  xfailOnSereneDB(JSONB_AGG)("tables", async () => {
    await listed("tables", () => meta.tables.list({ ...PUBLIC, includeColumns: true }));
  });

  xfailOnSereneDB(
    "SELECT without a select list (EXISTS (SELECT FROM ...)) is rejected",
  )("types", async () => {
    await listed("types", () => meta.types.list(PUBLIC));
  });

  xfailOnSereneDB("array_fill is missing")("functions", async () => {
    await listed("functions", () => meta.functions.list(PUBLIC));
  });

  xfailOnSereneDB(JSONB_AGG)("indexes", async () => {
    await listed("indexes", () => meta.indexes.list(PUBLIC));
  });

  xfailOnSereneDB(JSONB_AGG)("relationships", async () => {
    await listed("relationships", () => meta.relationships.list(PUBLIC));
  });

  xfailOnSereneDB(JSONB_AGG)("table and column privileges", async () => {
    await listed("table-privileges", () => meta.tablePrivileges.list(PUBLIC));
    await listed("column-privileges", () => meta.columnPrivileges.list(PUBLIC));
  });

  xfailOnSereneDB("CREATE MATERIALIZED VIEW is a syntax error")(
    "materialized views",
    async () => {
      await execute(
        "CREATE MATERIALIZED VIEW team_sizes AS SELECT team_id, count(*) AS n FROM players GROUP BY team_id",
      );
      try {
        await listed("materialized-views", () =>
          meta.materializedViews.list({ ...PUBLIC, includeColumns: true }),
        );
      } finally {
        await execute("DROP MATERIALIZED VIEW team_sizes");
      }
    },
  );

  xfailOnSereneDB("ALTER TABLE ... ENABLE ROW LEVEL SECURITY and CREATE POLICY are missing")(
    "policies",
    async () => {
      await execute(`
        ALTER TABLE players ENABLE ROW LEVEL SECURITY;
        CREATE POLICY players_visible ON players FOR SELECT TO "${ROLE}" USING (true);
      `);
      try {
        await listed("policies", () => meta.policies.list(PUBLIC));
      } finally {
        await execute(`
          DROP POLICY players_visible ON players;
          ALTER TABLE players DISABLE ROW LEVEL SECURITY;
        `);
      }
    },
  );

  xfailOnSereneDB("CREATE PUBLICATION is missing")("publications", async () => {
    await execute(`CREATE PUBLICATION "${MAIN}_pub" FOR TABLE teams`);
    try {
      const { data, error } = await meta.publications.list({});
      expect(error).toBeNull();
      checkGolden(
        path.join(FIXTURES, "publications.json"),
        stableJson(normalize(data.filter((p) => p.name === `${MAIN}_pub`))),
      );
    } finally {
      await execute(`DROP PUBLICATION "${MAIN}_pub"`);
    }
  });

  xfailOnSereneDB(JSONB_AGG)("TypeScript type generation", async () => {
    process.env.PG_META_DB_URL = databaseUrl(MAIN);
    const { build } = await import("@supabase/postgres-meta/dist/server/app.js");
    const app = build({ logger: false });
    try {
      const response = await app.inject({
        method: "GET",
        url: "/generators/typescript?included_schemas=public",
        headers: { pg: databaseUrl(MAIN) },
      });
      expect(response.statusCode).toBe(200);
      checkGolden(path.join(FIXTURES, "database.types.ts"), response.body);
    } finally {
      await app.close();
    }
  });
});
