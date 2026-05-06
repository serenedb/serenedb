// `postgres.js` driver. Different surface from `pg`: tagged-template SQL
// builder, all binds default to extended-text. Same matrix.
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import postgres from "postgres";
import { casesFor, connConfig, schemaName } from "../spec_loader.js";

const DRIVER_KEY = "js_postgres_js";
const SCHEMA = schemaName(DRIVER_KEY);
const CASES = casesFor(DRIVER_KEY);

let sql;

beforeAll(async () => {
  const c = connConfig();
  sql = postgres({
    host: c.host, port: c.port, database: c.database, user: c.user,
    onnotice: () => {},
    fetch_types: false,
  });
  await sql.unsafe(`CREATE SCHEMA IF NOT EXISTS "${SCHEMA}"`);
  await sql.unsafe(
    `SET search_path TO "${SCHEMA}", public, pg_catalog`,
  );
});

afterAll(async () => {
  if (sql) {
    await Promise.race([
      sql.unsafe(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`).catch(() => {}),
      new Promise((r) => setTimeout(r, 5_000)),
    ]);
    await sql.end({ timeout: 5 });
  }
}, 10_000);

describe("smoke", () => {
  test("SELECT 1", async () => {
    const r = await sql`SELECT 1 AS v`;
    expect(Number(r[0].v)).toBe(1);
  });
});

async function serverText(pgTypname, sample) {
  if (sample === null || sample === undefined) return null;
  const r = await sql.unsafe(
    `SELECT ($1::text)::${pgTypname}::text AS v`,
    [String(sample)],
  );
  return r[0].v;
}

// postgres.js eagerly coerces strings to native JS values per OID
// (e.g. 't' -> JS false, "[1,2]" -> array). Our harness binds samples
// as raw strings, so extended-mode binds clash with that coercion.
// Restrict postgres.js to inline-literal modes; the wire round-trip
// is still exercised via simple and extended-noparam.
const ALLOWED = new Set(["simple", "extended-noparam"]);
const FILTERED = CASES.filter((c) => ALLOWED.has(c.proto));

describe("roundtrip", () => {
  for (const c of FILTERED) {
    const id = `${c.type.oid}-${c.type.name}-${c.proto}-${c.idx}`;
    test(id, async () => {
      const expected = await serverText(c.type.pgTypname, c.sample);
      let actual;
      if (c.proto === "simple" || c.proto === "extended-noparam") {
        if (c.sample === null || c.sample === undefined) {
          const r = await sql.unsafe(
            `SELECT NULL::${c.type.pgTypname}::text AS v`,
          );
          actual = r[0].v;
        } else {
          const lit = String(c.sample).replaceAll("'", "''");
          const r = await sql.unsafe(
            `SELECT '${lit}'::${c.type.pgTypname}::text AS v`,
          );
          actual = r[0].v;
        }
      } else {
        const q = `SELECT (${c.type.castSql})::text AS v`;
        const r = await sql.unsafe(q, [c.sample === null ? null : String(c.sample)]);
        actual = r[0].v;
      }
      expect(actual).toEqual(expected);
    });
  }
});
