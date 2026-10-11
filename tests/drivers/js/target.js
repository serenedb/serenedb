import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import { expect, test } from "vitest";
import { connConfig } from "./spec_loader.js";

export async function adminQuery(sql) {
  const client = new pg.Client(connConfig());
  await client.connect();
  try {
    return await client.query(sql);
  } finally {
    await client.end();
  }
}

export const SERENEDB = (
  await adminQuery("SELECT version() AS v")
).rows[0].v.includes("SereneDB");

export function databaseName(suite) {
  return `js_${suite}_${process.env.SDB_DRV_RUN_ID || "0"}`;
}

export function databaseConfig(database) {
  return { ...connConfig(), database };
}

export function databaseUrl(database) {
  const c = connConfig();
  return `postgresql://${c.user}@${c.host}:${c.port}/${database}`;
}

export async function recreateDatabase(database) {
  await dropDatabase(database);
  await adminQuery(`CREATE DATABASE "${database}"`);
}

export async function dropDatabase(database) {
  for (let attempt = 1; ; attempt++) {
    try {
      await adminQuery(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${database}' AND pid <> pg_backend_pid()`,
      );
      await adminQuery(`DROP DATABASE IF EXISTS "${database}"`);
      return;
    } catch (e) {
      if (e.code !== "55006" || attempt === 50) throw e;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
}

export function xfailOnSereneDB(reason) {
  if (!SERENEDB || process.env.SDB_DRV_RUNXFAIL === "1") return test;
  return (name, fn, timeout) =>
    test.fails(`${name} [SereneDB: ${reason}]`, fn, timeout);
}

export function checkGolden(file, text) {
  const parsed = path.parse(file);
  const twin = path.join(parsed.dir, `${parsed.name}.serenedb${parsed.ext}`);
  if (process.env.SDB_DRV_UPDATE_GOLDENS === "1") {
    if (!SERENEDB) {
      fs.mkdirSync(parsed.dir, { recursive: true });
      fs.writeFileSync(file, text);
    } else if (text === fs.readFileSync(file, "utf8")) {
      fs.rmSync(twin, { force: true });
    } else {
      fs.writeFileSync(twin, text);
    }
    return;
  }
  const expected = SERENEDB && fs.existsSync(twin) ? twin : file;
  expect(text).toBe(fs.readFileSync(expected, "utf8"));
}

export function stableJson(value) {
  return `${JSON.stringify(sortKeys(value), null, 1)}\n`;
}

function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === "object" && value.constructor === Object) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sortKeys(value[key])]),
    );
  }
  return value;
}
