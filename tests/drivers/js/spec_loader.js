// Mirrors tests/drivers/harness/spec_loader.py for JS.
import fs from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";

export const PROTOCOLS = [
  "simple",
  "extended-noparam",
  "extended-text",
  "extended-binary",
];

const specDir =
  process.env.SDB_DRV_SPEC ||
  path.resolve(import.meta.dirname || ".", "..", "spec");

export function loadTypes() {
  const raw = parseYaml(
    fs.readFileSync(path.join(specDir, "types.yaml"), "utf8"),
  );
  if (!Array.isArray(raw)) {
    throw new Error("types.yaml must be a top-level list");
  }
  return raw.map((e) => {
    const name = e.name || e.pg_typname;
    const pgTypname = e.pg_typname || name;
    return {
      oid: Number(e.oid),
      name,
      pgTypname,
      castSql: e.cast_sql || `$1::${pgTypname}`,
      text: !!e.text,
      binary: !!e.binary,
      array: e.array,
      samples: Array.isArray(e.samples) ? e.samples : [],
      skip: e.skip || {},
    };
  });
}

export function casesFor(driverKey) {
  const typeRegex = new RegExp(process.env.SDB_DRV_TYPES || ".*");
  const protos = (
    process.env.SDB_DRV_PROTOCOLS || PROTOCOLS.join(",")
  ).split(",");
  const out = [];
  for (const t of loadTypes()) {
    if (!typeRegex.test(t.name)) continue;
    if (!t.text && !t.binary) continue;
    for (const proto of protos) {
      if (!PROTOCOLS.includes(proto)) continue;
      const supports = proto === "extended-binary" ? t.binary : t.text;
      if (!supports) continue;
      const skipModes = t.skip[driverKey] || [];
      if (skipModes.includes("all") || skipModes.includes(proto)) continue;
      t.samples.forEach((s, idx) =>
        out.push({ type: t, proto, idx, sample: s }),
      );
    }
  }
  return out;
}

export function connConfig() {
  return {
    host: process.env.SDB_DRV_HOST || "localhost",
    port: Number(process.env.SDB_DRV_PORT || "5432"),
    database: process.env.SDB_DRV_DATABASE || "serenedb",
    user: process.env.SDB_DRV_USER || "serenedb",
  };
}

export function schemaName(driverKey) {
  return `drv_${driverKey}_${process.env.SDB_DRV_RUN_ID || "0"}`;
}
