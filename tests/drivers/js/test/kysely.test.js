import path from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { Kysely, PostgresDialect, sql } from "kysely";
import { Migrator } from "kysely/migration";
import { jsonArrayFrom } from "kysely/helpers/postgres";
import {
  checkGolden,
  databaseConfig,
  databaseName,
  dropDatabase,
  recreateDatabase,
  stableJson,
  xfailOnSereneDB,
} from "../target.js";

const FIXTURES = path.resolve(import.meta.dirname, "..", "fixtures", "kysely");
const MAIN = databaseName("kysely");
const MIGRATIONS = databaseName("kysely_migrations");
const DATABASES = [MAIN, MIGRATIONS];

function connect(database) {
  return new Kysely({
    dialect: new PostgresDialect({ pool: new pg.Pool(databaseConfig(database)) }),
  });
}

let db;

beforeAll(async () => {
  for (const database of DATABASES) await recreateDatabase(database);
  db = connect(MAIN);
  await db.schema.createType("hero_role").asEnum(["carry", "support", "offlane"]).execute();
  await db.schema
    .createTable("heroes")
    .addColumn("id", "serial", (c) => c.primaryKey())
    .addColumn("name", "varchar(100)", (c) => c.notNull().unique())
    .addColumn("title", "text")
    .addColumn("role", sql`hero_role`, (c) => c.notNull().defaultTo("carry"))
    .addColumn("level", "int2", (c) => c.notNull().defaultTo(1))
    .addColumn("gold", "int8", (c) => c.notNull().defaultTo(0))
    .addColumn("rating", "float8")
    .addColumn("power", "numeric(10, 2)", (c) => c.notNull().defaultTo(0))
    .addColumn("active", "boolean", (c) => c.notNull().defaultTo(true))
    .addColumn("born", "date")
    .addColumn("portrait", "bytea")
    .addColumn("aliases", sql`text[]`)
    .addColumn("uid", "uuid", (c) => c.notNull().defaultTo(sql`gen_random_uuid()`))
    .addColumn("created_at", "timestamptz", (c) => c.notNull().defaultTo(sql`now()`))
    .addCheckConstraint("heroes_gold_nonneg", sql`gold >= 0`)
    .execute();
  await db.schema
    .createTable("items")
    .addColumn("id", "serial", (c) => c.primaryKey())
    .addColumn("name", "varchar(60)", (c) => c.notNull())
    .addColumn("cost", "integer", (c) => c.notNull().defaultTo(0))
    .addColumn("hero_id", "integer", (c) => c.references("heroes.id"))
    .addUniqueConstraint("items_hero_name", ["hero_id", "name"])
    .execute();
  await db.schema.createIndex("items_name_cost").on("items").columns(["name", "cost"]).execute();
  await sql`COMMENT ON COLUMN heroes.title IS 'epithet'`.execute(db);
});

async function createHeroItemsView() {
  await db.schema
    .createView("hero_items")
    .as(
      db
        .selectFrom("items")
        .innerJoin("heroes", "heroes.id", "items.hero_id")
        .select(["heroes.name as hero", "items.name as item", "items.cost"]),
    )
    .execute();
}

function describeTables(tables) {
  return tables
    .map((t) => ({ name: t.name, schema: t.schema, isView: t.isView, columns: t.columns }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

afterAll(async () => {
  if (db) await db.destroy();
  for (const database of DATABASES) await dropDatabase(database);
});

describe("introspection", () => {
  test("lists schemas, tables and columns", async () => {
    const schemas = (await db.introspection.getSchemas()).map((s) => s.name).sort();
    checkGolden(path.join(FIXTURES, "schemas.json"), stableJson(schemas));
    checkGolden(
      path.join(FIXTURES, "tables.json"),
      stableJson(describeTables(await db.introspection.getTables())),
    );
  });

  test("lists views", async () => {
    await createHeroItemsView();
    try {
      const views = describeTables(await db.introspection.getTables()).filter(
        (t) => t.isView,
      );
      checkGolden(path.join(FIXTURES, "views.json"), stableJson(views));
    } finally {
      await db.schema.dropView("hero_items").execute();
    }
  });
});

describe("migrations", () => {
  xfailOnSereneDB("no pg_advisory_xact_lock, the migrator locks with it")(
    "migrate to latest and back down",
    async () => {
      const migrations = {
        "001_leagues": {
          up: (k) =>
            k.schema
              .createTable("leagues")
              .addColumn("id", "serial", (c) => c.primaryKey())
              .addColumn("name", "varchar(50)", (c) => c.notNull().unique())
              .execute(),
          down: (k) => k.schema.dropTable("leagues").execute(),
        },
        "002_seasons": {
          up: (k) =>
            k.schema
              .createTable("seasons")
              .addColumn("id", "serial", (c) => c.primaryKey())
              .addColumn("year", "integer", (c) => c.notNull())
              .execute(),
          down: (k) => k.schema.dropTable("seasons").execute(),
        },
      };
      const target = connect(MIGRATIONS);
      try {
        const migrator = new Migrator({
          db: target,
          provider: { getMigrations: async () => migrations },
        });
        const up = await migrator.migrateToLatest();
        expect(up.error).toBeUndefined();
        expect(up.results.map((r) => [r.migrationName, r.status])).toEqual([
          ["001_leagues", "Success"],
          ["002_seasons", "Success"],
        ]);
        const listed = await migrator.getMigrations();
        expect(listed.every((m) => m.executedAt instanceof Date)).toBe(true);
        const down = await migrator.migrateDown();
        expect(down.error).toBeUndefined();
        expect(down.results.map((r) => r.migrationName)).toEqual(["002_seasons"]);
      } finally {
        await target.destroy();
      }
    },
  );
});

describe("queries", () => {
  test("inserts and reads every column type", async () => {
    const hero = await db
      .insertInto("heroes")
      .values({
        name: "Pudge",
        title: "Butcher",
        role: "offlane",
        level: 25,
        gold: "9007199254740993",
        rating: 4.5,
        power: "12.50",
        active: false,
        born: "2004-02-01",
        portrait: Buffer.from([0, 1, 254, 255]),
        aliases: ["butcher", "hook"],
      })
      .returningAll()
      .executeTakeFirstOrThrow();
    expect(hero.id).toBeGreaterThan(0);
    expect(hero.uid).toMatch(/^[0-9a-f-]{36}$/);
    expect(hero.created_at).toBeInstanceOf(Date);
    const fresh = await db
      .selectFrom("heroes")
      .selectAll()
      .where("id", "=", hero.id)
      .executeTakeFirstOrThrow();
    expect(fresh).toEqual(hero);
    expect(fresh.gold).toBe("9007199254740993");
    expect(fresh.power).toBe("12.50");
    expect([...fresh.portrait]).toEqual([0, 1, 254, 255]);
    expect(fresh.aliases).toEqual(["butcher", "hook"]);
  });

  test("joins, aggregates, subqueries and views", async () => {
    const [lina, zeus] = await db
      .insertInto("heroes")
      .values([{ name: "Lina", role: "support" }, { name: "Zeus" }])
      .returning("id")
      .execute();
    await db
      .insertInto("items")
      .values([
        { name: "Blink", cost: 2250, hero_id: lina.id },
        { name: "Aghanim", cost: 4200, hero_id: lina.id },
        { name: "Kaya", cost: 2100, hero_id: zeus.id },
      ])
      .execute();
    const totals = await db
      .selectFrom("items")
      .innerJoin("heroes", "heroes.id", "items.hero_id")
      .select((eb) => [
        "heroes.name",
        eb.fn.sum("items.cost").as("total"),
        eb.fn.countAll().as("n"),
      ])
      .groupBy("heroes.name")
      .having((eb) => eb.fn.sum("items.cost"), ">", 1000)
      .orderBy("total", "desc")
      .execute();
    expect(totals.map((r) => [r.name, Number(r.total), Number(r.n)])).toEqual([
      ["Lina", 6450, 2],
      ["Zeus", 2100, 1],
    ]);
    const rich = await db
      .selectFrom("heroes")
      .select("name")
      .where("id", "in", (eb) =>
        eb.selectFrom("items").select("hero_id").where("cost", ">", 4000),
      )
      .execute();
    expect(rich).toEqual([{ name: "Lina" }]);
    await createHeroItemsView();
    try {
      const viewed = await db
        .selectFrom("hero_items")
        .select(["hero", "item"])
        .where((eb) => eb.between("cost", 2000, 2300))
        .orderBy("item")
        .execute();
      expect(viewed).toEqual([
        { hero: "Lina", item: "Blink" },
        { hero: "Zeus", item: "Kaya" },
      ]);
    } finally {
      await db.schema.dropView("hero_items").execute();
    }
    const filtered = await db
      .selectFrom("heroes")
      .select("name")
      .where((eb) =>
        eb.or([eb("name", "ilike", "li%"), eb(sql.val("thundergod"), "=", eb.fn.any("aliases"))]),
      )
      .execute();
    expect(filtered).toEqual([{ name: "Lina" }]);
    const page = await db
      .selectFrom("items")
      .select("name")
      .orderBy("cost")
      .limit(1)
      .offset(1)
      .execute();
    expect(page).toEqual([{ name: "Blink" }]);
  });

  xfailOnSereneDB("json_agg is missing, jsonArrayFrom nests rows with it")(
    "nested rows with jsonArrayFrom",
    async () => {
      const lina = await db
        .selectFrom("heroes")
        .select((eb) => [
          "name",
          jsonArrayFrom(
            eb
              .selectFrom("items")
              .select(["items.name", "items.cost"])
              .whereRef("items.hero_id", "=", "heroes.id")
              .orderBy("items.cost", "desc"),
          ).as("items"),
        ])
        .where("name", "=", "Lina")
        .executeTakeFirstOrThrow();
      expect(lina.items.map((i) => i.name)).toEqual(["Aghanim", "Blink"]);
    },
  );

  test("upserts, updates and deletes with returning", async () => {
    await db.insertInto("heroes").values([{ name: "Axe" }, { name: "Sven" }]).execute();
    await db
      .insertInto("heroes")
      .values({ name: "Axe" })
      .onConflict((oc) => oc.column("name").doNothing())
      .execute();
    const merged = await db
      .insertInto("heroes")
      .values({ name: "Axe", title: "Mogul Khan" })
      .onConflict((oc) =>
        oc.column("name").doUpdateSet((eb) => ({ title: eb.ref("excluded.title") })),
      )
      .returning("title")
      .executeTakeFirstOrThrow();
    expect(merged.title).toBe("Mogul Khan");
    const updated = await db
      .updateTable("heroes")
      .set({ level: 4 })
      .where("name", "in", ["Axe", "Sven"])
      .returning(["name", "level"])
      .execute();
    expect(updated.map((r) => r.level)).toEqual([4, 4]);
    const deleted = await db
      .deleteFrom("heroes")
      .where("name", "=", "Sven")
      .returning("name")
      .execute();
    expect(deleted).toEqual([{ name: "Sven" }]);
  });

  xfailOnSereneDB(
    "Parse types $n next to an operator (col + $1) as text and describes the RETURNING result as unknown",
  )("update with an arithmetic parameter returning rows", async () => {
    await db.insertInto("heroes").values({ name: "Tiny" }).execute();
    const grown = await db
      .updateTable("heroes")
      .set((eb) => ({ level: eb("level", "+", 3) }))
      .where("name", "=", "Tiny")
      .returning(["name", "level"])
      .executeTakeFirstOrThrow();
    expect(grown).toEqual({ name: "Tiny", level: 4 });
  });

  test("transactions commit and roll back", async () => {
    await db.transaction().execute(async (trx) => {
      await trx.insertInto("heroes").values({ name: "Kunkka" }).execute();
    });
    await expect(
      db.transaction().execute(async (trx) => {
        await trx.insertInto("heroes").values({ name: "Ghost" }).execute();
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    const names = await db
      .selectFrom("heroes")
      .select("name")
      .where("name", "in", ["Kunkka", "Ghost"])
      .execute();
    expect(names).toEqual([{ name: "Kunkka" }]);
    await db
      .transaction()
      .setIsolationLevel("repeatable read")
      .execute(async (trx) => {
        expect(
          await trx.selectFrom("heroes").select("name").where("name", "=", "Kunkka").execute(),
        ).toHaveLength(1);
      });
  });

  xfailOnSereneDB("SAVEPOINT (#722)")("savepoints", async () => {
    const trx = await db.startTransaction().execute();
    try {
      await trx.insertInto("heroes").values({ name: "Outer" }).execute();
      const savepoint = await trx.savepoint("inner").execute();
      await savepoint.insertInto("heroes").values({ name: "Inner" }).execute();
      await savepoint.rollbackToSavepoint("inner").execute();
      await trx.commit().execute();
    } finally {
      if (!trx.isCommitted) await trx.rollback().execute();
    }
    const names = await db
      .selectFrom("heroes")
      .select("name")
      .where("name", "in", ["Outer", "Inner"])
      .execute();
    expect(names).toEqual([{ name: "Outer" }]);
  });

  xfailOnSereneDB("SELECT ... FOR UPDATE/SHARE (#1431)")("row locks", async () => {
    await db.transaction().execute(async (trx) => {
      const rows = await trx
        .selectFrom("heroes")
        .selectAll()
        .where("name", "=", "Pudge")
        .forUpdate()
        .skipLocked()
        .execute();
      expect(rows).toHaveLength(1);
    });
  });
});
