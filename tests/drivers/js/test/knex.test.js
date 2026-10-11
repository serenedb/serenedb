import path from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import knexFactory from "knex";
import {
  checkGolden,
  databaseConfig,
  databaseName,
  dropDatabase,
  recreateDatabase,
  stableJson,
  xfailOnSereneDB,
} from "../target.js";

const FIXTURES = path.resolve(import.meta.dirname, "..", "fixtures", "knex");
const MAIN = databaseName("knex");
const MIGRATIONS = databaseName("knex_migrations");
const FOREIGN_KEYS = databaseName("knex_fk");
const DATABASES = [MAIN, MIGRATIONS, FOREIGN_KEYS];

function connect(database) {
  const c = databaseConfig(database);
  return knexFactory({
    client: "pg",
    connection: { host: c.host, port: c.port, user: c.user, database: c.database },
    pool: { min: 0, max: 4 },
  });
}

function heroesTable(table) {
  table.increments("id");
  table.string("name", 100).notNullable().unique();
  table.text("title");
  table.enu("role", ["carry", "support", "offlane"], {
    useNative: true,
    enumName: "hero_role",
  }).defaultTo("carry");
  table.smallint("level").defaultTo(1);
  table.bigInteger("gold").defaultTo(0);
  table.double("rating");
  table.decimal("power", 10, 2).defaultTo(0);
  table.boolean("active").defaultTo(true);
  table.date("born");
  table.binary("portrait");
  table.specificType("aliases", "text[]");
  table.uuid("uid").defaultTo(knex.raw("gen_random_uuid()"));
  table.timestamp("created_at", { useTz: false }).defaultTo(knex.fn.now());
  table.check("?? >= 0", ["gold"], "heroes_gold_nonneg");
}

let knex;

beforeAll(async () => {
  for (const database of DATABASES) await recreateDatabase(database);
  knex = connect(MAIN);
  await knex.schema.createTable("heroes", heroesTable);
  await knex.schema.createTable("items", (table) => {
    table.increments("id");
    table.string("name", 60).notNullable();
    table.integer("cost").notNullable().defaultTo(0);
    table.integer("hero_id");
    table.index(["name", "cost"], "items_name_cost");
    table.unique(["hero_id", "name"], { indexName: "items_hero_name" });
  });
});

afterAll(async () => {
  if (knex) await knex.destroy();
  for (const database of DATABASES) await dropDatabase(database);
});

describe("schema", () => {
  test("columnInfo describes every column", async () => {
    const info = {
      heroes: await knex("heroes").columnInfo(),
      items: await knex("items").columnInfo(),
    };
    checkGolden(path.join(FIXTURES, "column-info.json"), stableJson(info));
    expect(await knex.schema.hasTable("heroes")).toBe(true);
    expect(await knex.schema.hasTable("villains")).toBe(false);
    expect(await knex.schema.hasColumn("heroes", "aliases")).toBe(true);
    expect(await knex.schema.hasColumn("heroes", "missing")).toBe(false);
    expect(await knex("heroes").columnInfo("gold")).toEqual(info.heroes.gold);
  });

  test("alters, renames and drops", async () => {
    await knex.schema.createTable("towers", (table) => {
      table.increments("id");
      table.string("lane", 10).notNullable();
      table.integer("tier").defaultTo(1);
    });
    await knex.schema.alterTable("towers", (table) => {
      table.integer("armor");
      table.index(["lane", "tier"], "towers_lane_tier");
    });
    await knex.schema.alterTable("towers", (table) => {
      table.renameColumn("armor", "plating");
    });
    await knex.schema.alterTable("towers", (table) => {
      table.dropIndex(["lane", "tier"], "towers_lane_tier");
    });
    await knex.schema.renameTable("towers", "buildings");
    expect(Object.keys(await knex("buildings").columnInfo()).sort()).toEqual([
      "id",
      "lane",
      "plating",
      "tier",
    ]);
    await knex.schema.dropTable("buildings");
    expect(await knex.schema.hasTable("buildings")).toBe(false);
    await knex.schema.createSchema("arena");
    await knex.schema.withSchema("arena").createTable("duels", (table) => {
      table.increments("id");
    });
    expect(await knex.schema.withSchema("arena").hasTable("duels")).toBe(true);
    await knex.schema.dropSchema("arena", true);
  });

  test("alter changes a column type, nullability and default", async () => {
    await knex.schema.createTable("wards", (table) => {
      table.increments("id");
      table.integer("charges");
    });
    await knex.schema.alterTable("wards", (table) => {
      table.bigInteger("charges").notNullable().defaultTo(2).alter();
    });
    const charges = await knex("wards").columnInfo("charges");
    checkGolden(path.join(FIXTURES, "altered-column.json"), stableJson(charges));
    expect(charges.type).toBe("bigint");
    expect(charges.nullable).toBe(false);
  });

  xfailOnSereneDB("ALTER TABLE ADD FOREIGN KEY (#1427)")(
    "foreign keys are added after the table",
    async () => {
      const fk = connect(FOREIGN_KEYS);
      try {
        await fk.schema.createTable("teams", (table) => {
          table.increments("id");
        });
        await fk.schema.createTable("players", (table) => {
          table.increments("id");
          table.integer("team_id").references("id").inTable("teams");
        });
      } finally {
        await fk.destroy();
      }
    },
  );

  test("migrations run, report status and roll back", async () => {
    const migrator = connect(MIGRATIONS);
    const migrations = {
      "001_leagues": {
        up: (k) =>
          k.schema.createTable("leagues", (table) => {
            table.increments("id");
            table.string("name", 50).notNullable().unique();
          }),
        down: (k) => k.schema.dropTable("leagues"),
      },
      "002_seasons": {
        up: (k) =>
          k.schema.createTable("seasons", (table) => {
            table.increments("id");
            table.integer("league_id").notNullable();
            table.integer("year").notNullable();
          }),
        down: (k) => k.schema.dropTable("seasons"),
      },
    };
    const config = {
      migrationSource: {
        getMigrations: async () => Object.keys(migrations),
        getMigrationName: (name) => name,
        getMigration: async (name) => migrations[name],
      },
    };
    try {
      const [batch, applied] = await migrator.migrate.latest(config);
      expect(batch).toBe(1);
      expect(applied).toEqual(["001_leagues", "002_seasons"]);
      expect(await migrator.migrate.currentVersion(config)).toBe("002");
      const [completed, pending] = await migrator.migrate.list(config);
      expect(completed.map((m) => m.name)).toEqual(["001_leagues", "002_seasons"]);
      expect(pending).toEqual([]);
      await migrator.migrate.down(config);
      expect(await migrator.schema.hasTable("seasons")).toBe(false);
      await migrator.migrate.rollback(config, true);
      expect(await migrator.schema.hasTable("leagues")).toBe(false);
      expect(await migrator("knex_migrations").count("* as n")).toEqual([{ n: "0" }]);
    } finally {
      await migrator.destroy();
    }
  });
});

describe("queries", () => {
  test("inserts and reads every column type", async () => {
    const [hero] = await knex("heroes")
      .insert({
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
      .returning(["id", "uid", "created_at"]);
    expect(hero.id).toBeGreaterThan(0);
    expect(hero.uid).toMatch(/^[0-9a-f-]{36}$/);
    expect(hero.created_at).toBeInstanceOf(Date);
    const fresh = await knex("heroes").where({ id: hero.id }).first();
    expect(fresh.role).toBe("offlane");
    expect(fresh.level).toBe(25);
    expect(fresh.gold).toBe("9007199254740993");
    expect(fresh.rating).toBe(4.5);
    expect(fresh.power).toBe("12.50");
    expect(fresh.active).toBe(false);
    expect(fresh.born).toBeInstanceOf(Date);
    expect([...fresh.portrait]).toEqual([0, 1, 254, 255]);
    expect(fresh.aliases).toEqual(["butcher", "hook"]);
    await expect(
      knex("heroes").insert({ name: "Debt", gold: -1 }),
    ).rejects.toThrow();
  });

  test("joins, aggregates, subqueries, unions and CTEs", async () => {
    const [lina] = await knex("heroes").insert({ name: "Lina", role: "support" }).returning("id");
    const [zeus] = await knex("heroes").insert({ name: "Zeus" }).returning("id");
    await knex("items").insert([
      { name: "Blink", cost: 2250, hero_id: lina.id },
      { name: "Aghanim", cost: 4200, hero_id: lina.id },
      { name: "Kaya", cost: 2100, hero_id: zeus.id },
    ]);
    const totals = await knex("items as i")
      .join("heroes as h", "h.id", "i.hero_id")
      .select("h.name")
      .sum({ total: "i.cost" })
      .count({ items: "*" })
      .groupBy("h.name")
      .having(knex.raw("sum(i.cost) > ?", [1000]))
      .orderBy("total", "desc");
    expect(totals.map((r) => [r.name, Number(r.total), Number(r.items)])).toEqual([
      ["Lina", 6450, 2],
      ["Zeus", 2100, 1],
    ]);
    expect(
      await knex("heroes")
        .whereIn("id", knex("items").select("hero_id").where("cost", ">", 4000))
        .pluck("name"),
    ).toEqual(["Lina"]);
    expect(
      await knex("items")
        .where("name", "like", "B%")
        .orWhereBetween("cost", [4000, 5000])
        .orderBy("name")
        .pluck("name"),
    ).toEqual(["Aghanim", "Blink"]);
    const union = await knex("items")
      .select("name")
      .where("cost", "<", 2200)
      .union((qb) => qb.select("name").from("heroes").where("name", "Zeus"))
      .orderBy("name");
    expect(union.map((r) => r.name)).toEqual(["Kaya", "Zeus"]);
    const cte = await knex
      .with("expensive", (qb) => qb.select("hero_id").from("items").where("cost", ">", 2200))
      .select("name")
      .from("heroes")
      .whereIn("id", knex.select("hero_id").from("expensive"))
      .orderBy("name");
    expect(cte.map((r) => r.name)).toEqual(["Lina"]);
    const page = await knex("items").orderBy("cost").offset(1).limit(1).pluck("name");
    expect(page).toEqual(["Blink"]);
    expect(
      await knex("heroes").whereRaw("? = ANY(aliases)", ["hook"]).pluck("name"),
    ).toEqual(["Pudge"]);
  });

  test("upserts, updates and deletes", async () => {
    await knex("heroes").insert([{ name: "Axe" }, { name: "Sven" }]);
    await knex("heroes").insert({ name: "Axe" }).onConflict("name").ignore();
    await knex("heroes")
      .insert({ name: "Axe", title: "Mogul Khan" })
      .onConflict("name")
      .merge(["title"]);
    expect((await knex("heroes").where({ name: "Axe" }).first()).title).toBe("Mogul Khan");
    expect(
      await knex("heroes").whereIn("name", ["Axe", "Sven"]).update({ gold: 100 }),
    ).toBe(2);
    expect(await knex("heroes").where({ name: "Sven" }).del()).toBe(1);
    const [updated] = await knex("heroes")
      .where({ name: "Axe" })
      .update({ title: "Axe" }, ["title"]);
    expect(updated.title).toBe("Axe");
  });

  test("increment and decrement", async () => {
    await knex("heroes").insert({ name: "Tiny" });
    await knex("heroes").where({ name: "Tiny" }).increment({ level: 3, gold: 5 });
    await knex("heroes").where({ name: "Tiny" }).decrement("level", 1);
    const tiny = await knex("heroes").where({ name: "Tiny" }).first();
    expect(tiny.level).toBe(3);
    expect(tiny.gold).toBe("5");
  });

  test("transactions commit and roll back", async () => {
    await knex.transaction(async (trx) => {
      await trx("heroes").insert({ name: "Kunkka" });
    });
    await expect(
      knex.transaction(async (trx) => {
        await trx("heroes").insert({ name: "Ghost" });
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(await knex("heroes").where({ name: "Kunkka" }).count("* as n")).toEqual([
      { n: "1" },
    ]);
    expect(await knex("heroes").where({ name: "Ghost" }).count("* as n")).toEqual([
      { n: "0" },
    ]);
    const trx = await knex.transaction({ isolationLevel: "repeatable read" });
    expect(await trx("heroes").where({ name: "Kunkka" }).pluck("name")).toEqual([
      "Kunkka",
    ]);
    await trx.commit();
  });

  xfailOnSereneDB("SAVEPOINT (#722)")("nested transactions", async () => {
    await knex.transaction(async (outer) => {
      await outer("heroes").insert({ name: "Outer" });
      await expect(
        outer.transaction(async (inner) => {
          await inner("heroes").insert({ name: "Inner" });
          throw new Error("inner");
        }),
      ).rejects.toThrow("inner");
    });
    expect(await knex("heroes").whereIn("name", ["Outer", "Inner"]).pluck("name")).toEqual([
      "Outer",
    ]);
  });

  xfailOnSereneDB("SELECT ... FOR UPDATE/SHARE (#1431)")("row locks", async () => {
    await knex.transaction(async (trx) => {
      const rows = await trx("heroes").where({ name: "Pudge" }).forUpdate().skipLocked();
      expect(rows).toHaveLength(1);
      await trx("heroes").where({ name: "Pudge" }).forShare();
    });
  });
});
