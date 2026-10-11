import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import {
  and,
  asc,
  between,
  count,
  desc,
  eq,
  gt,
  ilike,
  inArray,
  isNull,
  like,
  or,
  sql,
  sum,
} from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-postgres";
import { generateDrizzleJson, generateMigration } from "drizzle-kit/api";
import { schema } from "./drizzle-schema.js";
import {
  checkGolden,
  databaseConfig,
  databaseName,
  databaseUrl,
  dropDatabase,
  recreateDatabase,
  xfailOnSereneDB,
} from "../target.js";

const ROOT = path.resolve(import.meta.dirname, "..");
const FIXTURES = path.join(ROOT, "fixtures", "drizzle");
const WORK = path.join(ROOT, "generated", "drizzle");
const MAIN = databaseName("drizzle");
const FOREIGN_KEYS = databaseName("drizzle_fk");
const INDEXES = databaseName("drizzle_indexes");
const DATABASES = [MAIN, FOREIGN_KEYS, INDEXES];
const PLAIN = { foreignKeys: false, indexes: false };
const SELECT_FROM =
  "SELECT without a select list (EXISTS (SELECT FROM ...)) is rejected, drizzle-kit introspects columns with it";

const tables = schema(PLAIN);
let pool;
let db;

function run(command, args) {
  const result = spawnSync(command, args, { cwd: ROOT, encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(
      `${path.basename(command)} ${args.join(" ")} exited ${result.status}\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result.stdout;
}

function push(database, options, mode) {
  const output = path.join(WORK, `push-${database}.json`);
  fs.mkdirSync(WORK, { recursive: true });
  fs.rmSync(output, { force: true });
  run(process.execPath, [
    path.join(ROOT, "test", "drizzle-push.js"),
    databaseUrl(database),
    JSON.stringify(options),
    mode,
    output,
  ]);
  return JSON.parse(fs.readFileSync(output, "utf8"));
}

beforeAll(async () => {
  for (const database of DATABASES) await recreateDatabase(database);
  push(MAIN, PLAIN, "apply");
  pool = new pg.Pool(databaseConfig(MAIN));
  db = drizzle({ client: pool, schema: tables });
});

afterAll(async () => {
  if (pool) await pool.end();
  for (const database of DATABASES) await dropDatabase(database);
});

describe("drizzle-kit", () => {
  test("generates the migration for the schema", async () => {
    const statements = await generateMigration(
      generateDrizzleJson({}),
      generateDrizzleJson(schema({ foreignKeys: true, indexes: true })),
    );
    checkGolden(path.join(FIXTURES, "migration.sql"), `${statements.join(";\n")};\n`);
  });

  xfailOnSereneDB(SELECT_FROM)("push finds the database in sync", () => {
    checkGolden(
      path.join(FIXTURES, "push-diff.json"),
      `${JSON.stringify(push(MAIN, PLAIN, "diff"), null, 1)}\n`,
    );
  });

  xfailOnSereneDB(SELECT_FROM)("pull introspects the schema", () => {
    const out = path.join(WORK, "pull");
    fs.rmSync(out, { recursive: true, force: true });
    run(path.join(ROOT, "node_modules", ".bin", "drizzle-kit"), [
      "pull",
      "--dialect=postgresql",
      `--url=${databaseUrl(MAIN)}`,
      `--out=${out}`,
    ]);
    checkGolden(
      path.join(FIXTURES, "pull-schema.ts"),
      fs.readFileSync(path.join(out, "schema.ts"), "utf8"),
    );
    checkGolden(
      path.join(FIXTURES, "pull-relations.ts"),
      fs.readFileSync(path.join(out, "relations.ts"), "utf8"),
    );
  });

  xfailOnSereneDB("ALTER TABLE ADD FOREIGN KEY (#1427)")("push creates foreign keys", () => {
    push(FOREIGN_KEYS, { foreignKeys: true, indexes: false }, "apply");
  });

  xfailOnSereneDB("CREATE INDEX ... USING btree is rejected: Unknown index type: btree")(
    "push creates btree indexes",
    () => {
      push(INDEXES, { foreignKeys: false, indexes: true }, "apply");
    },
  );
});

describe("orm", () => {
  test("inserts and reads every column type", async () => {
    const { heroes } = tables;
    const [hero] = await db
      .insert(heroes)
      .values({
        name: "Pudge",
        title: "Butcher",
        role: "offlane",
        level: 25,
        gold: 9007199254740993n,
        rating: 4.5,
        power: "12.50",
        active: false,
        born: "2004-02-01",
        aliases: ["butcher", "hook"],
      })
      .returning();
    expect(hero.id).toBeGreaterThan(0);
    expect(hero.uid).toMatch(/^[0-9a-f-]{36}$/);
    expect(hero.createdAt).toBeInstanceOf(Date);
    const [fresh] = await db.select().from(heroes).where(eq(heroes.id, hero.id));
    expect(fresh).toEqual(hero);
    expect(fresh.gold).toBe(9007199254740993n);
    expect(fresh.power).toBe("12.50");
    expect(fresh.born).toBe("2004-02-01");
    expect(fresh.aliases).toEqual(["butcher", "hook"]);
  });

  test("joins, filters, aggregates and pagination", async () => {
    const { heroes, items } = tables;
    const [lina, zeus] = await db
      .insert(heroes)
      .values([{ name: "Lina", role: "support" }, { name: "Zeus" }])
      .returning({ id: heroes.id });
    await db.insert(items).values([
      { name: "Blink", cost: 2250, heroId: lina.id },
      { name: "Aghanim", cost: 4200, heroId: lina.id },
      { name: "Kaya", cost: 2100, heroId: zeus.id },
    ]);
    const totals = await db
      .select({ name: heroes.name, total: sum(items.cost), n: count() })
      .from(items)
      .innerJoin(heroes, eq(heroes.id, items.heroId))
      .groupBy(heroes.name)
      .having(gt(sum(items.cost), 1000))
      .orderBy(desc(sum(items.cost)));
    expect(totals).toEqual([
      { name: "Lina", total: "6450", n: 2 },
      { name: "Zeus", total: "2100", n: 1 },
    ]);
    const filtered = await db
      .select({ name: items.name })
      .from(items)
      .where(
        or(
          and(like(items.name, "B%"), between(items.cost, 2000, 3000)),
          inArray(items.name, ["Kaya"]),
        ),
      )
      .orderBy(asc(items.name));
    expect(filtered.map((r) => r.name)).toEqual(["Blink", "Kaya"]);
    expect(
      await db.select({ name: heroes.name }).from(heroes).where(ilike(heroes.name, "LI%")),
    ).toEqual([{ name: "Lina" }]);
    expect(
      (await db.select().from(heroes).where(isNull(heroes.title))).length,
    ).toBeGreaterThan(0);
    const page = await db
      .select({ name: items.name })
      .from(items)
      .orderBy(asc(items.cost))
      .limit(1)
      .offset(1);
    expect(page).toEqual([{ name: "Blink" }]);
    const counted = await db.$count(items, gt(items.cost, 2200));
    expect(counted).toBe(2);
  });

  xfailOnSereneDB("json_agg is missing, relational queries aggregate rows with it")(
    "relational queries",
    async () => {
      const lina = await db.query.heroes.findFirst({
        where: (h, { eq: equals }) => equals(h.name, "Lina"),
        with: { items: { orderBy: (i, { desc: down }) => [down(i.cost)] } },
      });
      expect(lina.items.map((i) => i.name)).toEqual(["Aghanim", "Blink"]);
      const kaya = await db.query.items.findFirst({
        where: (i, { eq: equals }) => equals(i.name, "Kaya"),
        with: { hero: { columns: { name: true } } },
      });
      expect(kaya.hero).toEqual({ name: "Zeus" });
    },
  );

  test("upserts, updates and deletes with returning", async () => {
    const { heroes } = tables;
    await db.insert(heroes).values([{ name: "Axe" }, { name: "Sven" }]);
    await db.insert(heroes).values({ name: "Axe" }).onConflictDoNothing();
    const [merged] = await db
      .insert(heroes)
      .values({ name: "Axe", title: "Mogul Khan" })
      .onConflictDoUpdate({ target: heroes.name, set: { title: "Mogul Khan" } })
      .returning({ title: heroes.title });
    expect(merged.title).toBe("Mogul Khan");
    const updated = await db
      .update(heroes)
      .set({ level: sql`${heroes.level} + 3` })
      .where(inArray(heroes.name, ["Axe", "Sven"]))
      .returning({ name: heroes.name, level: heroes.level });
    expect(updated.map((r) => r.level)).toEqual([4, 4]);
    const deleted = await db
      .delete(heroes)
      .where(eq(heroes.name, "Sven"))
      .returning({ name: heroes.name });
    expect(deleted).toEqual([{ name: "Sven" }]);
  });

  test("prepared statements and raw sql", async () => {
    const { heroes } = tables;
    const byName = db
      .select({ id: heroes.id, name: heroes.name })
      .from(heroes)
      .where(eq(heroes.name, sql.placeholder("name")))
      .prepare("hero_by_name");
    expect((await byName.execute({ name: "Pudge" }))[0].name).toBe("Pudge");
    expect((await byName.execute({ name: "Lina" }))[0].name).toBe("Lina");
    const result = await db.execute(sql`SELECT ${2}::int + ${3}::int AS sum`);
    expect(result.rows).toEqual([{ sum: 5 }]);
  });

  test("transactions commit and roll back", async () => {
    const { heroes } = tables;
    await db.transaction(async (tx) => {
      await tx.insert(heroes).values({ name: "Kunkka" });
    });
    await expect(
      db.transaction(async (tx) => {
        await tx.insert(heroes).values({ name: "Ghost" });
        tx.rollback();
      }),
    ).rejects.toThrow();
    expect(await db.$count(heroes, eq(heroes.name, "Kunkka"))).toBe(1);
    expect(await db.$count(heroes, eq(heroes.name, "Ghost"))).toBe(0);
    await db.transaction(
      async (tx) => {
        expect(await tx.$count(heroes, eq(heroes.name, "Kunkka"))).toBe(1);
      },
      { isolationLevel: "repeatable read", accessMode: "read only" },
    );
  });

  xfailOnSereneDB("SAVEPOINT (#722)")("nested transactions", async () => {
    const { heroes } = tables;
    await db.transaction(async (tx) => {
      await tx.insert(heroes).values({ name: "Outer" });
      await expect(
        tx.transaction(async (inner) => {
          await inner.insert(heroes).values({ name: "Inner" });
          inner.rollback();
        }),
      ).rejects.toThrow();
    });
    expect(await db.$count(heroes, inArray(heroes.name, ["Outer", "Inner"]))).toBe(1);
  });

  xfailOnSereneDB("SELECT ... FOR UPDATE/SHARE (#1431)")("row locks", async () => {
    const { heroes } = tables;
    await db.transaction(async (tx) => {
      const rows = await tx
        .select()
        .from(heroes)
        .where(eq(heroes.name, "Pudge"))
        .for("update", { skipLocked: true });
      expect(rows).toHaveLength(1);
    });
  });
});
