import "reflect-metadata";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { DataSource, EntitySchema, Table, TableIndex } from "typeorm";
import {
  checkGolden,
  databaseConfig,
  databaseName,
  dropDatabase,
  recreateDatabase,
  xfailOnSereneDB,
} from "../target.js";

const FIXTURES = path.resolve(import.meta.dirname, "..", "fixtures", "typeorm");
const MAIN = databaseName("typeorm");
const FOREIGN_KEYS = databaseName("typeorm_fk");
const JSONB = databaseName("typeorm_jsonb");
const MIGRATIONS = databaseName("typeorm_migrations");
const UUID_OSSP = databaseName("typeorm_uuid_ossp");
const DATABASES = [MAIN, FOREIGN_KEYS, JSONB, MIGRATIONS, UUID_OSSP];
const TABLES = ["hero", "item", "guild", "guild_heroes_hero"];

function entities(foreignKeys) {
  const Hero = new EntitySchema({
    name: "Hero",
    tableName: "hero",
    columns: {
      id: { type: "int", primary: true, generated: "increment" },
      name: { type: "varchar", length: 100, unique: true },
      title: { type: "text", nullable: true },
      role: { type: "enum", enum: ["carry", "support", "offlane"], default: "carry" },
      level: { type: "smallint", default: 1 },
      gold: { type: "bigint", default: 0 },
      rating: { type: "double precision", nullable: true },
      power: { type: "numeric", precision: 10, scale: 2, default: 0 },
      active: { type: "boolean", default: true },
      born: { type: "date", nullable: true },
      portrait: { type: "bytea", nullable: true },
      aliases: { type: "text", array: true, nullable: true },
      tags: { type: "simple-array", nullable: true },
      uid: { type: "uuid", generated: "uuid" },
      createdAt: { type: "timestamp", createDate: true },
      updatedAt: { type: "timestamp", updateDate: true },
      version: { type: "int", version: true },
    },
    relations: {
      items: { type: "one-to-many", target: "Item", inverseSide: "hero" },
    },
  });
  const Item = new EntitySchema({
    name: "Item",
    tableName: "item",
    columns: {
      id: { type: "int", primary: true, generated: "increment" },
      name: { type: "varchar", length: 60 },
      cost: { type: "int", default: 0 },
    },
    relations: {
      hero: {
        type: "many-to-one",
        target: "Hero",
        inverseSide: "items",
        nullable: true,
        createForeignKeyConstraints: foreignKeys,
      },
    },
    indices: [{ name: "IDX_item_name_cost", columns: ["name", "cost"] }],
    uniques: [{ name: "UQ_item_hero_name", columns: ["hero", "name"] }],
    checks: [{ name: "CHK_item_cost", expression: `"cost" >= 0` }],
  });
  const Guild = new EntitySchema({
    name: "Guild",
    tableName: "guild",
    columns: {
      id: { type: "int", primary: true, generated: "increment" },
      name: { type: "varchar", length: 40, unique: true },
    },
    relations: {
      heroes: {
        type: "many-to-many",
        target: "Hero",
        joinTable: true,
        createForeignKeyConstraints: foreignKeys,
      },
    },
  });
  return [Hero, Item, Guild];
}

function dataSource(database, options) {
  const c = databaseConfig(database);
  return new DataSource({
    type: "postgres",
    host: c.host,
    port: c.port,
    username: c.user,
    database: c.database,
    logging: false,
    ...options,
  });
}

function describeTable(table) {
  return {
    name: table.name,
    columns: table.columns.map((c) => ({
      name: c.name,
      type: c.type,
      length: c.length,
      precision: c.precision,
      scale: c.scale,
      isNullable: c.isNullable,
      isGenerated: c.isGenerated,
      generationStrategy: c.generationStrategy,
      isPrimary: c.isPrimary,
      isUnique: c.isUnique,
      isArray: c.isArray,
      default: c.default,
      enum: c.enum,
      enumName: c.enumName,
      comment: c.comment,
    })),
    indices: table.indices
      .map((i) => ({
        name: i.name,
        columnNames: i.columnNames,
        isUnique: i.isUnique,
        where: i.where,
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    uniques: table.uniques
      .map((u) => ({ name: u.name, columnNames: u.columnNames }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    checks: table.checks
      .map((c) => ({ name: c.name, expression: c.expression }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    foreignKeys: table.foreignKeys
      .map((f) => ({
        name: f.name,
        columnNames: f.columnNames,
        referencedTableName: f.referencedTableName,
        referencedColumnNames: f.referencedColumnNames,
        onDelete: f.onDelete,
        onUpdate: f.onUpdate,
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
  };
}

let main;

beforeAll(async () => {
  for (const database of DATABASES) await recreateDatabase(database);
  main = dataSource(MAIN, { entities: entities(false), uuidExtension: "pgcrypto" });
  await main.initialize();
  await main.synchronize();
});

afterAll(async () => {
  if (main?.isInitialized) await main.destroy();
  for (const database of DATABASES) await dropDatabase(database);
});

describe("schema", () => {
  test("a synchronized schema has nothing left to change", async () => {
    const pending = await main.driver.createSchemaBuilder().log();
    checkGolden(
      path.join(FIXTURES, "schema-diff.json"),
      JSON.stringify(
        pending.upQueries.map((q) => q.query),
        null,
        1,
      ) + "\n",
    );
  });

  test("the query runner introspects every table", async () => {
    const runner = main.createQueryRunner();
    try {
      const tables = await runner.getTables(TABLES);
      checkGolden(
        path.join(FIXTURES, "tables.json"),
        JSON.stringify(
          tables
            .map(describeTable)
            .sort((a, b) => a.name.localeCompare(b.name)),
          null,
          1,
        ) + "\n",
      );
      expect(await runner.hasTable("hero")).toBe(true);
      expect(await runner.hasColumn("hero", "aliases")).toBe(true);
      expect(await runner.hasColumn("hero", "missing")).toBe(false);
      expect(await runner.getCurrentSchema()).toBe("public");
      expect(await runner.getCurrentDatabase()).toBe(MAIN);
    } finally {
      await runner.release();
    }
  });

  xfailOnSereneDB("ALTER TABLE ADD FOREIGN KEY (#1427)")(
    "synchronize creates foreign keys",
    async () => {
      const source = dataSource(FOREIGN_KEYS, {
        entities: entities(true),
        uuidExtension: "pgcrypto",
      });
      await source.initialize();
      try {
        await source.synchronize();
        const runner = source.createQueryRunner();
        const item = await runner.getTable("item");
        await runner.release();
        expect(item.foreignKeys.map((f) => f.referencedTableName)).toEqual([
          "hero",
        ]);
      } finally {
        await source.destroy();
      }
    },
  );

  xfailOnSereneDB(
    "CREATE EXTENSION is a syntax error and uuid-ossp's uuid_generate_v4 is missing",
  )("uuid columns default to uuid-ossp", async () => {
    const Ticket = new EntitySchema({
      name: "Ticket",
      columns: {
        id: { type: "uuid", primary: true, generated: "uuid" },
      },
    });
    const source = dataSource(UUID_OSSP, { entities: [Ticket] });
    await source.initialize();
    try {
      await source.synchronize();
      const saved = await source.getRepository("Ticket").save({});
      expect(saved.id).toMatch(/^[0-9a-f-]{36}$/);
    } finally {
      await source.destroy();
    }
  });

  xfailOnSereneDB("jsonb columns (#613)")("jsonb columns", async () => {
    const Event = new EntitySchema({
      name: "Event",
      columns: {
        id: { type: "int", primary: true, generated: "increment" },
        payload: { type: "jsonb" },
      },
    });
    const source = dataSource(JSONB, { entities: [Event] });
    await source.initialize();
    try {
      await source.synchronize();
      await source.getRepository("Event").save({ payload: { hero: "pudge" } });
      const rows = await source
        .getRepository("Event")
        .createQueryBuilder("e")
        .where(`e.payload ->> 'hero' = :hero`, { hero: "pudge" })
        .getMany();
      expect(rows).toHaveLength(1);
    } finally {
      await source.destroy();
    }
  });

  test("migrations run and revert through the query runner", async () => {
    class CreateLeague1700000000000 {
      name = "CreateLeague1700000000000";
      async up(runner) {
        await runner.createTable(
          new Table({
            name: "league",
            columns: [
              { name: "id", type: "int", isPrimary: true, isGenerated: true, generationStrategy: "increment" },
              { name: "name", type: "varchar", length: "50", isUnique: true },
              { name: "tier", type: "int", default: 1 },
            ],
          }),
        );
        await runner.createIndex(
          "league",
          new TableIndex({ name: "IDX_league_tier", columnNames: ["tier"] }),
        );
      }
      async down(runner) {
        await runner.dropIndex("league", "IDX_league_tier");
        await runner.dropTable("league");
      }
    }
    class RenameLeague1700000000001 {
      name = "RenameLeague1700000000001";
      async up(runner) {
        await runner.renameTable("league", "division");
      }
      async down(runner) {
        await runner.renameTable("division", "league");
      }
    }
    const source = dataSource(MIGRATIONS, {
      migrations: [CreateLeague1700000000000, RenameLeague1700000000001],
    });
    await source.initialize();
    try {
      expect(await source.showMigrations()).toBe(true);
      const applied = await source.runMigrations({ transaction: "each" });
      expect(applied.map((m) => m.name)).toEqual([
        "CreateLeague1700000000000",
        "RenameLeague1700000000001",
      ]);
      expect(await source.showMigrations()).toBe(false);
      const runner = source.createQueryRunner();
      expect(await runner.hasTable("division")).toBe(true);
      await source.undoLastMigration({ transaction: "each" });
      expect(await runner.hasTable("league")).toBe(true);
      await source.undoLastMigration({ transaction: "each" });
      expect(await runner.hasTable("league")).toBe(false);
      await runner.release();
    } finally {
      await source.destroy();
    }
  });
});

describe("orm", () => {
  test("saves and loads every column type", async () => {
    const heroes = main.getRepository("Hero");
    const saved = await heroes.save({
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
      tags: ["strength", "melee"],
    });
    expect(saved.id).toBeGreaterThan(0);
    expect(saved.uid).toMatch(/^[0-9a-f-]{36}$/);
    expect(saved.version).toBe(1);
    const fresh = await heroes.findOneByOrFail({ id: saved.id });
    expect(fresh.role).toBe("offlane");
    expect(fresh.level).toBe(25);
    expect(fresh.gold).toBe("9007199254740993");
    expect(fresh.rating).toBe(4.5);
    expect(fresh.power).toBe("12.50");
    expect(fresh.active).toBe(false);
    expect(fresh.born).toBe("2004-02-01");
    expect([...fresh.portrait]).toEqual([0, 1, 254, 255]);
    expect(fresh.aliases).toEqual(["butcher", "hook"]);
    expect(fresh.tags).toEqual(["strength", "melee"]);
    expect(fresh.createdAt).toBeInstanceOf(Date);
    fresh.level = 26;
    const updated = await heroes.save(fresh);
    expect(updated.version).toBe(2);
  });

  test("relations, query builder and find options", async () => {
    const heroes = main.getRepository("Hero");
    const items = main.getRepository("Item");
    const guilds = main.getRepository("Guild");
    const lina = await heroes.save({ name: "Lina", role: "support" });
    const zeus = await heroes.save({ name: "Zeus" });
    await items.save([
      { name: "Blink", cost: 2250, hero: lina },
      { name: "Aghanim", cost: 4200, hero: lina },
      { name: "Kaya", cost: 2100, hero: zeus },
    ]);
    await guilds.save({ name: "Radiant", heroes: [lina, zeus] });
    const withItems = await heroes.findOneOrFail({
      where: { name: "Lina" },
      relations: { items: true },
      order: { items: { cost: "DESC" } },
    });
    expect(withItems.items.map((i) => i.name)).toEqual(["Aghanim", "Blink"]);
    const guild = await guilds.findOneOrFail({
      where: { name: "Radiant" },
      relations: { heroes: true },
    });
    expect(guild.heroes.map((h) => h.name).sort()).toEqual(["Lina", "Zeus"]);
    const rows = await items
      .createQueryBuilder("i")
      .innerJoin("i.hero", "h")
      .select("h.name", "hero")
      .addSelect("SUM(i.cost)", "total")
      .addSelect("COUNT(*)", "items")
      .groupBy("h.name")
      .having("SUM(i.cost) > :min", { min: 1000 })
      .orderBy("total", "DESC")
      .getRawMany();
    expect(rows).toEqual([
      { hero: "Lina", total: "6450", items: "2" },
      { hero: "Zeus", total: "2100", items: "1" },
    ]);
    const [page, count] = await items.findAndCount({
      order: { cost: "ASC" },
      skip: 1,
      take: 1,
    });
    expect(count).toBe(3);
    expect(page.map((i) => i.name)).toEqual(["Blink"]);
    const subquery = await heroes
      .createQueryBuilder("h")
      .where((qb) => {
        const sub = qb
          .subQuery()
          .select("i.heroId")
          .from("item", "i")
          .where("i.cost > :cost")
          .getQuery();
        return `h.id IN ${sub}`;
      })
      .setParameter("cost", 4000)
      .getMany();
    expect(subquery.map((h) => h.name)).toEqual(["Lina"]);
  });

  test("insert, upsert, update and delete builders", async () => {
    const heroes = main.getRepository("Hero");
    await heroes
      .createQueryBuilder()
      .insert()
      .values([{ name: "Axe" }, { name: "Sven" }])
      .execute();
    await heroes.upsert([{ name: "Axe", title: "Mogul Khan" }], ["name"]);
    expect((await heroes.findOneByOrFail({ name: "Axe" })).title).toBe("Mogul Khan");
    const result = await heroes
      .createQueryBuilder()
      .update()
      .set({ gold: () => "gold + 100" })
      .where("name IN (:...names)", { names: ["Axe", "Sven"] })
      .execute();
    expect(result.affected).toBe(2);
    expect(await heroes.countBy({ gold: "100" })).toBe(2);
    expect(
      (await heroes.createQueryBuilder().delete().where("name = :n", { n: "Sven" }).execute())
        .affected,
    ).toBe(1);
    await heroes.increment({ name: "Axe" }, "level", 3);
    expect((await heroes.findOneByOrFail({ name: "Axe" })).level).toBe(4);
  });

  test("transactions commit and roll back", async () => {
    await main.transaction(async (manager) => {
      await manager.getRepository("Hero").save({ name: "Tiny" });
    });
    await expect(
      main.transaction(async (manager) => {
        await manager.getRepository("Hero").save({ name: "Ghost" });
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    const heroes = main.getRepository("Hero");
    expect(await heroes.countBy({ name: "Tiny" })).toBe(1);
    expect(await heroes.countBy({ name: "Ghost" })).toBe(0);
    await main.transaction("REPEATABLE READ", async (manager) => {
      expect(await manager.getRepository("Hero").countBy({ name: "Tiny" })).toBe(1);
    });
  });

  xfailOnSereneDB("SAVEPOINT (#722)")(
    "nested transactions use savepoints",
    async () => {
      const runner = main.createQueryRunner();
      await runner.startTransaction();
      try {
        await runner.manager.getRepository("Hero").save({ name: "Outer" });
        await runner.startTransaction();
        await runner.manager.getRepository("Hero").save({ name: "Inner" });
        await runner.rollbackTransaction();
        await runner.commitTransaction();
      } finally {
        if (runner.isTransactionActive) await runner.query("ROLLBACK");
        await runner.release();
      }
      const heroes = main.getRepository("Hero");
      expect(await heroes.countBy({ name: "Outer" })).toBe(1);
      expect(await heroes.countBy({ name: "Inner" })).toBe(0);
    },
  );

  xfailOnSereneDB("SELECT ... FOR UPDATE/SHARE (#1431)")(
    "pessimistic locks",
    async () => {
      await main.transaction(async (manager) => {
        const hero = await manager
          .getRepository("Hero")
          .createQueryBuilder("h")
          .setLock("pessimistic_write")
          .where("h.name = :name", { name: "Pudge" })
          .getOneOrFail();
        expect(hero.name).toBe("Pudge");
      });
    },
  );
});
