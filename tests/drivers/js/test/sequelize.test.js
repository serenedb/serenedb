import path from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { DataTypes, Op, Sequelize, Transaction } from "sequelize";
import {
  checkGolden,
  databaseConfig,
  databaseName,
  dropDatabase,
  recreateDatabase,
  stableJson,
  xfailOnSereneDB,
} from "../target.js";

const FIXTURES = path.resolve(import.meta.dirname, "..", "fixtures", "sequelize");
const MAIN = databaseName("sequelize");
const CASCADE = databaseName("sequelize_cascade");
const DDL = databaseName("sequelize_ddl");
const ENUMS = databaseName("sequelize_enums");
const DATABASES = [MAIN, CASCADE, DDL, ENUMS];
const TABLES = ["heroes", "items", "guilds", "hero_guilds", "runes"];
const NO_ACTION = { onDelete: "NO ACTION", onUpdate: "NO ACTION" };
const UTC = "Etc/UTC";

function connect(database, timezone) {
  const c = databaseConfig(database);
  return new Sequelize(c.database, c.user, null, {
    host: c.host,
    port: c.port,
    dialect: "postgres",
    logging: false,
    timezone,
  });
}

function define(sequelize, references) {
  const Hero = sequelize.define(
    "Hero",
    {
      name: { type: DataTypes.STRING(100), allowNull: false, unique: true },
      title: DataTypes.TEXT,
      role: { type: DataTypes.ENUM("carry", "support", "offlane"), defaultValue: "carry" },
      level: { type: DataTypes.SMALLINT, defaultValue: 1 },
      gold: { type: DataTypes.BIGINT, defaultValue: 0 },
      rating: DataTypes.DOUBLE,
      power: { type: DataTypes.DECIMAL(10, 2), defaultValue: 0 },
      active: { type: DataTypes.BOOLEAN, defaultValue: true },
      born: DataTypes.DATEONLY,
      portrait: DataTypes.BLOB,
      aliases: DataTypes.ARRAY(DataTypes.TEXT),
      uid: { type: DataTypes.UUID, defaultValue: DataTypes.UUIDV4, unique: true },
    },
    { tableName: "heroes", timestamps: false, version: true },
  );
  const Item = sequelize.define(
    "Item",
    {
      name: { type: DataTypes.STRING(60), allowNull: false },
      cost: { type: DataTypes.INTEGER, defaultValue: 0, validate: { min: 0 } },
    },
    {
      tableName: "items",
      underscored: true,
      timestamps: false,
      indexes: [
        { name: "items_name_cost", fields: ["name", "cost"] },
        { name: "items_expensive", fields: ["cost"], where: { cost: { [Op.gt]: 3000 } } },
      ],
    },
  );
  const Guild = sequelize.define(
    "Guild",
    { name: { type: DataTypes.STRING(40), unique: true } },
    { tableName: "guilds", timestamps: false },
  );
  const Rune = sequelize.define(
    "Rune",
    { kind: { type: DataTypes.STRING(20), allowNull: false } },
    { tableName: "runes", paranoid: true },
  );
  Hero.hasMany(Item, { foreignKey: "hero_id", ...references });
  Item.belongsTo(Hero, { foreignKey: "hero_id", ...references });
  Hero.belongsToMany(Guild, { through: "hero_guilds", timestamps: false, ...references });
  Guild.belongsToMany(Hero, { through: "hero_guilds", timestamps: false, ...references });
  return { Hero, Item, Guild, Rune };
}

async function createRoleEnum(target) {
  await target.query(
    `CREATE TYPE "public"."enum_heroes_role" AS ENUM ('carry', 'support', 'offlane')`,
  );
}

let sequelize;
let models;

beforeAll(async () => {
  for (const database of DATABASES) await recreateDatabase(database);
  sequelize = connect(MAIN, UTC);
  models = define(sequelize, NO_ACTION);
  await createRoleEnum(sequelize);
  await sequelize.sync();
});

afterAll(async () => {
  if (sequelize) await sequelize.close();
  for (const database of DATABASES) await dropDatabase(database);
});

describe("query interface", () => {
  xfailOnSereneDB("INTERVAL '+00:00' HOUR TO MINUTE is rejected (#1434)")(
    "connects with the default offset time zone",
    async () => {
      const offset = connect(MAIN, "+00:00");
      try {
        await offset.authenticate();
      } finally {
        await offset.close();
      }
    },
  );

  test("describes tables, indexes and foreign keys", async () => {
    const qi = sequelize.getQueryInterface();
    const tables = (await qi.showAllTables()).sort();
    expect(tables).toEqual([...TABLES].sort());
    const described = {};
    for (const table of [...TABLES].sort()) {
      const indexes = await qi.showIndex(table);
      described[table] = {
        columns: await qi.describeTable(table),
        indexes: indexes
          .map((i) => ({
            name: i.name,
            primary: i.primary,
            unique: i.unique,
            indkey: i.indkey,
            fields: i.fields.map((f) => [f.attribute, f.order, f.collate, f.length]),
            definition: i.definition,
          }))
          .sort((a, b) => a.name.localeCompare(b.name)),
        references: (await qi.getForeignKeyReferencesForTable(table))
          .map((r) => ({
            constraintName: r.constraintName,
            columnName: r.columnName,
            referencedTableName: r.referencedTableName,
            referencedColumnName: r.referencedColumnName,
          }))
          .sort((a, b) => a.constraintName.localeCompare(b.constraintName)),
      };
    }
    checkGolden(
      path.join(FIXTURES, "tables.json"),
      stableJson(described).replaceAll(MAIN, "<database>"),
    );
    expect(await qi.tableExists("heroes")).toBe(true);
    expect(await qi.tableExists("villains")).toBe(false);
  });

  test("lists schemas and enums", async () => {
    const qi = sequelize.getQueryInterface();
    expect(await qi.showAllSchemas()).toEqual([]);
    const [enums] = await sequelize.query(
      sequelize.getQueryInterface().queryGenerator.pgListEnums("heroes"),
    );
    expect(enums.map((e) => [e.enum_name, e.enum_value])).toEqual([
      ["enum_heroes_role", "{carry,support,offlane}"],
    ]);
  });

  test("table DDL through the query interface", async () => {
    const ddl = connect(DDL, UTC);
    const qi = ddl.getQueryInterface();
    try {
      await qi.createTable("towers", {
        id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
        lane: { type: DataTypes.STRING(10), allowNull: false },
        tier: { type: DataTypes.INTEGER, defaultValue: 1 },
      });
      await qi.addColumn("towers", "armor", { type: DataTypes.INTEGER, allowNull: true });
      await qi.renameColumn("towers", "armor", "plating");
      await qi.addIndex("towers", ["lane", "tier"], { name: "towers_lane_tier" });
      await qi.addConstraint("towers", {
        type: "check",
        fields: ["tier"],
        where: { tier: { [Op.between]: [1, 4] } },
        name: "towers_tier_range",
      });
      await qi.removeConstraint("towers", "towers_tier_range");
      await qi.removeIndex("towers", "towers_lane_tier");
      await qi.renameTable("towers", "buildings");
      const columns = await qi.describeTable("buildings");
      expect(Object.keys(columns).sort()).toEqual(["id", "lane", "plating", "tier"]);
      await qi.dropTable("buildings");
      expect(await qi.tableExists("buildings")).toBe(false);
    } finally {
      await ddl.close();
    }
  });

  xfailOnSereneDB("DO blocks are a syntax error, sync({ alter }) re-creates enum types in one")(
    "sync with alter reconciles the schema in place",
    async () => {
      await sequelize.sync({ alter: true });
    },
  );

  xfailOnSereneDB("foreign keys reject ON DELETE SET NULL / ON UPDATE CASCADE")(
    "associations create foreign keys with Sequelize's default actions",
    async () => {
      const cascade = connect(CASCADE, UTC);
      try {
        define(cascade, {});
        await createRoleEnum(cascade);
        await cascade.sync();
      } finally {
        await cascade.close();
      }
    },
  );

  xfailOnSereneDB("DO blocks are a syntax error, sync creates enum types in one")(
    "sync creates enum types",
    async () => {
      const fresh = connect(ENUMS, UTC);
      try {
        const Ward = fresh.define(
          "Ward",
          { kind: DataTypes.ENUM("observer", "sentry") },
          { tableName: "wards", timestamps: false },
        );
        await fresh.sync();
        await Ward.create({ kind: "sentry" });
        expect(await Ward.count({ where: { kind: "sentry" } })).toBe(1);
      } finally {
        await fresh.close();
      }
    },
  );
});

describe("models", () => {
  test("creates and reads every attribute type", async () => {
    const { Hero } = models;
    const hero = await Hero.create({
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
    });
    expect(hero.id).toBeGreaterThan(0);
    expect(hero.version).toBe(0);
    const fresh = await Hero.findByPk(hero.id);
    expect(fresh.role).toBe("offlane");
    expect(fresh.level).toBe(25);
    expect(fresh.gold).toBe("9007199254740993");
    expect(fresh.rating).toBe(4.5);
    expect(fresh.power).toBe("12.50");
    expect(fresh.active).toBe(false);
    expect(fresh.born).toBe("2004-02-01");
    expect([...fresh.portrait]).toEqual([0, 1, 254, 255]);
    expect(fresh.aliases).toEqual(["butcher", "hook"]);
    expect(fresh.uid).toBe(hero.uid);
    fresh.level = 26;
    await fresh.save();
    expect(fresh.version).toBe(1);
  });

  test("associations, eager loading and operators", async () => {
    const { Hero, Item, Guild } = models;
    const lina = await Hero.create({ name: "Lina", role: "support" });
    const zeus = await Hero.create({ name: "Zeus", aliases: ["thundergod"] });
    await Item.bulkCreate([
      { name: "Blink", cost: 2250, hero_id: lina.id },
      { name: "Aghanim", cost: 4200, hero_id: lina.id },
      { name: "Kaya", cost: 2100, hero_id: zeus.id },
    ]);
    const radiant = await Guild.create({ name: "Radiant" });
    await radiant.addHeros([lina, zeus]);
    const loaded = await Hero.findOne({
      where: { name: "Lina" },
      include: [{ model: Item }, { model: Guild }],
      order: [[Item, "cost", "DESC"]],
    });
    expect(loaded.Items.map((i) => i.name)).toEqual(["Aghanim", "Blink"]);
    expect(loaded.Guilds.map((g) => g.name)).toEqual(["Radiant"]);
    expect(await radiant.countHeros()).toBe(2);
    expect(
      await Item.count({ where: { name: { [Op.iLike]: "b%" }, cost: { [Op.gte]: 2000 } } }),
    ).toBe(1);
    expect(
      await Item.count({ where: { cost: { [Op.between]: [2000, 2300] } } }),
    ).toBe(2);
    expect(
      await Item.count({
        where: { [Op.or]: [{ name: "Kaya" }, { cost: { [Op.gt]: 4000 } }] },
      }),
    ).toBe(2);
    expect(await Hero.count({ where: { aliases: { [Op.contains]: ["thundergod"] } } })).toBe(1);
    expect(await Hero.count({ where: { aliases: { [Op.overlap]: ["x", "thundergod"] } } })).toBe(1);
    const totals = await Item.findAll({
      attributes: ["hero_id", [sequelize.fn("SUM", sequelize.col("cost")), "total"]],
      group: ["hero_id"],
      having: sequelize.where(sequelize.fn("SUM", sequelize.col("cost")), Op.gt, 3000),
      raw: true,
    });
    expect(totals).toEqual([{ hero_id: lina.id, total: "6450" }]);
    expect(await Item.sum("cost")).toBe(8550);
    expect(await Item.max("cost")).toBe(4200);
    const page = await Item.findAndCountAll({ order: [["cost", "ASC"]], offset: 1, limit: 1 });
    expect(page.count).toBe(3);
    expect(page.rows.map((i) => i.name)).toEqual(["Blink"]);
  });

  test("bulk upserts, updates, increments and deletes", async () => {
    const { Hero } = models;
    await Hero.bulkCreate([{ name: "Axe" }, { name: "Sven" }], { returning: true });
    await Hero.bulkCreate([{ name: "Axe" }], { ignoreDuplicates: true });
    await Hero.bulkCreate([{ name: "Axe", title: "Mogul Khan" }], {
      updateOnDuplicate: ["title"],
      conflictAttributes: ["name"],
    });
    expect((await Hero.findOne({ where: { name: "Axe" } })).title).toBe("Mogul Khan");
    const [affected] = await Hero.update(
      { gold: 100 },
      { where: { name: { [Op.in]: ["Axe", "Sven"] } } },
    );
    expect(affected).toBe(2);
    await Hero.increment({ level: 3 }, { where: { name: "Axe" } });
    expect((await Hero.findOne({ where: { name: "Axe" } })).level).toBe(4);
    const [upserted] = await Hero.upsert({ name: "Sven", title: "Rogue Knight" }, { conflictFields: ["name"] });
    expect(upserted.title).toBe("Rogue Knight");
    expect(await Hero.destroy({ where: { name: "Sven" } })).toBe(1);
    expect(await Hero.count({ where: { name: "Sven" } })).toBe(0);
  });

  xfailOnSereneDB(
    "timestamptz input with a space before the offset ('2026-10-10 09:55:01.123 +00:00') is rejected",
  )("timestamps and paranoid deletes", async () => {
    const { Rune } = models;
    const rune = await Rune.create({ kind: "haste" });
    expect(rune.createdAt).toBeInstanceOf(Date);
    const fresh = await Rune.findByPk(rune.id);
    expect(fresh.updatedAt.getTime()).toBe(rune.updatedAt.getTime());
    await rune.destroy();
    expect(await Rune.count()).toBe(0);
    expect(await Rune.count({ paranoid: false })).toBe(1);
    await Rune.restore({ where: { id: rune.id } });
    expect(await Rune.count({ where: { createdAt: { [Op.lte]: new Date() } } })).toBe(1);
  });

  xfailOnSereneDB("findOrCreate wraps its insert in a PL/pgSQL pg_temp function")(
    "findOrCreate",
    async () => {
      const { Hero } = models;
      const [hero, created] = await Hero.findOrCreate({ where: { name: "Tiny" } });
      expect(created).toBe(true);
      const [again, createdAgain] = await Hero.findOrCreate({ where: { name: "Tiny" } });
      expect(again.id).toBe(hero.id);
      expect(createdAgain).toBe(false);
    },
  );

  test("managed transactions commit and roll back", async () => {
    const { Hero } = models;
    await sequelize.transaction(async (transaction) => {
      await Hero.create({ name: "Kunkka" }, { transaction });
    });
    await expect(
      sequelize.transaction(async (transaction) => {
        await Hero.create({ name: "Ghost" }, { transaction });
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(await Hero.count({ where: { name: "Kunkka" } })).toBe(1);
    expect(await Hero.count({ where: { name: "Ghost" } })).toBe(0);
    await sequelize.transaction(
      { isolationLevel: Transaction.ISOLATION_LEVELS.REPEATABLE_READ },
      async (transaction) => {
        expect(await Hero.count({ where: { name: "Kunkka" }, transaction })).toBe(1);
      },
    );
  });

  xfailOnSereneDB("SERIALIZABLE isolation is rejected")(
    "serializable transactions",
    async () => {
      await sequelize.transaction(
        { isolationLevel: Transaction.ISOLATION_LEVELS.SERIALIZABLE },
        async (transaction) => {
          await models.Hero.count({ transaction });
        },
      );
    },
  );

  xfailOnSereneDB("SAVEPOINT (#722)")("nested transactions", async () => {
    const { Hero } = models;
    await sequelize.transaction(async (outer) => {
      await Hero.create({ name: "Outer" }, { transaction: outer });
      await expect(
        sequelize.transaction({ transaction: outer }, async (inner) => {
          await Hero.create({ name: "Inner" }, { transaction: inner });
          throw new Error("inner");
        }),
      ).rejects.toThrow("inner");
    });
    expect(await Hero.count({ where: { name: "Outer" } })).toBe(1);
    expect(await Hero.count({ where: { name: "Inner" } })).toBe(0);
  });

  xfailOnSereneDB("SELECT ... FOR UPDATE/SHARE (#1431)")("row locks", async () => {
    await sequelize.transaction(async (transaction) => {
      const hero = await models.Hero.findOne({
        where: { name: "Pudge" },
        lock: transaction.LOCK.UPDATE,
        transaction,
      });
      expect(hero.name).toBe("Pudge");
    });
  });

  test("raw queries with replacements and bind parameters", async () => {
    const [byName] = await sequelize.query(
      "SELECT name FROM heroes WHERE name = :name",
      { replacements: { name: "Pudge" } },
    );
    expect(byName).toEqual([{ name: "Pudge" }]);
    const [bound] = await sequelize.query(
      "SELECT $1::int + $2::int AS sum, $3::text AS label",
      { bind: [2, 3, "hook"] },
    );
    expect(bound).toEqual([{ sum: 5, label: "hook" }]);
  });
});
