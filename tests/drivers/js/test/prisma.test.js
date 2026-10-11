import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
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
const FIXTURES = path.join(ROOT, "fixtures", "prisma");
const WORK = path.join(ROOT, "generated", "prisma");
const MAIN = databaseName("prisma");
const DEPLOY = databaseName("prisma_deploy");
const LOCKED = databaseName("prisma_locked");
const FOREIGN_KEYS = databaseName("prisma_fk");
const JSON_DB = databaseName("prisma_json");
const PRECISION = databaseName("prisma_precision");
const DATABASES = [MAIN, DEPLOY, LOCKED, FOREIGN_KEYS, JSON_DB, PRECISION];
const NO_LOCK = { PRISMA_SCHEMA_DISABLE_ADVISORY_LOCK: "1" };
const DESCRIBE_ANY =
  "Describe types the parameter of = ANY($1) as text, the schema engine binds a list";
const PARAMETER_TYPES =
  "Parse types $n next to an operator (col + $1, arr || $1, LIKE ($1 || '%') AND col >= $2) as text and describes the result as unknown";
const RETURNING_NAMES =
  "RETURNING names x::text CAST(x AS VARCHAR), Prisma reads enum fields as x::text";

const VARIANTS = {
  "foreign-keys": { models: "models.prisma", datasource: "" },
  "relation-mode-prisma": {
    models: "models.prisma",
    datasource: '  relationMode = "prisma"\n',
  },
  json: { models: "json.prisma", datasource: "" },
  precision: { models: "precision.prisma", datasource: "" },
};

const migrations = {};
let prisma;

function variantPath(name, file) {
  return path.join(WORK, name, file);
}

function writeVariant(name) {
  const variant = VARIANTS[name];
  fs.rmSync(path.join(WORK, name), { recursive: true, force: true });
  fs.mkdirSync(path.join(WORK, name), { recursive: true });
  fs.copyFileSync(
    path.join(FIXTURES, "prisma.config.mjs"),
    variantPath(name, "prisma.config.mjs"),
  );
  fs.writeFileSync(
    variantPath(name, "schema.prisma"),
    'generator client {\n  provider = "prisma-client"\n  output   = "client"\n}\n\n' +
      `datasource db {\n  provider = "postgresql"\n${variant.datasource}}\n\n` +
      fs.readFileSync(path.join(FIXTURES, variant.models), "utf8"),
  );
}

function prismaRun(name, database, args, env) {
  return spawnSync(
    path.join(ROOT, "node_modules", ".bin", "prisma"),
    [...args, "--config", variantPath(name, "prisma.config.mjs")],
    {
      cwd: ROOT,
      encoding: "utf8",
      env: {
        ...process.env,
        ...env,
        DATABASE_URL: databaseUrl(database),
        CHECKPOINT_DISABLE: "1",
        PRISMA_HIDE_UPDATE_MESSAGE: "1",
      },
    },
  );
}

function prismaCli(name, database, args, env) {
  const result = prismaRun(name, database, args, env);
  if (result.status !== 0) {
    throw new Error(
      `prisma ${args.join(" ")} exited ${result.status}\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result.stdout;
}

function renderMigration(name) {
  const sql = prismaCli(
    name,
    MAIN,
    [
      "migrate",
      "diff",
      "--from-empty",
      "--to-schema",
      variantPath(name, "schema.prisma"),
      "--script",
    ],
    {},
  );
  fs.mkdirSync(variantPath(name, "migrations/0_init"), { recursive: true });
  fs.writeFileSync(variantPath(name, "migrations/0_init/migration.sql"), sql);
  fs.writeFileSync(
    variantPath(name, "migrations/migration_lock.toml"),
    'provider = "postgresql"\n',
  );
  return sql;
}

async function execute(database, sql) {
  const client = new pg.Client(databaseConfig(database));
  await client.connect();
  try {
    await client.query(sql);
  } finally {
    await client.end();
  }
}

beforeAll(async () => {
  for (const name of Object.keys(VARIANTS)) {
    writeVariant(name);
    migrations[name] = renderMigration(name);
  }
  for (const database of DATABASES) await recreateDatabase(database);
  await execute(MAIN, migrations["relation-mode-prisma"]);
  prismaCli("relation-mode-prisma", MAIN, ["generate"], {});
  const { PrismaClient } = await import(
    variantPath("relation-mode-prisma", "client/client.ts")
  );
  const { PrismaPg } = await import("@prisma/adapter-pg");
  prisma = new PrismaClient({
    adapter: new PrismaPg({ connectionString: databaseUrl(MAIN) }),
  });
}, 300_000);

afterAll(async () => {
  if (prisma) await prisma.$disconnect();
  for (const database of DATABASES) await dropDatabase(database);
}, 120_000);

describe("schema engine", () => {
  test("migrate diff renders every schema as the committed migration", () => {
    for (const name of Object.keys(VARIANTS)) {
      checkGolden(path.join(FIXTURES, `${name}.migration.sql`), migrations[name]);
    }
  });

  xfailOnSereneDB(DESCRIBE_ANY)(
    "migrate deploy applies the migration and leaves no drift",
    () => {
      const deployed = prismaCli(
        "relation-mode-prisma",
        DEPLOY,
        ["migrate", "deploy"],
        NO_LOCK,
      );
      expect(deployed).toContain("All migrations have been successfully applied");
      expect(
        prismaCli("relation-mode-prisma", DEPLOY, ["migrate", "status"], NO_LOCK),
      ).toContain("Database schema is up to date");
      const drift = prismaRun(
        "relation-mode-prisma",
        DEPLOY,
        [
          "migrate",
          "diff",
          "--from-config-datasource",
          "--to-schema",
          variantPath("relation-mode-prisma", "schema.prisma"),
          "--exit-code",
        ],
        NO_LOCK,
      );
      expect(drift.stderr + drift.stdout).toContain("No difference detected");
      expect(drift.status).toBe(0);
    },
    120_000,
  );

  xfailOnSereneDB("no pg_advisory_lock, migrate deploy locks the database")(
    "migrate deploy holds the migration advisory lock",
    () => {
      expect(
        prismaCli("relation-mode-prisma", LOCKED, ["migrate", "deploy"], {}),
      ).toContain("All migrations have been successfully applied");
    },
    120_000,
  );

  xfailOnSereneDB(
    "foreign keys reject ON DELETE/UPDATE CASCADE, ALTER TABLE ADD FOREIGN KEY (#1427)",
  )(
    "the default relation mode adds foreign keys with ALTER TABLE",
    async () => {
      await execute(FOREIGN_KEYS, migrations["foreign-keys"]);
    },
  );

  xfailOnSereneDB("jsonb columns (#613)")(
    "Json fields are jsonb columns",
    async () => {
      await execute(JSON_DB, migrations.json);
    },
  );

  xfailOnSereneDB("TIME(p), TIMETZ(p) and TIMESTAMPTZ(p) are rejected (#1434)")(
    "Time, Timetz and Timestamptz fields keep their precision",
    async () => {
      await execute(PRECISION, migrations.precision);
    },
  );

  xfailOnSereneDB(DESCRIBE_ANY)(
    "db pull introspects the migrated schema",
    () => {
      checkGolden(
        path.join(FIXTURES, "pull.prisma"),
        prismaCli("relation-mode-prisma", MAIN, ["db", "pull", "--print"], {}),
      );
    },
    120_000,
  );

  xfailOnSereneDB(DESCRIBE_ANY)(
    "db push finds the database in sync",
    () => {
      expect(
        prismaCli("relation-mode-prisma", MAIN, ["db", "push"], NO_LOCK),
      ).toContain("The database is already in sync with the Prisma schema");
    },
    120_000,
  );
});

describe("client", () => {
  test("creates nested records and reads every scalar type back", async () => {
    const created = await prisma.user.create({
      data: {
        email: "pudge@dota.example",
        name: "Pudge",
        role: "WRITER",
        karma: 9007199254740993n,
        balance: "1234.56",
        ratio: 0.25,
        score: 1.5,
        active: false,
        avatar: new Uint8Array([0, 1, 254, 255]),
        birthday: new Date("2004-02-01T00:00:00Z"),
        nicknames: ["butcher", "hook"],
        profile: { create: { bio: "fresh meat" } },
        posts: {
          create: [
            {
              title: "Meat Hook",
              views: 7,
              categories: {
                connectOrCreate: {
                  where: { name: "guides" },
                  create: { name: "guides" },
                },
              },
            },
            { title: "Rot", body: "toggle", published: true },
          ],
        },
      },
      include: { profile: true, posts: { orderBy: { id: "asc" } } },
    });
    expect(created.id).toBeGreaterThan(0);
    expect(created.externalId).toMatch(/^[0-9a-f-]{36}$/);
    expect(created.profile.bio).toBe("fresh meat");
    expect(created.posts.map((p) => p.title)).toEqual(["Meat Hook", "Rot"]);

    const fresh = await prisma.user.findUniqueOrThrow({
      where: { email: "pudge@dota.example" },
      include: { posts: { include: { categories: true } } },
    });
    expect(fresh.role).toBe("WRITER");
    expect(fresh.karma).toBe(9007199254740993n);
    expect(fresh.balance.toString()).toBe("1234.56");
    expect(fresh.ratio).toBe(0.25);
    expect(fresh.score).toBe(1.5);
    expect(fresh.active).toBe(false);
    expect([...fresh.avatar]).toEqual([0, 1, 254, 255]);
    expect(fresh.birthday.toISOString()).toBe("2004-02-01T00:00:00.000Z");
    expect(fresh.nicknames).toEqual(["butcher", "hook"]);
    expect(fresh.createdAt).toBeInstanceOf(Date);
    expect(fresh.updatedAt).toBeInstanceOf(Date);
    expect(
      fresh.posts.find((p) => p.title === "Meat Hook").categories.map((c) => c.name),
    ).toEqual(["guides"]);
    expect(
      await prisma.user.findUnique({ where: { externalId: fresh.externalId } }),
    ).not.toBeNull();
  });

  xfailOnSereneDB(RETURNING_NAMES)(
    "create returns the record with its enum field",
    async () => {
      const user = await prisma.user.create({
        data: { email: "invoker@dota.example", role: "ADMIN" },
      });
      expect(user.role).toBe("ADMIN");
    },
  );

  test("filters, sorts and paginates", async () => {
    const author = await prisma.user.create({
      data: {
        email: "crystal@dota.example",
        name: "Crystal Maiden",
        posts: {
          create: Array.from({ length: 6 }, (_, i) => ({
            title: `Frostbite ${i}`,
            views: i * 10,
            published: i % 2 === 0,
          })),
        },
      },
      select: { id: true },
    });
    const posts = { authorId: author.id };
    expect(
      await prisma.post.count({
        where: { ...posts, title: { contains: "BITE", mode: "insensitive" } },
      }),
    ).toBe(6);
    expect(
      await prisma.post.count({ where: { ...posts, views: { in: [0, 20, 99] } } }),
    ).toBe(2);
    expect(
      await prisma.post.count({
        where: { ...posts, OR: [{ views: 0 }, { views: 50 }], NOT: { published: true } },
      }),
    ).toBe(1);
    expect(await prisma.post.count({ where: { ...posts, body: null } })).toBe(6);
    const page = await prisma.post.findMany({
      where: posts,
      orderBy: [{ published: "desc" }, { views: "desc" }],
      skip: 1,
      take: 3,
      select: { views: true },
    });
    expect(page.map((p) => p.views)).toEqual([20, 0, 50]);
    const first = await prisma.post.findFirst({
      where: posts,
      orderBy: { views: "asc" },
    });
    const after = await prisma.post.findMany({
      where: posts,
      cursor: { id: first.id },
      skip: 1,
      take: 2,
      orderBy: { id: "asc" },
    });
    expect(after.map((p) => p.views)).toEqual([10, 20]);
    expect(
      await prisma.user.count({
        where: { posts: { some: { title: "Frostbite 5" } } },
      }),
    ).toBe(1);
    expect(
      await prisma.user.count({
        where: { id: author.id, posts: { every: { views: { lt: 100 } } } },
      }),
    ).toBe(1);
    expect(
      await prisma.user.count({ where: { id: author.id, posts: { none: {} } } }),
    ).toBe(0);
    const distinct = await prisma.post.findMany({
      where: posts,
      distinct: ["published"],
      orderBy: { published: "asc" },
      select: { published: true },
    });
    expect(distinct.map((p) => p.published)).toEqual([false, true]);
  });

  xfailOnSereneDB(PARAMETER_TYPES)(
    "startsWith combined with a numeric filter",
    async () => {
      expect(
        await prisma.post.count({
          where: { title: { startsWith: "Frost" }, views: { gte: 30 } },
        }),
      ).toBe(3);
    },
  );

  test("aggregates and groups", async () => {
    const author = await prisma.user.create({
      data: {
        email: "shadow@dota.example",
        posts: {
          create: [10, 20, 20, 50].map((views, i) => ({
            title: `Requiem ${i}`,
            views,
            published: views === 20,
          })),
        },
      },
      select: { id: true },
    });
    const totals = await prisma.post.aggregate({
      where: { authorId: author.id },
      _count: { _all: true },
      _sum: { views: true },
      _avg: { views: true },
      _min: { views: true },
      _max: { views: true },
    });
    expect(totals._count._all).toBe(4);
    expect(totals._sum.views).toBe(100);
    expect(totals._avg.views).toBe(25);
    expect(totals._min.views).toBe(10);
    expect(totals._max.views).toBe(50);
    const groups = await prisma.post.groupBy({
      by: ["published"],
      where: { authorId: author.id },
      _count: { _all: true },
      _sum: { views: true },
      having: { views: { _sum: { gt: 0 } } },
      orderBy: { published: "asc" },
    });
    expect(groups.map((g) => [g.published, g._count._all, g._sum.views])).toEqual([
      [false, 2, 60],
      [true, 2, 40],
    ]);
    const withCounts = await prisma.user.findUniqueOrThrow({
      where: { id: author.id },
      include: { _count: { select: { posts: true } } },
    });
    expect(withCounts._count.posts).toBe(4);
  });

  test("updates, upserts and deletes records without enum fields", async () => {
    const author = await prisma.user.create({
      data: { email: "tinker@dota.example" },
      select: { id: true },
    });
    const post = await prisma.post.create({
      data: { title: "March", authorId: author.id },
    });
    const renamed = await prisma.post.update({
      where: { id: post.id },
      data: { title: "March of the Machines", published: true },
    });
    expect(renamed.title).toBe("March of the Machines");
    const created = await prisma.category.upsert({
      where: { name: "heroes" },
      update: {},
      create: { name: "heroes" },
    });
    const again = await prisma.category.upsert({
      where: { name: "heroes" },
      update: { name: "heroes" },
      create: { name: "heroes" },
    });
    expect(again.id).toBe(created.id);
    expect(
      (
        await prisma.post.updateMany({
          where: { authorId: author.id },
          data: { body: "rearm" },
        })
      ).count,
    ).toBe(1);
    const removed = await prisma.post.delete({ where: { id: post.id } });
    expect(removed.body).toBe("rearm");
    expect(await prisma.post.count({ where: { authorId: author.id } })).toBe(0);
  });

  xfailOnSereneDB(RETURNING_NAMES)(
    "update and upsert return the record with its enum field",
    async () => {
      await prisma.user.create({
        data: { email: "zeus@dota.example" },
        select: { id: true },
      });
      const updated = await prisma.user.update({
        where: { email: "zeus@dota.example" },
        data: { name: "Zeus", role: "ADMIN" },
      });
      expect(updated.role).toBe("ADMIN");
      const upserted = await prisma.user.upsert({
        where: { email: "zeus@dota.example" },
        update: { name: "Zeus" },
        create: { email: "zeus@dota.example" },
      });
      expect(upserted.role).toBe("ADMIN");
    },
  );

  xfailOnSereneDB(PARAMETER_TYPES)(
    "atomic number operations",
    async () => {
      const author = await prisma.user.create({
        data: { email: "techies@dota.example", posts: { create: { title: "Mines" } } },
        include: { posts: true },
      });
      const post = await prisma.post.update({
        where: { id: author.posts[0].id },
        data: { views: { increment: 5 } },
      });
      expect(post.views).toBe(5);
      const user = await prisma.user.update({
        where: { id: author.id },
        data: { karma: { increment: 3 }, balance: { decrement: 1 } },
        select: { karma: true, balance: true },
      });
      expect(user.karma).toBe(3n);
      expect(user.balance.toString()).toBe("-1");
    },
  );

  xfailOnSereneDB(PARAMETER_TYPES)(
    "scalar list push",
    async () => {
      const user = await prisma.user.create({
        data: { email: "meepo@dota.example", nicknames: ["geomancer"] },
        select: { id: true },
      });
      const pushed = await prisma.user.update({
        where: { id: user.id },
        data: { nicknames: { push: "divided we stand" } },
        select: { nicknames: true },
      });
      expect(pushed.nicknames).toEqual(["geomancer", "divided we stand"]);
    },
  );

  test("delete emulates restrict and cascade in relationMode prisma", async () => {
    const user = await prisma.user.create({
      data: {
        email: "undying@dota.example",
        profile: { create: { bio: "tombstone" } },
        posts: { create: { title: "Soul Rip" } },
      },
      select: { id: true },
    });
    await expect(
      prisma.user.delete({ where: { id: user.id }, select: { id: true } }),
    ).rejects.toThrow();
    await prisma.post.deleteMany({ where: { authorId: user.id } });
    await prisma.user.delete({ where: { id: user.id }, select: { id: true } });
    expect(await prisma.profile.count({ where: { userId: user.id } })).toBe(0);
    expect(await prisma.user.findUnique({ where: { id: user.id } })).toBeNull();
  });

  test("createMany skips duplicates and returns created rows", async () => {
    const created = await prisma.category.createManyAndReturn({
      data: [{ name: "lore" }, { name: "patches" }],
      select: { name: true },
    });
    expect(created.map((c) => c.name).sort()).toEqual(["lore", "patches"]);
    const result = await prisma.category.createMany({
      data: [{ name: "lore" }, { name: "esports" }],
      skipDuplicates: true,
    });
    expect(result.count).toBe(1);
    await prisma.tagging.createMany({
      data: [
        { postId: 1, tag: "meta" },
        { postId: 1, tag: "carry" },
      ],
    });
    expect(
      await prisma.tagging.findUnique({
        where: { postId_tag: { postId: 1, tag: "meta" } },
      }),
    ).not.toBeNull();
  });

  test("batch and interactive transactions", async () => {
    const [a, b] = await prisma.$transaction([
      prisma.category.create({ data: { name: "batch-a" } }),
      prisma.category.create({ data: { name: "batch-b" } }),
    ]);
    expect(b.id).toBeGreaterThan(a.id);
    await expect(
      prisma.$transaction(async (tx) => {
        await tx.category.create({ data: { name: "rolled-back" } });
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(await prisma.category.count({ where: { name: "rolled-back" } })).toBe(0);
    const total = await prisma.$transaction(
      async (tx) => {
        await tx.category.create({ data: { name: "repeatable" } });
        return tx.category.count();
      },
      { isolationLevel: "RepeatableRead" },
    );
    expect(total).toBeGreaterThan(0);
  });

  xfailOnSereneDB("SERIALIZABLE isolation is rejected")(
    "serializable interactive transaction",
    async () => {
      const total = await prisma.$transaction(
        async (tx) => {
          await tx.category.create({ data: { name: "serializable" } });
          return tx.category.count();
        },
        { isolationLevel: "Serializable" },
      );
      expect(total).toBeGreaterThan(0);
    },
  );

  test("raw queries bind parameters", async () => {
    const name = "guides";
    const rows =
      await prisma.$queryRaw`SELECT id, name FROM "Category" WHERE name = ${name}`;
    expect(rows.map((r) => r.name)).toEqual(["guides"]);
    const typed = await prisma.$queryRawUnsafe(
      "SELECT $1::int + $2::int AS sum, $3::text AS label, $4::bigint AS big",
      2,
      3,
      "hook",
      9007199254740993n,
    );
    expect(typed).toEqual([{ sum: 5, label: "hook", big: 9007199254740993n }]);
    expect(
      await prisma.$executeRaw`UPDATE "Category" SET name = name WHERE name = ${name}`,
    ).toBe(1);
  });

  test("scalar lists support has, hasSome, hasEvery and isEmpty", async () => {
    await prisma.user.createMany({
      data: [
        { email: "rubick@dota.example", nicknames: ["grand", "magus"] },
        { email: "lina@dota.example", nicknames: [] },
      ],
    });
    expect(
      await prisma.user.count({ where: { nicknames: { has: "magus" } } }),
    ).toBe(1);
    expect(
      await prisma.user.count({ where: { nicknames: { hasSome: ["grand", "x"] } } }),
    ).toBe(1);
    expect(
      await prisma.user.count({
        where: { nicknames: { hasEvery: ["grand", "magus"] } },
      }),
    ).toBe(1);
    expect(
      await prisma.user.count({
        where: { email: "lina@dota.example", nicknames: { isEmpty: true } },
      }),
    ).toBe(1);
  });
});
