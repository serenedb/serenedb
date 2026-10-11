import { relations, sql } from "drizzle-orm";
import {
  bigint,
  boolean,
  check,
  date,
  doublePrecision,
  index,
  integer,
  numeric,
  pgEnum,
  pgTable,
  serial,
  smallint,
  text,
  timestamp,
  unique,
  uuid,
  varchar,
} from "drizzle-orm/pg-core";

export function schema({ foreignKeys, indexes }) {
  const role = pgEnum("hero_role", ["carry", "support", "offlane"]);
  const heroes = pgTable(
    "heroes",
    {
      id: serial("id").primaryKey(),
      name: varchar("name", { length: 100 }).notNull().unique(),
      title: text("title"),
      role: role("role").default("carry").notNull(),
      level: smallint("level").default(1).notNull(),
      gold: bigint("gold", { mode: "bigint" }).default(sql`0`).notNull(),
      rating: doublePrecision("rating"),
      power: numeric("power", { precision: 10, scale: 2 }).default("0").notNull(),
      active: boolean("active").default(true).notNull(),
      born: date("born"),
      aliases: text("aliases").array(),
      uid: uuid("uid").defaultRandom().notNull(),
      createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
    },
    (t) => [check("heroes_gold_nonneg", sql`${t.gold} >= 0`)],
  );
  const heroId = integer("hero_id");
  const items = pgTable(
    "items",
    {
      id: serial("id").primaryKey(),
      name: varchar("name", { length: 60 }).notNull(),
      cost: integer("cost").default(0).notNull(),
      heroId: foreignKeys ? heroId.references(() => heroes.id) : heroId,
    },
    (t) => [
      ...(indexes ? [index("items_name_cost").on(t.name, t.cost)] : []),
      unique("items_hero_name").on(t.heroId, t.name),
    ],
  );
  const heroesRelations = relations(heroes, ({ many }) => ({ items: many(items) }));
  const itemsRelations = relations(items, ({ one }) => ({
    hero: one(heroes, { fields: [items.heroId], references: [heroes.id] }),
  }));
  return { role, heroes, items, heroesRelations, itemsRelations };
}
