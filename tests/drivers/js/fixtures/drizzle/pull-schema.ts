import { pgTable, unique, check, serial, varchar, text, smallint, bigint, doublePrecision, numeric, boolean, date, uuid, timestamp, integer, pgEnum } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"

export const heroRole = pgEnum("hero_role", ['carry', 'support', 'offlane'])


export const heroes = pgTable("heroes", {
	id: serial().primaryKey().notNull(),
	name: varchar({ length: 100 }).notNull(),
	title: text(),
	role: heroRole().default('carry').notNull(),
	level: smallint().default(1).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	gold: bigint({ mode: "number" }).default(0).notNull(),
	rating: doublePrecision(),
	power: numeric({ precision: 10, scale:  2 }).default('0').notNull(),
	active: boolean().default(true).notNull(),
	born: date(),
	aliases: text().array(),
	uid: uuid().defaultRandom().notNull(),
	createdAt: timestamp("created_at", { withTimezone: true, mode: 'string' }).defaultNow().notNull(),
}, (table) => [
	unique("heroes_name_unique").on(table.name),
	check("heroes_gold_nonneg", sql`gold >= 0`),
]);

export const items = pgTable("items", {
	id: serial().primaryKey().notNull(),
	name: varchar({ length: 60 }).notNull(),
	cost: integer().default(0).notNull(),
	heroId: integer("hero_id"),
}, (table) => [
	unique("items_hero_name").on(table.name, table.heroId),
]);
