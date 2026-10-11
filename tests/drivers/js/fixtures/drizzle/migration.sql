CREATE TYPE "public"."hero_role" AS ENUM('carry', 'support', 'offlane');;
CREATE TABLE "heroes" (
	"id" serial PRIMARY KEY NOT NULL,
	"name" varchar(100) NOT NULL,
	"title" text,
	"role" "hero_role" DEFAULT 'carry' NOT NULL,
	"level" smallint DEFAULT 1 NOT NULL,
	"gold" bigint DEFAULT 0 NOT NULL,
	"rating" double precision,
	"power" numeric(10, 2) DEFAULT '0' NOT NULL,
	"active" boolean DEFAULT true NOT NULL,
	"born" date,
	"aliases" text[],
	"uid" uuid DEFAULT gen_random_uuid() NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "heroes_name_unique" UNIQUE("name"),
	CONSTRAINT "heroes_gold_nonneg" CHECK ("heroes"."gold" >= 0)
);
;
CREATE TABLE "items" (
	"id" serial PRIMARY KEY NOT NULL,
	"name" varchar(60) NOT NULL,
	"cost" integer DEFAULT 0 NOT NULL,
	"hero_id" integer,
	CONSTRAINT "items_hero_name" UNIQUE("hero_id","name")
);
;
ALTER TABLE "items" ADD CONSTRAINT "items_hero_id_heroes_id_fk" FOREIGN KEY ("hero_id") REFERENCES "public"."heroes"("id") ON DELETE no action ON UPDATE no action;;
CREATE INDEX "items_name_cost" ON "items" USING btree ("name","cost");;
