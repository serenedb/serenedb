-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateTable
CREATE TABLE "Clock" (
    "id" SERIAL NOT NULL,
    "wakes_at" TIME(6) NOT NULL,
    "sleeps_at" TIMETZ(3) NOT NULL,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Clock_pkey" PRIMARY KEY ("id")
);

