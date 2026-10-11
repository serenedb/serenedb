-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateTable
CREATE TABLE "Event" (
    "id" SERIAL NOT NULL,
    "payload" JSONB NOT NULL,
    "tags" JSONB DEFAULT '[]',

    CONSTRAINT "Event_pkey" PRIMARY KEY ("id")
);

