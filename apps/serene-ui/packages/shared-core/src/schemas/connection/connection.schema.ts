import z from "zod";
import { PostgresConnectionUnion } from "./postgres-connection.schema";

export const ConnectionSchema = PostgresConnectionUnion;
export type ConnectionSchema = z.infer<typeof ConnectionSchema>;
