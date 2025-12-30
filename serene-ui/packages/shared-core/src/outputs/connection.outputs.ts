import { ConnectionSchema } from "../schemas/connection";
import z from "zod";

export const ListMyConnectionOutput = z.array(ConnectionSchema);
export type ListMyConnectionOutput = z.infer<typeof ListMyConnectionOutput>;

export const AddConnectionOutput = ConnectionSchema;
export type AddConnectionOutput = z.infer<typeof AddConnectionOutput>;

export const UpdateConnectionOutput = ConnectionSchema;
export type UpdateConnectionOutput = z.infer<typeof UpdateConnectionOutput>;

export const DeleteConnectionOutput = ConnectionSchema;
export type DeleteConnectionOutput = z.infer<typeof DeleteConnectionOutput>;
