import { ConnectionSchema } from "../schemas/connection";
import z from "zod";

export const ConnectionListItemSchema = z.intersection(
    ConnectionSchema,
    z.object({
        isDefault: z.boolean().optional(),
    }),
);
export type ConnectionListItemSchema = z.infer<typeof ConnectionListItemSchema>;

export const ListMyConnectionOutput = z.array(ConnectionListItemSchema);
export type ListMyConnectionOutput = z.infer<typeof ListMyConnectionOutput>;

export const AddConnectionOutput = ConnectionSchema;
export type AddConnectionOutput = z.infer<typeof AddConnectionOutput>;

export const UpdateConnectionOutput = ConnectionSchema;
export type UpdateConnectionOutput = z.infer<typeof UpdateConnectionOutput>;

export const DeleteConnectionOutput = ConnectionSchema;
export type DeleteConnectionOutput = z.infer<typeof DeleteConnectionOutput>;
