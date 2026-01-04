import { SavedQuerySchema } from "../schemas";
import z from "zod";

export const AddSavedQueryInput = SavedQuerySchema.omit({ id: true });
export type AddSavedQueryInput = z.infer<typeof AddSavedQueryInput>;

export const UpdateSavedQueryInput = SavedQuerySchema.partial().required({
    id: true,
});
export type UpdateSavedQueryInput = z.infer<typeof UpdateSavedQueryInput>;

export const DeleteSavedQueryInput = z.object({ id: z.number() });
export type DeleteSavedQueryInput = z.infer<typeof DeleteSavedQueryInput>;
