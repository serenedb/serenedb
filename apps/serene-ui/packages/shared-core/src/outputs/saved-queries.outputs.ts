import { SavedQuerySchema } from "../schemas";
import z from "zod";

export const ListMySavedQueriesOutput = z.array(SavedQuerySchema);
export type ListMySavedQueriesOutput = z.infer<typeof ListMySavedQueriesOutput>;

export const AddSavedQueryOutput = SavedQuerySchema;
export type AddSavedQueryOutput = z.infer<typeof AddSavedQueryOutput>;

export const UpdateSavedQueryOutput = SavedQuerySchema;
export type UpdateSavedQueryOutput = z.infer<typeof UpdateSavedQueryOutput>;

export const DeleteSavedQueryOutput = SavedQuerySchema;
export type DeleteSavedQueryOutput = z.infer<typeof DeleteSavedQueryOutput>;
