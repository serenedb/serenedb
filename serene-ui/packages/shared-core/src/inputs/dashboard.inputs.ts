import z from "zod";
import {
    DashboardBaseSchema,
    DashboardBlockInputSchema,
    DashboardBlockUpdateSchema,
} from "../schemas";

export const AddDashboardBlockInput = DashboardBlockInputSchema;
export type AddDashboardBlockInput = z.infer<typeof AddDashboardBlockInput>;

export const UpdateDashboardBlockInput = DashboardBlockUpdateSchema;
export type UpdateDashboardBlockInput = z.infer<
    typeof UpdateDashboardBlockInput
>;

export const AddDashboardInput = DashboardBaseSchema.omit({
    id: true,
    created_at: true,
    updated_at: true,
}).extend({
    blocks: z.array(AddDashboardBlockInput).default([]),
});
export type AddDashboardInput = z.infer<typeof AddDashboardInput>;

export const UpdateDashboardInput = DashboardBaseSchema.omit({
    created_at: true,
    updated_at: true,
})
    .partial()
    .required({
        id: true,
    })
    .extend({
        blocks: z.array(UpdateDashboardBlockInput).optional(),
    });
export type UpdateDashboardInput = z.infer<typeof UpdateDashboardInput>;

export const GetDashboardInput = z.object({
    id: z.number(),
});
export type GetDashboardInput = z.infer<typeof GetDashboardInput>;

export const DeleteDashboardInput = z.object({
    id: z.number(),
});
export type DeleteDashboardInput = z.infer<typeof DeleteDashboardInput>;
