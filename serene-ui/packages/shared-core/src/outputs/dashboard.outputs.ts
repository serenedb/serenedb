import z from "zod";
import { DashboardSchema } from "../schemas";

export const ListMyDashboardsOutput = z.array(DashboardSchema);
export type ListMyDashboardsOutput = z.infer<typeof ListMyDashboardsOutput>;

export const ListFavoriteDashboardsOutput = z.array(DashboardSchema);
export type ListFavoriteDashboardsOutput = z.infer<
    typeof ListFavoriteDashboardsOutput
>;

export const GetDashboardOutput = DashboardSchema;
export type GetDashboardOutput = z.infer<typeof GetDashboardOutput>;

export const AddDashboardOutput = DashboardSchema;
export type AddDashboardOutput = z.infer<typeof AddDashboardOutput>;

export const UpdateDashboardOutput = DashboardSchema;
export type UpdateDashboardOutput = z.infer<typeof UpdateDashboardOutput>;

export const DeleteDashboardOutput = DashboardSchema;
export type DeleteDashboardOutput = z.infer<typeof DeleteDashboardOutput>;
