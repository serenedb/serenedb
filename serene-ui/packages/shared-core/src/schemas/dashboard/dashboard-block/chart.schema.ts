import z from "zod";

export const DashboardChartSeriesSchema = z.object({
    key: z.string().min(1).max(255),
    label: z.string().min(1).max(255),
    color: z.string().max(255),
});
export type DashboardChartSeriesSchema = z.infer<
    typeof DashboardChartSeriesSchema
>;

export const DashboardLineTypeSchema = z.enum([
    "basis",
    "basisClosed",
    "basisOpen",
    "bump",
    "bumpX",
    "bumpY",
    "linear",
    "linearClosed",
    "monotone",
    "monotoneX",
    "monotoneY",
    "natural",
    "step",
    "stepAfter",
    "stepBefore",
]);
export type DashboardLineTypeSchema = z.infer<typeof DashboardLineTypeSchema>;
