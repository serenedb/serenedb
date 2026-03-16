import z from "zod";
import {
    DashboardBlockBaseSchema,
    DashboardBlockBoundsSchema,
    DashboardQueryBlockBaseSchema,
} from "./base.schema";
import {
    DashboardBarChartBlockInputSchema,
    DashboardBarChartBlockSchema,
    DashboardBarChartBlockUpdateSchema,
} from "./bar-chart-block.schema";
import {
    DashboardChartSeriesSchema,
    DashboardLineTypeSchema,
} from "./chart.schema";
import {
    DashboardLineChartBlockInputSchema,
    DashboardLineChartBlockSchema,
    DashboardLineChartBlockUpdateSchema,
} from "./line-chart-block.schema";
import {
    DashboardPieChartBlockInputSchema,
    DashboardPieChartBlockSchema,
    DashboardPieChartBlockUpdateSchema,
} from "./pie-chart-block.schema";
import {
    DashboardSingleStringBlockInputSchema,
    DashboardSingleStringBlockSchema,
    DashboardSingleStringBlockUpdateSchema,
} from "./single-string-block.schema";
import {
    DashboardSpacerBlockInputSchema,
    DashboardSpacerBlockSchema,
    DashboardSpacerBlockUpdateSchema,
} from "./spacer-block.schema";
import {
    DashboardTableBlockInputSchema,
    DashboardTableBlockSchema,
    DashboardTableBlockUpdateSchema,
} from "./table-block.schema";
import {
    DashboardTextBlockInputSchema,
    DashboardTextBlockSchema,
    DashboardTextBlockUpdateSchema,
} from "./text-block.schema";

export {
    DashboardBlockBaseSchema,
    DashboardBlockBoundsSchema,
    DashboardQueryBlockBaseSchema,
} from "./base.schema";
export {
    DashboardBarChartBlockInputSchema,
    DashboardBarChartBlockSchema,
    DashboardBarChartBlockUpdateSchema,
} from "./bar-chart-block.schema";
export {
    DashboardChartSeriesSchema,
    DashboardLineTypeSchema,
} from "./chart.schema";
export {
    DashboardLineChartBlockInputSchema,
    DashboardLineChartBlockSchema,
    DashboardLineChartBlockUpdateSchema,
} from "./line-chart-block.schema";
export {
    DashboardPieChartBlockInputSchema,
    DashboardPieChartBlockSchema,
    DashboardPieChartBlockUpdateSchema,
} from "./pie-chart-block.schema";
export {
    DashboardSingleStringBlockInputSchema,
    DashboardSingleStringBlockSchema,
    DashboardSingleStringBlockUpdateSchema,
} from "./single-string-block.schema";
export {
    DashboardSpacerBlockInputSchema,
    DashboardSpacerBlockSchema,
    DashboardSpacerBlockUpdateSchema,
} from "./spacer-block.schema";
export {
    DashboardTableBlockInputSchema,
    DashboardTableBlockSchema,
    DashboardTableBlockUpdateSchema,
} from "./table-block.schema";
export {
    DashboardTextBlockInputSchema,
    DashboardTextBlockSchema,
    DashboardTextBlockUpdateSchema,
} from "./text-block.schema";

export const DashboardBlockSchema = z.discriminatedUnion("type", [
    DashboardTextBlockSchema,
    DashboardSpacerBlockSchema,
    DashboardTableBlockSchema,
    DashboardSingleStringBlockSchema,
    DashboardBarChartBlockSchema,
    DashboardLineChartBlockSchema,
    DashboardPieChartBlockSchema,
]);
export type DashboardBlockSchema = z.infer<typeof DashboardBlockSchema>;

export const DashboardBlockInputSchema = z.discriminatedUnion("type", [
    DashboardTextBlockInputSchema,
    DashboardSpacerBlockInputSchema,
    DashboardTableBlockInputSchema,
    DashboardSingleStringBlockInputSchema,
    DashboardBarChartBlockInputSchema,
    DashboardLineChartBlockInputSchema,
    DashboardPieChartBlockInputSchema,
]);
export type DashboardBlockInputSchema = z.infer<
    typeof DashboardBlockInputSchema
>;

export const DashboardBlockUpdateSchema = z.discriminatedUnion("type", [
    DashboardTextBlockUpdateSchema,
    DashboardSpacerBlockUpdateSchema,
    DashboardTableBlockUpdateSchema,
    DashboardSingleStringBlockUpdateSchema,
    DashboardBarChartBlockUpdateSchema,
    DashboardLineChartBlockUpdateSchema,
    DashboardPieChartBlockUpdateSchema,
]);
export type DashboardBlockUpdateSchema = z.infer<
    typeof DashboardBlockUpdateSchema
>;
