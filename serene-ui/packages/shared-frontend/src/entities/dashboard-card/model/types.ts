import type {
    AddDashboardBlockInput,
    DashboardBlockSchema,
    UpdateDashboardBlockInput,
} from "@serene-ui/shared-core";

export type DashboardCardSchema = DashboardBlockSchema;
export type AddDashboardCardInput = AddDashboardBlockInput;
export type UpdateDashboardCardInput = UpdateDashboardBlockInput;

export const toDashboardCardUpdateInput = (
    card: DashboardCardSchema,
): UpdateDashboardCardInput => {
    const { dashboard_id: _dashboardId, ...nextCard } = card;

    return nextCard;
};

export const toDashboardCardAddInput = (
    card: DashboardCardSchema,
): AddDashboardCardInput => {
    const { id: _id, dashboard_id: _dashboardId, ...nextCard } = card;

    return nextCard;
};

export const isDashboardQueryCard = (
    card: DashboardCardSchema,
): card is Extract<
    DashboardCardSchema,
    | { type: "table" }
    | { type: "single_string" }
    | { type: "bar_chart" }
    | { type: "line_chart" }
    | { type: "area_chart" }
    | { type: "pie_chart" }
> => {
    return (
        card.type === "table" ||
        card.type === "single_string" ||
        card.type === "bar_chart" ||
        card.type === "line_chart" ||
        card.type === "area_chart" ||
        card.type === "pie_chart"
    );
};
