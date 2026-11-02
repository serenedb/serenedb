import type { DashboardBlockSchema } from "@serene-ui/shared-core";

type DashboardBlockMinSize = {
    minW: number;
    minH: number;
};

const DEFAULT_DASHBOARD_BLOCK_MIN_SIZE: DashboardBlockMinSize = {
    minW: 8,
    minH: 6,
};

const DASHBOARD_BLOCK_MIN_SIZE: Record<
    DashboardBlockSchema["type"],
    DashboardBlockMinSize
> = {
    text: {
        minW: 8,
        minH: 2,
    },
    spacer: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
    table: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
    single_string: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
    bar_chart: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
    line_chart: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
    area_chart: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
    pie_chart: DEFAULT_DASHBOARD_BLOCK_MIN_SIZE,
};

export const getDashboardBlockMinSize = (
    type: DashboardBlockSchema["type"],
): DashboardBlockMinSize => DASHBOARD_BLOCK_MIN_SIZE[type];
