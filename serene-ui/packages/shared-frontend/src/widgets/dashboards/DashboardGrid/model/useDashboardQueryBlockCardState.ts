import React from "react";
import type { DashboardSchema } from "@serene-ui/shared-core";
import {
    type DashboardQueryRow,
    collectDashboardColumnMetadata,
    formatDashboardDateValue,
} from "../../model/dashboardChartColumns";
import {
    isDashboardInteractiveBlock,
    syncDashboardInteractiveSelection,
} from "./useInteractiveSelection";
import {
    type DashboardQueryBlock,
    getColumnMetaByName,
    getInteractiveSelectionValues,
} from "./dashboardQueryBlockCard";
import { useDashboardQueryBlock } from "./useDashboardQueryBlock";

interface UseDashboardQueryBlockCardStateProps {
    block: DashboardQueryBlock;
    dashboard?: DashboardSchema | null;
}

export const useDashboardQueryBlockCardState = ({
    block,
    dashboard,
}: UseDashboardQueryBlockCardStateProps) => {
    const { rows, status, errorMessage } = useDashboardQueryBlock({
        block,
        dashboard,
    });

    const columns = React.useMemo(
        () =>
            collectDashboardColumnMetadata({
                rows: rows as DashboardQueryRow[],
            }),
        [rows],
    );

    const columnsByName = React.useMemo(
        () => getColumnMetaByName(columns),
        [columns],
    );

    const barLineDateFormatter = React.useCallback(
        (value: string | number) => formatDashboardDateValue(value),
        [],
    );

    const interactiveSelectionValues = React.useMemo(
        () =>
            getInteractiveSelectionValues({
                block,
                status,
                rows,
                columnsByName,
            }),
        [block, columnsByName, rows, status],
    );

    const interactiveSelectionValuesKey = React.useMemo(
        () => interactiveSelectionValues?.join("\u0000") ?? "",
        [interactiveSelectionValues],
    );

    const interactiveSelectionBlockKey = React.useMemo(() => {
        if (!isDashboardInteractiveBlock(block)) {
            return "static";
        }

        if (block.type === "pie_chart") {
            return `pie:${block.interactive ? "interactive" : "static"}`;
        }

        return `${block.type}:${block.variant}`;
    }, [block]);

    React.useEffect(() => {
        if (
            !isDashboardInteractiveBlock(block) ||
            status !== "ready" ||
            !interactiveSelectionValues
        ) {
            return;
        }

        syncDashboardInteractiveSelection({
            dashboardId: dashboard?.id ?? block.dashboard_id,
            blockId: block.id,
            query: block.query,
            availableValues: interactiveSelectionValues,
        });
    }, [
        block.id,
        block.dashboard_id,
        block.query,
        dashboard?.id,
        interactiveSelectionBlockKey,
        interactiveSelectionValuesKey,
        status,
    ]);

    return {
        rows,
        status,
        errorMessage,
        columns,
        columnsByName,
        barLineDateFormatter,
    };
};
