import React from "react";
import type { DashboardBlockSchema } from "@serene-ui/shared-core";

const DASHBOARD_INTERACTIVE_SELECTION_STORAGE_PREFIX =
    "dashboard:interactive-selection";

interface DashboardInteractiveSelectionPayload {
    query: string;
    selected: string;
}

type DashboardInteractiveBlock = Extract<
    DashboardBlockSchema,
    { type: "bar_chart" | "line_chart" | "pie_chart" }
>;

const getStorageKey = (dashboardId: number, blockId: number) =>
    `${DASHBOARD_INTERACTIVE_SELECTION_STORAGE_PREFIX}:${dashboardId}:${blockId}`;

const isBrowser = () => typeof window !== "undefined";

const parseSelectionPayload = (value: string | null) => {
    if (!value) {
        return null;
    }

    try {
        const parsed = JSON.parse(
            value,
        ) as Partial<DashboardInteractiveSelectionPayload>;

        if (
            typeof parsed.query !== "string" ||
            typeof parsed.selected !== "string"
        ) {
            return null;
        }

        return {
            query: parsed.query,
            selected: parsed.selected,
        } satisfies DashboardInteractiveSelectionPayload;
    } catch {
        return null;
    }
};

export const isDashboardInteractiveBlock = (
    block: DashboardBlockSchema,
): block is DashboardInteractiveBlock => {
    return (
        (block.type === "bar_chart" && block.variant === "interactive") ||
        (block.type === "line_chart" && block.variant === "interactive") ||
        (block.type === "pie_chart" && block.interactive)
    );
};

export const readDashboardInteractiveSelection = (
    dashboardId: number,
    blockId: number,
) => {
    if (!isBrowser()) {
        return null;
    }

    return parseSelectionPayload(
        window.localStorage.getItem(getStorageKey(dashboardId, blockId)),
    );
};

export const writeDashboardInteractiveSelection = (
    dashboardId: number,
    blockId: number,
    payload: DashboardInteractiveSelectionPayload,
) => {
    if (!isBrowser()) {
        return;
    }

    window.localStorage.setItem(
        getStorageKey(dashboardId, blockId),
        JSON.stringify(payload),
    );
};

export const removeDashboardInteractiveSelection = (
    dashboardId: number,
    blockId: number,
) => {
    if (!isBrowser()) {
        return;
    }

    window.localStorage.removeItem(getStorageKey(dashboardId, blockId));
};

const getResolvedDashboardInteractiveSelection = ({
    dashboardId,
    blockId,
    query,
    availableValues,
}: {
    dashboardId: number;
    blockId: number;
    query: string;
    availableValues: string[];
}) => {
    const firstAvailableValue = availableValues[0] ?? "";

    if (!isBrowser()) {
        return firstAvailableValue;
    }

    if (!availableValues.length) {
        return "";
    }

    const storedSelection = readDashboardInteractiveSelection(
        dashboardId,
        blockId,
    );

    if (
        storedSelection &&
        storedSelection.query === query &&
        availableValues.includes(storedSelection.selected)
    ) {
        return storedSelection.selected;
    }

    return firstAvailableValue;
};

const resolveDashboardInteractiveSelection = ({
    dashboardId,
    blockId,
    query,
    availableValues,
}: {
    dashboardId: number;
    blockId: number;
    query: string;
    availableValues: string[];
}) => {
    const nextSelectedValue = getResolvedDashboardInteractiveSelection({
        dashboardId,
        blockId,
        query,
        availableValues,
    });

    if (!availableValues.length) {
        removeDashboardInteractiveSelection(dashboardId, blockId);
        return "";
    }

    const storedSelection = readDashboardInteractiveSelection(
        dashboardId,
        blockId,
    );

    if (
        storedSelection &&
        storedSelection.query === query &&
        storedSelection.selected === nextSelectedValue
    ) {
        return nextSelectedValue;
    }

    removeDashboardInteractiveSelection(dashboardId, blockId);

    if (nextSelectedValue) {
        writeDashboardInteractiveSelection(dashboardId, blockId, {
            query,
            selected: nextSelectedValue,
        });
    }

    return nextSelectedValue;
};

export const syncDashboardInteractiveSelection = ({
    dashboardId,
    blockId,
    query,
    availableValues,
}: {
    dashboardId: number;
    blockId: number;
    query: string;
    availableValues: string[];
}) =>
    resolveDashboardInteractiveSelection({
        dashboardId,
        blockId,
        query,
        availableValues,
    });

export const cleanupDashboardInteractiveSelections = ({
    dashboardId,
    blocks,
}: {
    dashboardId: number;
    blocks: DashboardBlockSchema[];
}) => {
    if (!isBrowser()) {
        return;
    }

    const interactiveBlocks = new Map(
        blocks
            .filter(isDashboardInteractiveBlock)
            .map((block) => [block.id, block.query]),
    );

    Object.keys(window.localStorage)
        .filter((key) =>
            key.startsWith(
                `${DASHBOARD_INTERACTIVE_SELECTION_STORAGE_PREFIX}:${dashboardId}:`,
            ),
        )
        .forEach((key) => {
            const blockId = Number(key.split(":").pop());

            if (!Number.isFinite(blockId)) {
                window.localStorage.removeItem(key);
                return;
            }

            const blockQuery = interactiveBlocks.get(blockId);

            if (!blockQuery) {
                window.localStorage.removeItem(key);
                return;
            }

            const payload = parseSelectionPayload(
                window.localStorage.getItem(key),
            );

            if (!payload || payload.query !== blockQuery) {
                window.localStorage.removeItem(key);
            }
        });
};

export const useDashboardInteractiveSelection = ({
    dashboardId,
    blockId,
    query,
    availableValues,
}: {
    dashboardId: number;
    blockId: number;
    query: string;
    availableValues: string[];
}) => {
    const availableValuesKey = React.useMemo(
        () => availableValues.join("\u0000"),
        [availableValues],
    );
    const availableValueSet = React.useMemo(
        () => new Set(availableValues),
        [availableValuesKey],
    );
    const [selectedValue, setSelectedValue] = React.useState(() =>
        getResolvedDashboardInteractiveSelection({
            dashboardId,
            blockId,
            query,
            availableValues,
        }),
    );

    React.useEffect(() => {
        const nextSelectedValue = resolveDashboardInteractiveSelection({
            dashboardId,
            blockId,
            query,
            availableValues,
        });

        setSelectedValue((currentValue) =>
            currentValue === nextSelectedValue
                ? currentValue
                : nextSelectedValue,
        );
    }, [availableValuesKey, blockId, dashboardId, query]);

    const handleSetSelectedValue = React.useCallback(
        (nextValue: string) => {
            if (!availableValueSet.has(nextValue)) {
                const fallbackValue = resolveDashboardInteractiveSelection({
                    dashboardId,
                    blockId,
                    query,
                    availableValues,
                });

                setSelectedValue(fallbackValue);
                return;
            }

            setSelectedValue(nextValue);
            writeDashboardInteractiveSelection(dashboardId, blockId, {
                query,
                selected: nextValue,
            });
        },
        [availableValues, availableValueSet, blockId, dashboardId, query],
    );

    return {
        selectedValue,
        setSelectedValue: handleSetSelectedValue,
    };
};
