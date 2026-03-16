import React from "react";
import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import {
    Button,
    Checkbox,
    CrossIcon,
    Input,
    Label,
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
    Textarea,
} from "@serene-ui/shared-frontend";
import {
    toDashboardCardUpdateInput,
    useUpdateDashboardCard,
} from "../../../../entities/dashboard-card";
import { getErrorMessage } from "../../../../shared";
import { DashboardSelectChartParams } from "./DashboardSelectChartParams";
import { Separator } from "@serene-ui/shared-frontend";
import { PGSQLEditor } from "../../../shared/PGSQLEditor";
import { toast } from "sonner";

const DASHBOARD_CARD_EDITOR_AUTOSAVE_DELAY_MS = 500;
const DASHBOARD_CARD_EDITOR_QUERY_DEBOUNCE_MS = 2000;

const CARD_TYPE_OPTIONS: Array<{
    value: DashboardBlockSchema["type"];
    label: string;
}> = [
    { value: "text", label: "Text" },
    { value: "spacer", label: "Spacer" },
    { value: "table", label: "Table" },
    { value: "single_string", label: "Single string" },
    { value: "bar_chart", label: "Bar chart" },
    { value: "line_chart", label: "Line chart" },
    { value: "pie_chart", label: "Pie chart" },
];

const BAR_CHART_VARIANT_OPTIONS = [
    { value: "interactive", label: "Interactive" },
    { value: "vertical", label: "Vertical" },
    { value: "horizontal", label: "Horizontal" },
] as const;

const PIE_CHART_VARIANT_OPTIONS = [
    { value: "interactive", label: "Interactive" },
    { value: "pie", label: "Pie" },
    { value: "donut", label: "Donut" },
] as const;

const isDashboardQueryBlock = (
    block: DashboardBlockSchema,
): block is Extract<
    DashboardBlockSchema,
    | { type: "table" }
    | { type: "single_string" }
    | { type: "bar_chart" }
    | { type: "line_chart" }
    | { type: "pie_chart" }
> => {
    return (
        block.type === "table" ||
        block.type === "single_string" ||
        block.type === "bar_chart" ||
        block.type === "line_chart" ||
        block.type === "pie_chart"
    );
};

const isDashboardChartBlock = (
    block: DashboardBlockSchema,
): block is Extract<
    DashboardBlockSchema,
    { type: "bar_chart" | "line_chart" | "pie_chart" }
> => {
    return (
        block.type === "bar_chart" ||
        block.type === "line_chart" ||
        block.type === "pie_chart"
    );
};

const getQueryBlockBase = (block: DashboardBlockSchema) => ({
    connection_id: isDashboardQueryBlock(block)
        ? block.connection_id
        : undefined,
    database: isDashboardQueryBlock(block) ? block.database : undefined,
    query: isDashboardQueryBlock(block) ? block.query : "select 1",
    name: isDashboardQueryBlock(block) ? block.name : "No name",
    description: isDashboardQueryBlock(block) ? block.description : undefined,
    custom_refresh_interval_enabled: isDashboardQueryBlock(block)
        ? block.custom_refresh_interval_enabled
        : false,
    custom_refresh_interval: isDashboardQueryBlock(block)
        ? block.custom_refresh_interval
        : 60,
    custom_row_limit_enabled: isDashboardQueryBlock(block)
        ? block.custom_row_limit_enabled
        : false,
    custom_row_limit: isDashboardQueryBlock(block)
        ? block.custom_row_limit
        : 1000,
});

const replaceDashboardBlockType = (
    block: DashboardBlockSchema,
    nextType: DashboardBlockSchema["type"],
): DashboardBlockSchema => {
    if (block.type === nextType) {
        return block;
    }

    const baseBlock = {
        id: block.id,
        dashboard_id: block.dashboard_id,
        bounds: block.bounds,
    };

    switch (nextType) {
        case "text":
            return {
                ...baseBlock,
                type: "text",
                text: block.type === "text" ? block.text : "",
            };
        case "spacer":
            return {
                ...baseBlock,
                type: "spacer",
            };
        case "table":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "table",
                columns: block.type === "table" ? block.columns : [],
            };
        case "single_string":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "single_string",
                column: block.type === "single_string" ? block.column : "value",
                fallback_value:
                    block.type === "single_string"
                        ? block.fallback_value
                        : undefined,
            };
        case "bar_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "bar_chart",
                variant:
                    block.type === "bar_chart" ? block.variant : "vertical",
                category_key:
                    block.type === "bar_chart"
                        ? block.category_key
                        : "category",
                default_active_key:
                    block.type === "bar_chart"
                        ? block.default_active_key
                        : undefined,
                value_label:
                    block.type === "bar_chart" ? block.value_label : "Value",
                is_stacked:
                    block.type === "bar_chart" ? block.is_stacked : false,
                series:
                    block.type === "bar_chart"
                        ? block.series
                        : [
                              {
                                  key: "value",
                                  label: "Value",
                                  color: "var(--chart-1)",
                              },
                          ],
            };
        case "line_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "line_chart",
                variant:
                    block.type === "line_chart" ? block.variant : "default",
                x_axis_key:
                    block.type === "line_chart" ? block.x_axis_key : "category",
                default_active_key:
                    block.type === "line_chart"
                        ? block.default_active_key
                        : undefined,
                value_label:
                    block.type === "line_chart" ? block.value_label : "Value",
                line_type:
                    block.type === "line_chart" ? block.line_type : "natural",
                series:
                    block.type === "line_chart"
                        ? block.series
                        : [
                              {
                                  key: "value",
                                  label: "Value",
                                  color: "var(--chart-1)",
                              },
                          ],
            };
        case "pie_chart":
            return {
                ...baseBlock,
                ...getQueryBlockBase(block),
                type: "pie_chart",
                interactive:
                    block.type === "pie_chart" ? block.interactive : false,
                variant: block.type === "pie_chart" ? block.variant : "pie",
                name_key: block.type === "pie_chart" ? block.name_key : "name",
                value_key:
                    block.type === "pie_chart" ? block.value_key : "value",
                default_active_key:
                    block.type === "pie_chart"
                        ? block.default_active_key
                        : undefined,
                value_label:
                    block.type === "pie_chart" ? block.value_label : "Value",
                color_key:
                    block.type === "pie_chart" ? block.color_key : "fill",
                show_labels:
                    block.type === "pie_chart" ? block.show_labels : false,
                show_center_label:
                    block.type === "pie_chart"
                        ? block.show_center_label
                        : false,
                center_value:
                    block.type === "pie_chart" ? block.center_value : undefined,
                center_label:
                    block.type === "pie_chart" ? block.center_label : undefined,
            };
    }
};

interface DashboardCardEditorProps {
    editedBlock?: DashboardBlockSchema | null;
    onClose?: () => void;
    onEditedBlockChange?: (block: DashboardBlockSchema | null) => void;
}

export const DashboardCardEditor: React.FC<DashboardCardEditorProps> = ({
    editedBlock,
    onClose,
    onEditedBlockChange,
}) => {
    const { mutateAsync: updateDashboardCard } = useUpdateDashboardCard();
    const latestEditedBlockRef = React.useRef<DashboardBlockSchema | null>(
        null,
    );
    const previousEditedBlockIdRef = React.useRef<number | null>(null);
    const pendingQueryBlockRef = React.useRef<Extract<
        DashboardBlockSchema,
        | { type: "table" }
        | { type: "single_string" }
        | { type: "bar_chart" }
        | { type: "line_chart" }
        | { type: "pie_chart" }
    > | null>(null);
    const currentBlockIdRef = React.useRef<number | null>(null);
    const lastPersistedRef = React.useRef<string | null>(null);
    const pendingBlockRef = React.useRef<DashboardBlockSchema | null>(null);
    const isPersistingRef = React.useRef(false);
    const [queryDraft, setQueryDraft] = React.useState<{
        blockId: number | null;
        value: string;
    }>({
        blockId: null,
        value: "",
    });

    React.useEffect(() => {
        latestEditedBlockRef.current = editedBlock ?? null;

        if (
            editedBlock &&
            isDashboardQueryBlock(editedBlock) &&
            queryDraft.blockId === editedBlock.id
        ) {
            pendingQueryBlockRef.current = editedBlock;
        }
    }, [editedBlock, queryDraft.blockId]);

    const flushPendingQuery = React.useCallback(() => {
        if (queryDraft.blockId === null) {
            return false;
        }

        const latestBlock = latestEditedBlockRef.current;
        const sourceBlock =
            latestBlock &&
            isDashboardQueryBlock(latestBlock) &&
            latestBlock.id === queryDraft.blockId
                ? latestBlock
                : pendingQueryBlockRef.current;

        if (!sourceBlock || sourceBlock.id !== queryDraft.blockId) {
            return false;
        }

        if (sourceBlock.query === queryDraft.value) {
            return false;
        }

        onEditedBlockChange?.({
            ...sourceBlock,
            query: queryDraft.value,
        });

        return true;
    }, [onEditedBlockChange, queryDraft]);

    React.useEffect(() => {
        const nextBlockId = editedBlock?.id ?? null;

        if (
            previousEditedBlockIdRef.current !== null &&
            previousEditedBlockIdRef.current !== nextBlockId
        ) {
            flushPendingQuery();
        }

        previousEditedBlockIdRef.current = nextBlockId;
    }, [editedBlock?.id, flushPendingQuery]);

    React.useEffect(() => {
        if (editedBlock && isDashboardQueryBlock(editedBlock)) {
            setQueryDraft((currentState) => {
                if (
                    currentState.blockId === editedBlock.id &&
                    currentState.value === editedBlock.query
                ) {
                    return currentState;
                }

                return {
                    blockId: editedBlock.id,
                    value: editedBlock.query,
                };
            });

            return;
        }

        setQueryDraft((currentState) => {
            if (currentState.blockId === null && currentState.value === "") {
                return currentState;
            }

            return {
                blockId: null,
                value: "",
            };
        });
    }, [
        editedBlock?.id,
        editedBlock?.type,
        editedBlock && isDashboardQueryBlock(editedBlock)
            ? editedBlock.query
            : undefined,
    ]);

    React.useEffect(() => {
        if (
            !editedBlock ||
            !isDashboardQueryBlock(editedBlock) ||
            queryDraft.blockId !== editedBlock.id ||
            queryDraft.value === editedBlock.query
        ) {
            return;
        }

        const timeoutId = window.setTimeout(() => {
            flushPendingQuery();
        }, DASHBOARD_CARD_EDITOR_QUERY_DEBOUNCE_MS);

        return () => {
            window.clearTimeout(timeoutId);
        };
    }, [editedBlock, flushPendingQuery, queryDraft]);

    const displayEditedBlock = React.useMemo(() => {
        if (
            !editedBlock ||
            !isDashboardQueryBlock(editedBlock) ||
            queryDraft.blockId !== editedBlock.id
        ) {
            return editedBlock;
        }

        if (editedBlock.query === queryDraft.value) {
            return editedBlock;
        }

        return {
            ...editedBlock,
            query: queryDraft.value,
        };
    }, [editedBlock, queryDraft]);

    const getPersistKey = React.useCallback((block: DashboardBlockSchema) => {
        return `${block.id}:${JSON.stringify(toDashboardCardUpdateInput(block))}`;
    }, []);

    const flushPersist = React.useCallback(async () => {
        const nextBlock = pendingBlockRef.current;

        if (!nextBlock || nextBlock.id < 0 || isPersistingRef.current) {
            return;
        }

        const persistKey = getPersistKey(nextBlock);

        if (persistKey === lastPersistedRef.current) {
            pendingBlockRef.current = null;
            return;
        }

        isPersistingRef.current = true;

        try {
            await updateDashboardCard({
                dashboardId: nextBlock.dashboard_id,
                card: {
                    ...toDashboardCardUpdateInput(nextBlock),
                    id: nextBlock.id,
                },
            });

            lastPersistedRef.current = persistKey;
        } catch (error) {
            toast.error("Failed to update dashboard card", {
                description: getErrorMessage(
                    error,
                    "Failed to update dashboard card.",
                ),
            });
        } finally {
            isPersistingRef.current = false;

            if (!pendingBlockRef.current) {
                return;
            }

            const pendingPersistKey = getPersistKey(pendingBlockRef.current);

            if (pendingPersistKey === lastPersistedRef.current) {
                pendingBlockRef.current = null;
                return;
            }

            if (pendingPersistKey !== persistKey) {
                void flushPersist();
            }
        }
    }, [getPersistKey, updateDashboardCard]);

    React.useEffect(() => {
        if (!editedBlock) {
            currentBlockIdRef.current = null;
            lastPersistedRef.current = null;
            pendingBlockRef.current = null;
            return;
        }

        if (currentBlockIdRef.current === editedBlock.id) {
            return;
        }

        currentBlockIdRef.current = editedBlock.id;
        lastPersistedRef.current = getPersistKey(editedBlock);
        pendingBlockRef.current = null;
    }, [editedBlock, getPersistKey]);

    React.useEffect(() => {
        if (!editedBlock || editedBlock.id < 0) {
            return;
        }

        const persistKey = getPersistKey(editedBlock);

        if (persistKey === lastPersistedRef.current) {
            return;
        }

        pendingBlockRef.current = editedBlock;

        const timeoutId = window.setTimeout(() => {
            void flushPersist();
        }, DASHBOARD_CARD_EDITOR_AUTOSAVE_DELAY_MS);

        return () => {
            window.clearTimeout(timeoutId);
        };
    }, [editedBlock, flushPersist, getPersistKey]);

    const pieChartVariantValue =
        displayEditedBlock?.type === "pie_chart"
            ? displayEditedBlock.interactive
                ? "interactive"
                : displayEditedBlock.variant
            : undefined;

    const handleClose = React.useCallback(() => {
        const hasFlushedPendingQuery = flushPendingQuery();

        if (hasFlushedPendingQuery) {
            window.setTimeout(() => {
                onClose?.();
            }, 0);

            return;
        }

        onClose?.();
    }, [flushPendingQuery, onClose]);

    return (
        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
            <div className="flex h-12 items-center justify-between border-b-1 bg-background pl-4 pr-2">
                <p className="text-primary-foreground uppercase text-xs font-extrabold">
                    {editedBlock ? "Edit card" : "Card editor"}
                </p>
                <Button
                    size="icon"
                    variant="secondary"
                    title="Close editor"
                    onClick={handleClose}>
                    <CrossIcon className="size-3" />
                </Button>
            </div>
            <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-4 overflow-x-hidden overflow-y-auto pt-4 pb-4">
                <div className="flex flex-col gap-2 px-4">
                    <Label htmlFor="dashboard-card-type">Type</Label>
                    <Select
                        value={displayEditedBlock?.type}
                        onValueChange={(value) => {
                            if (!displayEditedBlock) {
                                return;
                            }

                            onEditedBlockChange?.(
                                replaceDashboardBlockType(
                                    displayEditedBlock,
                                    value as DashboardBlockSchema["type"],
                                ),
                            );
                        }}>
                        <SelectTrigger
                            id="dashboard-card-type"
                            className="w-full"
                            aria-label="Card type"
                            disabled={!editedBlock}>
                            <SelectValue placeholder="Select card type" />
                        </SelectTrigger>
                        <SelectContent>
                            {CARD_TYPE_OPTIONS.map((item) => (
                                <SelectItem key={item.value} value={item.value}>
                                    {item.label}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </div>
                {displayEditedBlock?.type === "bar_chart" ? (
                    <div className="flex flex-col gap-2 px-4">
                        <Label htmlFor="dashboard-card-variant">Variant</Label>
                        <Select
                            value={displayEditedBlock.variant}
                            onValueChange={(value) => {
                                onEditedBlockChange?.({
                                    ...displayEditedBlock,
                                    variant:
                                        value as (typeof BAR_CHART_VARIANT_OPTIONS)[number]["value"],
                                });
                            }}>
                            <SelectTrigger
                                id="dashboard-card-variant"
                                className="w-full"
                                aria-label="Bar chart variant">
                                <SelectValue placeholder="Select variant" />
                            </SelectTrigger>
                            <SelectContent>
                                {BAR_CHART_VARIANT_OPTIONS.map((item) => (
                                    <SelectItem
                                        key={item.value}
                                        value={item.value}>
                                        {item.label}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>
                ) : null}
                <Separator />
                {displayEditedBlock?.type === "pie_chart" ? (
                    <div className="flex flex-col gap-2 px-4">
                        <Label htmlFor="dashboard-card-variant">Variant</Label>
                        <Select
                            value={pieChartVariantValue}
                            onValueChange={(value) => {
                                onEditedBlockChange?.({
                                    ...displayEditedBlock,
                                    interactive: value === "interactive",
                                    variant:
                                        value === "interactive"
                                            ? displayEditedBlock.variant
                                            : (value as "pie" | "donut"),
                                });
                            }}>
                            <SelectTrigger
                                id="dashboard-card-variant"
                                className="w-full"
                                aria-label="Pie chart variant">
                                <SelectValue placeholder="Select variant" />
                            </SelectTrigger>
                            <SelectContent>
                                {PIE_CHART_VARIANT_OPTIONS.map((item) => (
                                    <SelectItem
                                        key={item.value}
                                        value={item.value}>
                                        {item.label}
                                    </SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>
                ) : null}

                {displayEditedBlock &&
                isDashboardQueryBlock(displayEditedBlock) ? (
                    <>
                        <div className="flex flex-col gap-2 px-4">
                            <Label htmlFor="dashboard-card-title">Title</Label>
                            <Input
                                id="dashboard-card-title"
                                value={displayEditedBlock.name ?? ""}
                                placeholder="No name"
                                onChange={(event) => {
                                    onEditedBlockChange?.({
                                        ...displayEditedBlock,
                                        name: event.target.value,
                                    });
                                }}
                            />
                        </div>
                        <div className="flex flex-col gap-2 px-4">
                            <Label htmlFor="dashboard-card-description">
                                Description
                            </Label>
                            <Textarea
                                id="dashboard-card-description"
                                value={displayEditedBlock.description ?? ""}
                                placeholder="Add description"
                                className="min-h-24 resize-none"
                                onChange={(event) => {
                                    onEditedBlockChange?.({
                                        ...displayEditedBlock,
                                        description:
                                            event.target.value || undefined,
                                    });
                                }}
                            />
                        </div>
                        <div className="flex flex-col gap-2 px-4">
                            <Label>Query</Label>
                            <div className="h-64 overflow-hidden rounded-md border">
                                <PGSQLEditor
                                    value={queryDraft.value}
                                    onChange={(value) => {
                                        setQueryDraft({
                                            blockId: displayEditedBlock.id,
                                            value,
                                        });
                                    }}
                                />
                            </div>
                        </div>
                    </>
                ) : null}
                <Separator />
                {editedBlock && isDashboardChartBlock(editedBlock) ? (
                    <DashboardSelectChartParams
                        block={editedBlock}
                        onBlockChange={(block) => {
                            if (
                                queryDraft.blockId === block.id &&
                                queryDraft.value !== block.query
                            ) {
                                onEditedBlockChange?.({
                                    ...block,
                                    query: queryDraft.value,
                                });
                                return;
                            }

                            onEditedBlockChange?.(block);
                        }}
                    />
                ) : null}
                {displayEditedBlock &&
                isDashboardQueryBlock(displayEditedBlock) ? (
                    <>
                        <Separator />
                        <div className="flex flex-col gap-4 px-4">
                            <div className="flex flex-col gap-3">
                                <div className="flex items-start gap-3">
                                    <Checkbox
                                        id="dashboard-card-custom-refresh-interval"
                                        checked={
                                            displayEditedBlock.custom_refresh_interval_enabled
                                        }
                                        onCheckedChange={(checked) => {
                                            onEditedBlockChange?.({
                                                ...displayEditedBlock,
                                                custom_refresh_interval_enabled:
                                                    checked === true,
                                            });
                                        }}
                                    />
                                    <div className="flex min-w-0 flex-1 flex-col gap-1">
                                        <Label htmlFor="dashboard-card-custom-refresh-interval">
                                            Custom refresh interval
                                        </Label>
                                        <p className="text-xs text-muted-foreground">
                                            Override the dashboard auto-refresh
                                            interval for this card.
                                        </p>
                                    </div>
                                </div>
                                {displayEditedBlock.custom_refresh_interval_enabled ? (
                                    <div className="flex flex-col gap-2 pl-7">
                                        <Label htmlFor="dashboard-card-refresh-interval">
                                            Refresh interval (seconds)
                                        </Label>
                                        <Input
                                            id="dashboard-card-refresh-interval"
                                            type="number"
                                            min={1}
                                            step={1}
                                            value={String(
                                                displayEditedBlock.custom_refresh_interval,
                                            )}
                                            onChange={(event) => {
                                                const nextValue = Number(
                                                    event.target.value,
                                                );

                                                onEditedBlockChange?.({
                                                    ...displayEditedBlock,
                                                    custom_refresh_interval:
                                                        Number.isFinite(
                                                            nextValue,
                                                        ) && nextValue > 0
                                                            ? nextValue
                                                            : displayEditedBlock.custom_refresh_interval,
                                                });
                                            }}
                                        />
                                    </div>
                                ) : null}
                            </div>

                            <div className="flex flex-col gap-3">
                                <div className="flex items-start gap-3">
                                    <Checkbox
                                        id="dashboard-card-custom-row-limit"
                                        checked={
                                            displayEditedBlock.custom_row_limit_enabled
                                        }
                                        onCheckedChange={(checked) => {
                                            onEditedBlockChange?.({
                                                ...displayEditedBlock,
                                                custom_row_limit_enabled:
                                                    checked === true,
                                            });
                                        }}
                                    />
                                    <div className="flex min-w-0 flex-1 flex-col gap-1">
                                        <Label htmlFor="dashboard-card-custom-row-limit">
                                            Custom row limit
                                        </Label>
                                        <p className="text-xs text-muted-foreground">
                                            Override the dashboard row limit
                                            for this card.
                                        </p>
                                    </div>
                                </div>
                                {displayEditedBlock.custom_row_limit_enabled ? (
                                    <div className="flex flex-col gap-2 pl-7">
                                        <Label htmlFor="dashboard-card-row-limit">
                                            Row limit
                                        </Label>
                                        <Input
                                            id="dashboard-card-row-limit"
                                            type="number"
                                            min={1}
                                            step={1}
                                            value={String(
                                                displayEditedBlock.custom_row_limit,
                                            )}
                                            onChange={(event) => {
                                                const nextValue = Number(
                                                    event.target.value,
                                                );

                                                onEditedBlockChange?.({
                                                    ...displayEditedBlock,
                                                    custom_row_limit:
                                                        Number.isFinite(
                                                            nextValue,
                                                        ) && nextValue > 0
                                                            ? nextValue
                                                            : displayEditedBlock.custom_row_limit,
                                                });
                                            }}
                                        />
                                    </div>
                                ) : null}
                            </div>
                        </div>
                    </>
                ) : null}
            </div>
        </div>
    );
};
