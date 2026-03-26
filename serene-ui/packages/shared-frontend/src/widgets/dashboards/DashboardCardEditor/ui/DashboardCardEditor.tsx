import React from "react";
import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import {
    Button,
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
import { DashboardSelectChartParams } from "./DashboardSelectChartParams";
import { Separator } from "@serene-ui/shared-frontend";
import { PGSQLEditor } from "../../../shared/PGSQLEditor";
import { DashboardNumericOverrideControl } from "./components";
import {
    isDashboardChartBlock,
    isDashboardQueryBlock,
    replaceDashboardBlockType,
    useDashboardCardEditorState,
} from "../model";

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
    { value: "area_chart", label: "Area chart" },
    { value: "pie_chart", label: "Pie chart" },
];

const BAR_CHART_VARIANT_OPTIONS = [
    { value: "interactive", variant: "interactive", isStacked: false, label: "Interactive" },
    { value: "vertical", variant: "vertical", isStacked: false, label: "Vertical" },
    { value: "horizontal", variant: "horizontal", isStacked: false, label: "Horizontal" },
    {
        value: "vertical_stacked",
        variant: "vertical",
        isStacked: true,
        label: "Vertical Stacked",
    },
    {
        value: "horizontal_stacked",
        variant: "horizontal",
        isStacked: true,
        label: "Horizontal Stacked",
    },
] as const;

const PIE_CHART_VARIANT_OPTIONS = [
    { value: "interactive", label: "Interactive" },
    { value: "pie", label: "Pie" },
    { value: "donut", label: "Donut" },
] as const;

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
    const { displayEditedBlock, handleClose, queryDraft, setQueryDraft } =
        useDashboardCardEditorState({
            editedBlock,
            onClose,
            onEditedBlockChange,
        });

    const pieChartVariantValue =
        displayEditedBlock?.type === "pie_chart"
            ? displayEditedBlock.interactive
                ? "interactive"
                : displayEditedBlock.variant
            : undefined;
    const barChartVariantValue =
        displayEditedBlock?.type === "bar_chart"
            ? displayEditedBlock.is_stacked &&
              displayEditedBlock.variant !== "interactive"
                ? `${displayEditedBlock.variant}_stacked`
                : displayEditedBlock.variant
            : undefined;

    return (
        <div
            className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden"
            data-testid="dashboardCardEditor-root">
            <div className="flex h-12 items-center justify-between border-b-1 bg-background pl-4 pr-2">
                <p className="text-primary-foreground uppercase text-xs font-extrabold">
                    {editedBlock ? "Edit card" : "Card editor"}
                </p>
                <Button
                    size="icon"
                    variant="secondary"
                    title="Close editor"
                    data-testid="dashboardCardEditor-closeButton"
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
                            data-testid="dashboardCardEditor-typeSelect"
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
                            value={barChartVariantValue}
                            onValueChange={(value) => {
                                const selectedOption =
                                    BAR_CHART_VARIANT_OPTIONS.find(
                                        (item) => item.value === value,
                                    ) ??
                                    BAR_CHART_VARIANT_OPTIONS[0];

                                onEditedBlockChange?.({
                                    ...displayEditedBlock,
                                    variant: selectedOption.variant,
                                    is_stacked: selectedOption.isStacked,
                                });
                            }}>
                            <SelectTrigger
                                id="dashboard-card-variant"
                                className="w-full"
                                data-testid="dashboardCardEditor-barVariantSelect"
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
                                data-testid="dashboardCardEditor-pieVariantSelect"
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
                                data-testid="dashboardCardEditor-titleInput"
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
                                data-testid="dashboardCardEditor-descriptionInput"
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
                            <div
                                className="h-64 overflow-hidden rounded-md border"
                                data-testid="dashboardCardEditor-queryInput">
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
                            <DashboardNumericOverrideControl
                                checkboxId="dashboard-card-custom-refresh-interval"
                                inputId="dashboard-card-refresh-interval"
                                title="Custom refresh interval"
                                description="Override the dashboard auto-refresh interval for this card."
                                valueLabel="Refresh interval (seconds)"
                                enabled={
                                    displayEditedBlock.custom_refresh_interval_enabled
                                }
                                value={displayEditedBlock.custom_refresh_interval}
                                checkboxTestId="dashboardCardEditor-customRefreshCheckbox"
                                inputTestId="dashboardCardEditor-refreshIntervalInput"
                                onEnabledChange={(enabled) => {
                                    onEditedBlockChange?.({
                                        ...displayEditedBlock,
                                        custom_refresh_interval_enabled: enabled,
                                    });
                                }}
                                onValueChange={(value) => {
                                    onEditedBlockChange?.({
                                        ...displayEditedBlock,
                                        custom_refresh_interval: value,
                                    });
                                }}
                            />
                            <DashboardNumericOverrideControl
                                checkboxId="dashboard-card-custom-row-limit"
                                inputId="dashboard-card-row-limit"
                                title="Custom row limit"
                                description="Override the dashboard row limit for this card."
                                valueLabel="Row limit"
                                enabled={displayEditedBlock.custom_row_limit_enabled}
                                value={displayEditedBlock.custom_row_limit}
                                checkboxTestId="dashboardCardEditor-customRowLimitCheckbox"
                                inputTestId="dashboardCardEditor-rowLimitInput"
                                onEnabledChange={(enabled) => {
                                    onEditedBlockChange?.({
                                        ...displayEditedBlock,
                                        custom_row_limit_enabled: enabled,
                                    });
                                }}
                                onValueChange={(value) => {
                                    onEditedBlockChange?.({
                                        ...displayEditedBlock,
                                        custom_row_limit: value,
                                    });
                                }}
                            />
                        </div>
                    </>
                ) : null}
            </div>
        </div>
    );
};
