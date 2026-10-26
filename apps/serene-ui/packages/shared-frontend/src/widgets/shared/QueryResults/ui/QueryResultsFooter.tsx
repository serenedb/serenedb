import { DownloadResultsButton } from "@serene-ui/shared-frontend/features";
import {
    ArrowDownIcon,
    Button,
    Input,
    Popover,
    PopoverContent,
    PopoverTrigger,
    Tabs,
    TabsList,
    TabsTrigger,
    TreeColumnsIcon,
    cn,
} from "@serene-ui/shared-frontend/shared";
import { useContext, useEffect, useMemo, useState } from "react";
import { TimelineCard, type TimelineItem } from "../../TimelineCard";
import { ConsoleContext } from "../../../console/Console/model/ConsoleContext";

interface QueryResultsFooterProps {
    children: React.ReactNode;
    results: {
        jobId?: number;
        status: "success" | "failed" | "pending" | "running" | "";
        statementIndex?: number;
        statementQuery?: string;
    }[];
    selectedResultIndex: number;
    onSelectResult?: (index: number) => void;
    rows?: Record<string, unknown>[];
    created_at?: string;
    execution_started_at?: string;
    execution_finished_at?: string;
    received_at?: string;
    showJsonByDefault?: boolean;
    viewerLabel?: string;
    sourcePanelId?: string;
}

const getExecutionHistoryEntryId = (
    panelId: string,
    result: Pick<
        QueryResultsFooterProps["results"][number],
        "jobId" | "statementIndex"
    >,
) => {
    if (result.jobId == null) {
        return null;
    }

    return `${panelId}:${result.jobId}:${result.statementIndex ?? -1}`;
};

export const QueryResultsFooter: React.FC<QueryResultsFooterProps> = ({
    children,
    results,
    selectedResultIndex,
    onSelectResult,
    rows,
    created_at,
    execution_started_at,
    execution_finished_at,
    received_at,
    showJsonByDefault = false,
    viewerLabel = "Viewer",
    sourcePanelId,
}) => {
    const [isPopoverOpen, setIsPopoverOpen] = useState(false);
    const [searchValue, setSearchValue] = useState("");
    const [viewMode, setViewMode] = useState<"viewer" | "json">(
        showJsonByDefault ? "json" : "viewer",
    );
    const queueTime =
        created_at && execution_started_at
            ? new Date(execution_started_at).getTime() -
              new Date(created_at).getTime()
            : 0;
    const execTime =
        execution_started_at && execution_finished_at
            ? new Date(execution_finished_at).getTime() -
              new Date(execution_started_at).getTime()
            : 0;
    const transferTime =
        execution_finished_at && received_at
            ? new Date(received_at).getTime() -
              new Date(execution_finished_at).getTime()
            : 0;

    const executionTime = execTime > 0 ? execTime : null;
    const timelineItems: TimelineItem[] = [
        { name: "Queue", time: queueTime, color: "rgb(234, 179, 8)" },
        { name: "Execution", time: execTime, color: "rgb(34, 197, 94)" },
        { name: "Transfer", time: transferTime, color: "rgb(59, 130, 246)" },
    ];
    const showViewModes = Boolean(rows?.length);
    const consoleContext = useContext(ConsoleContext);
    const canOpenExecutionHistorySidebar = Boolean(
        consoleContext?.openExecutionHistorySidebar && sourcePanelId,
    );
    const visibleResults = useMemo(() => {
        const indexedResults = results.map((result, index) => ({
            result,
            index,
        }));

        if (!sourcePanelId || !consoleContext?.executionHistoryEntries) {
            return indexedResults.map((item, position) => ({
                ...item,
                position,
            }));
        }

        const historyEntryIds = new Set(
            consoleContext.executionHistoryEntries
                .filter((entry) => entry.panelId === sourcePanelId)
                .map(
                    (entry) =>
                        `${entry.panelId}:${entry.jobId}:${entry.statementIndex ?? -1}`,
                ),
        );
        const resultsFromHistory = indexedResults.filter(({ result }) => {
            const entryId = getExecutionHistoryEntryId(sourcePanelId, result);
            return entryId ? historyEntryIds.has(entryId) : false;
        });
        const lastResult = indexedResults[indexedResults.length - 1];

        if (!resultsFromHistory.length) {
            return lastResult ? [{ ...lastResult, position: 0 }] : [];
        }

        if (!lastResult) {
            return resultsFromHistory.map((item, position) => ({
                ...item,
                position,
            }));
        }

        const lastResultEntryId = getExecutionHistoryEntryId(
            sourcePanelId,
            lastResult.result,
        );
        const includesLastResult = lastResultEntryId
            ? resultsFromHistory.some(
                  ({ result }) =>
                      getExecutionHistoryEntryId(sourcePanelId, result) ===
                      lastResultEntryId,
              )
            : resultsFromHistory.some(
                  ({ index }) => index === lastResult.index,
              );
        const nextVisibleResults = includesLastResult
            ? resultsFromHistory
            : [...resultsFromHistory, lastResult];

        return nextVisibleResults.map((item, position) => ({
            ...item,
            position,
        }));
    }, [consoleContext?.executionHistoryEntries, results, sourcePanelId]);

    useEffect(() => {
        setViewMode(showJsonByDefault ? "json" : "viewer");
    }, [showJsonByDefault]);

    useEffect(() => {
        if (!visibleResults.length || !onSelectResult) {
            return;
        }

        const isSelectedResultVisible = visibleResults.some(
            ({ index }) => index === selectedResultIndex,
        );

        if (isSelectedResultVisible) {
            return;
        }

        const fallbackResultIndex =
            visibleResults[visibleResults.length - 1]?.index;

        if (
            fallbackResultIndex != null &&
            fallbackResultIndex !== selectedResultIndex
        ) {
            onSelectResult(fallbackResultIndex);
        }
    }, [onSelectResult, selectedResultIndex, visibleResults]);

    const filteredResults = useMemo(() => {
        const normalizedSearch = searchValue.trim().toLowerCase();
        if (!normalizedSearch) {
            return visibleResults;
        }

        return visibleResults.filter(({ result }) =>
            (result.statementQuery || "")
                .toLowerCase()
                .includes(normalizedSearch),
        );
    }, [searchValue, visibleResults]);

    const selectedVisibleResultIndex = visibleResults.findIndex(
        ({ index }) => index === selectedResultIndex,
    );
    const activeVisibleResultIndex =
        selectedVisibleResultIndex >= 0
            ? selectedVisibleResultIndex
            : Math.max(0, visibleResults.length - 1);
    const activeVisibleResult =
        visibleResults[activeVisibleResultIndex]?.result ||
        results[selectedResultIndex];
    const canGoPrevious = activeVisibleResultIndex > 0;
    const canGoNext = activeVisibleResultIndex < visibleResults.length - 1;
    const showResultNavigation =
        visibleResults.length > 1 || canOpenExecutionHistorySidebar;

    const getResultButtonClassName = (
        status: QueryResultsFooterProps["results"][number]["status"],
        isActive: boolean,
    ) => {
        if (isActive) {
            return "bg-transparent hover:bg-accent";
        }

        if (status === "failed") {
            return "border-destructive/30 text-destructive hover:bg-destructive/10";
        }

        if (status === "pending" || status === "running") {
            return "border-amber-500/30 text-amber-500 hover:bg-amber-500/10";
        }

        if (status === "success") {
            return "border-emerald-500/30 text-emerald-500 hover:bg-emerald-500/10";
        }

        return "";
    };

    const getStatusBadgeClassName = (
        status: QueryResultsFooterProps["results"][number]["status"],
    ) => {
        if (status === "failed") {
            return "border-destructive/30 bg-destructive/10 text-destructive";
        }

        if (status === "pending" || status === "running") {
            return "border-amber-500/30 bg-amber-500/10 text-amber-500";
        }

        if (status === "success") {
            return "border-emerald-500/30 bg-emerald-500/10 text-emerald-500";
        }

        return "border-border text-muted-foreground";
    };

    const getShortQuery = (query?: string) => {
        if (!query) {
            return "No query";
        }

        const normalized = query.replace(/\s+/g, " ").trim();
        if (normalized.length <= 18) {
            return normalized;
        }

        return `${normalized.slice(0, 15)}...`;
    };

    return (
        <Tabs
            value={viewMode}
            onValueChange={(value) => {
                setViewMode(value as "viewer" | "json");
            }}
            className="h-full flex flex-col min-h-0">
            <div className="flex-1 min-h-0">{children}</div>
            <TabsList className="mt-0 h-max px-0">
                <div className="flex w-full border-t-[0.5px] border-border justify-between">
                    <div className="flex items-center">
                        <div className="flex items-center px-3 border-r-[0.5px] h-full">
                            <TreeColumnsIcon />
                            <p className="text-xs ml-2 text-foreground">
                                {rows?.length || 0}{" "}
                                {rows?.length === 1 ? "element" : "elements"}
                            </p>
                        </div>
                        <TimelineCard
                            title="Query Execution Timeline"
                            items={timelineItems}
                            displayTime={executionTime}
                            disabled={true}
                        />
                    </div>
                    <div className="flex">
                        {showViewModes ? (
                            <div>
                                {viewMode === "viewer" ? (
                                    <TabsTrigger
                                        key="to-json"
                                        className="dark:bg-transparent dark:hover:bg-accent duration-300 px-3 text-xs w-max rounded-none h-full border-0 border-l-[0.5px] border-border"
                                        value="json">
                                        JSON
                                    </TabsTrigger>
                                ) : (
                                    <TabsTrigger
                                        key="to-viewer"
                                        className="dark:bg-transparent dark:hover:bg-accent duration-300 px-3 text-xs w-max rounded-none h-full border-0 border-l-[0.5px] border-border"
                                        value="viewer">
                                        {viewerLabel}
                                    </TabsTrigger>
                                )}
                            </div>
                        ) : (
                            <div />
                        )}

                        <DownloadResultsButton rows={rows} />
                        {showResultNavigation && (
                            <div className="flex items-center h-full">
                                <Button
                                    variant="ghost"
                                    className="rounded-none h-full border-l-[0.5px] w-9 "
                                    size="icon"
                                    disabled={!canGoPrevious}
                                    onClick={() => {
                                        if (canGoPrevious) {
                                            onSelectResult?.(
                                                visibleResults[
                                                    activeVisibleResultIndex - 1
                                                ]!.index,
                                            );
                                        }
                                    }}>
                                    <ArrowDownIcon className="rotate-90" />
                                </Button>
                                {canOpenExecutionHistorySidebar ? (
                                    <Button
                                        variant="ghost"
                                        size="small"
                                        className={cn(
                                            "min-w-18 px-2 rounded-none h-full border-l-[0.5px] w-9 border-r-[0.5px]",
                                            getResultButtonClassName(
                                                activeVisibleResult?.status ||
                                                    "",
                                                true,
                                            ),
                                        )}
                                        onClick={() => {
                                            if (!sourcePanelId) {
                                                return;
                                            }

                                            consoleContext?.openExecutionHistorySidebar(
                                                {
                                                    tab: "history",
                                                    panelId: sourcePanelId,
                                                },
                                            );
                                        }}>
                                        {activeVisibleResultIndex + 1} /{" "}
                                        {visibleResults.length}
                                    </Button>
                                ) : (
                                    <Popover
                                        open={isPopoverOpen}
                                        onOpenChange={(open) => {
                                            setIsPopoverOpen(open);
                                            if (!open) {
                                                setSearchValue("");
                                            }
                                        }}>
                                        <PopoverTrigger asChild>
                                            <Button
                                                variant="ghost"
                                                size="small"
                                                className={cn(
                                                    "min-w-18 px-2 rounded-none h-full border-l-[0.5px] w-9 border-r-[0.5px]",
                                                    getResultButtonClassName(
                                                        activeVisibleResult?.status ||
                                                            "",
                                                        true,
                                                    ),
                                                )}>
                                                {activeVisibleResultIndex + 1} /{" "}
                                                {visibleResults.length}
                                            </Button>
                                        </PopoverTrigger>
                                        <PopoverContent
                                            align="start"
                                            className="p-1"
                                            style={{ width: "20rem" }}>
                                            <div className="flex flex-col gap-1">
                                                <Input
                                                    value={searchValue}
                                                    onChange={(event) => {
                                                        setSearchValue(
                                                            event.target.value,
                                                        );
                                                    }}
                                                    placeholder="Search query"
                                                    className="h-8"
                                                />
                                                <div className="max-h-72 overflow-y-auto">
                                                    <div className="flex flex-col gap-1">
                                                        {filteredResults.length ===
                                                        0 ? (
                                                            <div className="px-2 py-3 text-sm text-muted-foreground">
                                                                No executions
                                                                found
                                                            </div>
                                                        ) : (
                                                            filteredResults.map(
                                                                ({
                                                                    result,
                                                                    index,
                                                                    position,
                                                                }) => (
                                                                    <button
                                                                        key={`${result.status}-${index}`}
                                                                        type="button"
                                                                        className={cn(
                                                                            "flex w-full items-center justify-between gap-2 rounded-md px-2 py-1.5 text-left text-sm hover:bg-accent hover:text-accent-foreground",
                                                                            position ===
                                                                                activeVisibleResultIndex &&
                                                                                "bg-accent text-accent-foreground",
                                                                        )}
                                                                        onClick={() => {
                                                                            onSelectResult?.(
                                                                                index,
                                                                            );
                                                                            setIsPopoverOpen(
                                                                                false,
                                                                            );
                                                                        }}>
                                                                        <div className="flex min-w-0 items-center gap-2">
                                                                            <span className="w-6 shrink-0 text-xs text-muted-foreground">
                                                                                {position +
                                                                                    1}
                                                                            </span>
                                                                            <span className="truncate">
                                                                                {getShortQuery(
                                                                                    result.statementQuery,
                                                                                )}
                                                                            </span>
                                                                        </div>
                                                                        <span
                                                                            className={cn(
                                                                                "shrink-0 rounded border px-1.5 py-0.5 text-[10px] uppercase tracking-wide",
                                                                                getStatusBadgeClassName(
                                                                                    result.status,
                                                                                ),
                                                                            )}>
                                                                            {result.status ||
                                                                                "idle"}
                                                                        </span>
                                                                    </button>
                                                                ),
                                                            )
                                                        )}
                                                    </div>
                                                </div>
                                            </div>
                                        </PopoverContent>
                                    </Popover>
                                )}
                                <Button
                                    className="rounded-none h-full w-9"
                                    variant="ghost"
                                    size="icon"
                                    disabled={!canGoNext}
                                    onClick={() => {
                                        if (canGoNext) {
                                            onSelectResult?.(
                                                visibleResults[
                                                    activeVisibleResultIndex + 1
                                                ]!.index,
                                            );
                                        }
                                    }}>
                                    <ArrowDownIcon className="rotate-[-90deg]" />
                                </Button>
                            </div>
                        )}
                    </div>
                </div>
            </TabsList>
        </Tabs>
    );
};
