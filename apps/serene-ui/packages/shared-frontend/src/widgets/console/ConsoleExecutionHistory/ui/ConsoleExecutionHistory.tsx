import { useCallback, useEffect, useMemo, useState } from "react";
import type { DockviewIDisposable, DockviewApi } from "dockview";
import {
    ArrowDownIcon,
    Button,
    CheckIcon,
    ClearHistoryIcon,
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    Popover,
    PopoverContent,
    PopoverTrigger,
    SearchIcon,
    cn,
} from "@serene-ui/shared-frontend/shared";
import { useConsole } from "../../Console/model";
import {
    CONSOLE_RESULTS_PANEL_COMPONENT,
    createResultsPanelTitle,
    getResultsPanelId,
    normalizePanelParams,
    type ConsoleResult,
    type EditorPanelParams,
} from "../../ConsoleEditor/model";
import { ConsoleExecutionHistoryTopbar } from "./ConsoleExecutionHistoryTopbar";

const ALL_TABS_FILTER = "__all__";
const RESULTS_PANEL_SUFFIX = "__results";

interface OpenEditorTab {
    panelId: string;
    title: string;
    query: string;
    results: ConsoleResult[];
    hasResultsTab: boolean;
}

interface RunningExecutionItem {
    panelId: string;
    panelTitle: string;
    jobId: number;
    statementIndex?: number;
    statementQuery?: string;
    sourceQuery?: string;
    status: "pending" | "running";
    created_at?: string;
    execution_started_at?: string;
    execution_finished_at?: string;
}

const getStatusBadgeClassName = (status: ConsoleResult["status"]) => {
    if (status === "failed") {
        return "border-destructive/30 bg-destructive/10 text-destructive";
    }

    if (status === "success") {
        return "border-emerald-500/30 bg-emerald-500/10 text-emerald-500";
    }

    if (status === "pending" || status === "running") {
        return "border-amber-500/30 bg-amber-500/10 text-amber-500";
    }

    return "border-border bg-muted text-muted-foreground";
};

const formatExecutionStartedAt = (value?: string) => {
    if (!value) {
        return "n/a";
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return "n/a";
    }

    const pad = (input: number) => `${input}`.padStart(2, "0");

    return `${pad(date.getDate())}.${pad(date.getMonth() + 1)}.${date.getFullYear()} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
};

const getResultSortTime = (
    result: Pick<
        ConsoleResult,
        "execution_finished_at" | "execution_started_at" | "created_at"
    >,
) => {
    const timestamp =
        result.execution_finished_at ||
        result.execution_started_at ||
        result.created_at;

    if (!timestamp) {
        return 0;
    }

    const value = new Date(timestamp).getTime();
    return Number.isFinite(value) ? value : 0;
};

const getEntryQuery = (
    statementQuery?: string,
    sourceQuery?: string,
    fallbackQuery?: string,
) => {
    const query = statementQuery || sourceQuery || fallbackQuery || "";
    return query.trim() || "No query";
};

const isEditorPanel = (id: string) => !id.endsWith(RESULTS_PANEL_SUFFIX);

const collectOpenEditorTabs = (api: DockviewApi): OpenEditorTab[] =>
    api.panels
        .filter((panel) => isEditorPanel(panel.id))
        .map((panel) => {
            const panelState = normalizePanelParams(
                panel.api.getParameters<EditorPanelParams>(),
            );

            return {
                panelId: panel.id,
                title: panel.api.title || panel.id,
                query: panelState.query,
                results: panelState.results,
                hasResultsTab: Boolean(
                    api.getPanel(getResultsPanelId(panel.id)),
                ),
            };
        });

export const ConsoleExecutionHistory = () => {
    const {
        clearExecutionHistoryEntries,
        consoleEditorApi,
        executionHistoryActiveTab,
        executionHistoryEntries,
        executionHistoryPanelFilter,
        setExecutionHistoryActiveTab,
        setExecutionHistoryPanelFilter,
        setExecutionHistorySidebarCollapsed,
    } = useConsole();
    const [runningSearch, setRunningSearch] = useState("");
    const [historySearch, setHistorySearch] = useState("");
    const [openTabs, setOpenTabs] = useState<OpenEditorTab[]>(() =>
        consoleEditorApi ? collectOpenEditorTabs(consoleEditorApi) : [],
    );
    const [hasSyncedOpenTabs, setHasSyncedOpenTabs] = useState(() =>
        Boolean(consoleEditorApi),
    );
    const [isFilterPopoverOpen, setIsFilterPopoverOpen] = useState(false);
    const [isClearDialogOpen, setIsClearDialogOpen] = useState(false);

    useEffect(() => {
        if (!consoleEditorApi) {
            setOpenTabs([]);
            setHasSyncedOpenTabs(false);
            return;
        }

        setHasSyncedOpenTabs(false);

        const panelDisposables = new Map<string, DockviewIDisposable>();
        const bindPanelSubscriptions = () => {
            const editorPanels = consoleEditorApi.panels.filter((panel) =>
                isEditorPanel(panel.id),
            );
            const currentIds = new Set(editorPanels.map((panel) => panel.id));

            panelDisposables.forEach((disposable, panelId) => {
                if (!currentIds.has(panelId)) {
                    disposable.dispose();
                    panelDisposables.delete(panelId);
                }
            });

            editorPanels.forEach((panel) => {
                if (panelDisposables.has(panel.id)) {
                    return;
                }

                const onParamsChange = panel.api.onDidParametersChange(() => {
                    setOpenTabs(collectOpenEditorTabs(consoleEditorApi));
                });

                panelDisposables.set(panel.id, {
                    dispose: () => {
                        onParamsChange.dispose();
                    },
                });
            });
        };

        const syncTabs = () => {
            bindPanelSubscriptions();
            setOpenTabs(collectOpenEditorTabs(consoleEditorApi));
            setHasSyncedOpenTabs(true);
        };

        syncTabs();

        const onAdd = consoleEditorApi.onDidAddPanel(syncTabs);
        const onRemove = consoleEditorApi.onDidRemovePanel(syncTabs);
        const onLayoutChange = consoleEditorApi.onDidLayoutChange(syncTabs);

        return () => {
            panelDisposables.forEach((disposable) => disposable.dispose());
            onAdd.dispose();
            onRemove.dispose();
            onLayoutChange.dispose();
        };
    }, [consoleEditorApi]);

    const runningItems = useMemo<RunningExecutionItem[]>(() => {
        return openTabs
            .flatMap(
                (tab) =>
                    tab.results
                        .filter(
                            (result) =>
                                result.status === "pending" ||
                                result.status === "running",
                        )
                        .map((result) => ({
                            panelId: tab.panelId,
                            panelTitle: tab.title,
                            jobId: result.jobId,
                            statementIndex: result.statementIndex,
                            statementQuery: result.statementQuery,
                            sourceQuery: result.sourceQuery,
                            status:
                                result.status === "running"
                                    ? "running"
                                    : "pending",
                            created_at: result.created_at,
                            execution_started_at: result.execution_started_at,
                            execution_finished_at: result.execution_finished_at,
                        })) as RunningExecutionItem[],
            )
            .sort((left, right) => {
                const leftTime = getResultSortTime(left);
                const rightTime = getResultSortTime(right);

                if (leftTime !== rightTime) {
                    return rightTime - leftTime;
                }

                return right.jobId - left.jobId;
            });
    }, [openTabs]);

    const openTabById = useMemo(
        () => new Map(openTabs.map((tab) => [tab.panelId, tab])),
        [openTabs],
    );
    const tabLabelById = useMemo(
        () =>
            new Map(
                openTabs.map((tab, index) => [
                    tab.panelId,
                    `Query ${index + 1}`,
                ]),
            ),
        [openTabs],
    );

    useEffect(() => {
        if (!hasSyncedOpenTabs) {
            return;
        }

        if (executionHistoryPanelFilter === ALL_TABS_FILTER) {
            return;
        }

        if (openTabById.has(executionHistoryPanelFilter)) {
            return;
        }

        setExecutionHistoryPanelFilter(ALL_TABS_FILTER);
    }, [
        executionHistoryPanelFilter,
        hasSyncedOpenTabs,
        openTabById,
        setExecutionHistoryPanelFilter,
    ]);

    const filteredRunningItems = useMemo(() => {
        const normalizedSearch = runningSearch.trim().toLowerCase();
        if (!normalizedSearch) {
            return runningItems;
        }

        return runningItems.filter((item) => {
            const query = getEntryQuery(
                item.statementQuery,
                item.sourceQuery,
            ).toLowerCase();

            return (
                query.includes(normalizedSearch) ||
                item.panelTitle.toLowerCase().includes(normalizedSearch)
            );
        });
    }, [runningItems, runningSearch]);

    const resolvedHistoryItems = useMemo(
        () =>
            executionHistoryEntries.filter(
                (entry) =>
                    entry.status === "success" || entry.status === "failed",
            ),
        [executionHistoryEntries],
    );

    const filteredHistoryItems = useMemo(() => {
        const normalizedSearch = historySearch.trim().toLowerCase();

        return resolvedHistoryItems
            .filter((entry) => {
                if (executionHistoryPanelFilter === ALL_TABS_FILTER) {
                    return true;
                }

                return entry.panelId === executionHistoryPanelFilter;
            })
            .filter((entry) => {
                if (!normalizedSearch) {
                    return true;
                }

                const query = getEntryQuery(
                    entry.statementQuery,
                    entry.sourceQuery,
                ).toLowerCase();
                const panelTitle = (entry.panelTitle || "").toLowerCase();

                return (
                    query.includes(normalizedSearch) ||
                    panelTitle.includes(normalizedSearch) ||
                    `${entry.jobId}`.includes(normalizedSearch)
                );
            });
    }, [executionHistoryPanelFilter, historySearch, resolvedHistoryItems]);

    const filterLabel = useMemo(() => {
        if (executionHistoryPanelFilter === ALL_TABS_FILTER) {
            return "All Open Tabs";
        }

        return tabLabelById.get(executionHistoryPanelFilter) || "Query";
    }, [executionHistoryPanelFilter, tabLabelById]);

    const focusExecution = useCallback(
        (
            panelId: string,
            options?: {
                jobId?: number;
                statementIndex?: number;
            },
        ) => {
            if (!consoleEditorApi) {
                return;
            }

            const editorPanel = consoleEditorApi.getPanel(panelId);
            if (!editorPanel) {
                return;
            }

            const panelState = normalizePanelParams(
                editorPanel.api.getParameters<EditorPanelParams>(),
            );

            if (options?.jobId != null) {
                const nextSelectedResultIndex = panelState.results.findIndex(
                    (result) =>
                        result.jobId === options.jobId &&
                        (options.statementIndex == null ||
                            result.statementIndex === options.statementIndex),
                );

                if (nextSelectedResultIndex >= 0) {
                    editorPanel.api.updateParameters({
                        ...panelState,
                        selectedResultIndex: nextSelectedResultIndex,
                    });
                }
            }

            const resultsPanelId = getResultsPanelId(panelId);
            let resultsPanel = consoleEditorApi.getPanel(resultsPanelId);

            if (!resultsPanel && panelState.results.length > 0) {
                consoleEditorApi.addPanel({
                    id: resultsPanelId,
                    component: CONSOLE_RESULTS_PANEL_COMPONENT,
                    title: createResultsPanelTitle(editorPanel.api.title),
                    params: {
                        sourcePanelId: panelId,
                    },
                    position: {
                        referencePanel: panelId,
                        direction: "below",
                    },
                });

                resultsPanel = consoleEditorApi.getPanel(resultsPanelId);
            }

            if (resultsPanel) {
                resultsPanel.api.setActive();
            }

            editorPanel.api.setActive();
        },
        [consoleEditorApi],
    );

    const renderRunningList = () => (
        <>
            <div className="min-h-0 flex-1 overflow-y-auto">
                {filteredRunningItems.length === 0 ? (
                    <div className="px-3 py-4 text-xs text-muted-foreground">
                        No running queries
                    </div>
                ) : (
                    filteredRunningItems.map((item) => (
                        <button
                            key={`${item.panelId}:${item.jobId}:${item.statementIndex ?? -1}`}
                            type="button"
                            className="w-full border-b-[0.5px] px-3 py-2 text-left hover:bg-accent/50"
                            onClick={() =>
                                focusExecution(item.panelId, {
                                    jobId: item.jobId,
                                    statementIndex: item.statementIndex,
                                })
                            }>
                            <div className="mb-1 flex items-center gap-1.5">
                                <span
                                    className={cn(
                                        "rounded border px-1.5 py-0.5 text-[10px] uppercase tracking-wide",
                                        getStatusBadgeClassName(item.status),
                                    )}>
                                    {item.status}
                                </span>
                                <span className="rounded border border-border bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                                    {formatExecutionStartedAt(
                                        item.execution_started_at,
                                    )}
                                </span>
                            </div>
                            <p className="text-xs leading-4 whitespace-pre-wrap text-foreground/90 overflow-hidden [display:-webkit-box] [-webkit-line-clamp:3] [-webkit-box-orient:vertical]">
                                {getEntryQuery(
                                    item.statementQuery,
                                    item.sourceQuery,
                                )}
                            </p>
                        </button>
                    ))
                )}
            </div>
            <div className="relative border-t-[0.5px] border-border">
                <SearchIcon className="pointer-events-none absolute top-1/2 left-3 size-3 -translate-y-1/2 text-muted-foreground/50" />
                <input
                    value={runningSearch}
                    onChange={(event) => setRunningSearch(event.target.value)}
                    placeholder="Search running query"
                    className="h-9 w-full border-0 bg-transparent pr-3 pl-8 text-xs text-foreground placeholder:text-muted-foreground/50 outline-none"
                />
            </div>
        </>
    );

    const renderHistoryList = () => (
        <>
            <div className="min-h-0 flex-1 overflow-y-auto">
                {filteredHistoryItems.length === 0 ? (
                    <div className="px-3 py-4 text-xs text-muted-foreground">
                        No execution history
                    </div>
                ) : (
                    filteredHistoryItems.map((entry) => {
                        const openTab = openTabById.get(entry.panelId);
                        const canOpen = Boolean(openTab);

                        return (
                            <button
                                key={entry.id}
                                type="button"
                                className={cn(
                                    "w-full border-b-[0.5px] px-3 py-2 text-left",
                                    canOpen
                                        ? "hover:bg-accent/50"
                                        : "cursor-default opacity-80",
                                )}
                                disabled={!canOpen}
                                onClick={() =>
                                    focusExecution(entry.panelId, {
                                        jobId: entry.jobId,
                                        statementIndex: entry.statementIndex,
                                    })
                                }>
                                <div className="mb-1 flex items-center gap-1.5">
                                    <span
                                        className={cn(
                                            "rounded border px-1.5 py-0.5 text-[10px] uppercase tracking-wide",
                                            getStatusBadgeClassName(
                                                entry.status,
                                            ),
                                        )}>
                                        {entry.status}
                                    </span>
                                    <span className="rounded border border-border bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                                        {formatExecutionStartedAt(
                                            entry.execution_started_at,
                                        )}
                                    </span>
                                    {openTab ? (
                                        <span className="rounded border border-border bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                                            {openTab.title}
                                        </span>
                                    ) : null}
                                </div>
                                <p className="text-xs leading-4 whitespace-pre-wrap text-foreground/90 overflow-hidden [display:-webkit-box] [-webkit-line-clamp:3] [-webkit-box-orient:vertical]">
                                    {getEntryQuery(
                                        entry.statementQuery,
                                        entry.sourceQuery,
                                    )}
                                </p>
                            </button>
                        );
                    })
                )}
            </div>
            <Popover
                open={isFilterPopoverOpen}
                onOpenChange={setIsFilterPopoverOpen}>
                <PopoverTrigger asChild>
                    <button
                        type="button"
                        className="flex h-9 w-full items-center justify-between border-0 border-t-[0.5px] border-border bg-transparent px-3 text-xs text-foreground hover:bg-accent/40">
                        <span className="truncate text-left">
                            {filterLabel}
                        </span>
                        <ArrowDownIcon
                            className={cn(
                                "size-3 text-muted-foreground transition-transform",
                                isFilterPopoverOpen && "rotate-180",
                            )}
                        />
                    </button>
                </PopoverTrigger>
                <PopoverContent
                    align="start"
                    alignOffset={-8}
                    sideOffset={6}
                    className="overflow-hidden rounded-md border border-border bg-muted p-0 shadow-xl">
                    <div className="max-h-56 overflow-y-auto">
                        <button
                            type="button"
                            className={cn(
                                "flex h-9 w-full items-center justify-between border-b-[0.5px] px-3 text-left text-xs hover:bg-accent/40",
                                executionHistoryPanelFilter ===
                                    ALL_TABS_FILTER && "bg-accent/30",
                            )}
                            onClick={() => {
                                setExecutionHistoryPanelFilter(ALL_TABS_FILTER);
                                setIsFilterPopoverOpen(false);
                            }}>
                            <span>All Open Tabs</span>
                            {executionHistoryPanelFilter === ALL_TABS_FILTER ? (
                                <CheckIcon className="size-3 text-foreground/80" />
                            ) : null}
                        </button>
                        {openTabs.map((tab, index) => (
                            <button
                                key={tab.panelId}
                                type="button"
                                className={cn(
                                    "flex h-9 w-full items-center justify-between border-b-[0.5px] px-3 text-left text-xs hover:bg-accent/40",
                                    executionHistoryPanelFilter ===
                                        tab.panelId && "bg-accent/30",
                                )}
                                onClick={() => {
                                    setExecutionHistoryPanelFilter(tab.panelId);
                                    setIsFilterPopoverOpen(false);
                                }}>
                                <span>Query {index + 1}</span>
                                {executionHistoryPanelFilter === tab.panelId ? (
                                    <CheckIcon className="size-3 text-foreground/80" />
                                ) : null}
                            </button>
                        ))}
                    </div>
                </PopoverContent>
            </Popover>
            <div className="flex">
                <div className="relative min-w-0 flex-1 border-t-[0.5px] border-border">
                    <SearchIcon className="pointer-events-none absolute top-1/2 left-3 size-3 -translate-y-1/2 text-muted-foreground/50" />
                    <input
                        value={historySearch}
                        onChange={(event) =>
                            setHistorySearch(event.target.value)
                        }
                        placeholder="Search history query"
                        className="h-9 min-w-0 w-full border-0 bg-transparent pr-3 pl-8 text-xs text-foreground placeholder:text-muted-foreground/50 outline-none"
                    />
                </div>
                <Button
                    variant="ghost"
                    size="icon"
                    title="Clear history"
                    className="size-9 rounded-none border-0 border-l-[0.5px] border-t-[0.5px] border-border bg-transparent"
                    onClick={() => setIsClearDialogOpen(true)}>
                    <ClearHistoryIcon className="size-3.5" />
                </Button>
            </div>
        </>
    );

    return (
        <div className="flex h-full w-full flex-col">
            <ConsoleExecutionHistoryTopbar
                onClose={() => setExecutionHistorySidebarCollapsed(true)}
            />
            <div className="flex border-b-[0.5px]">
                <button
                    type="button"
                    className={cn(
                        "h-[35px] flex-1 rounded-none text-xs border-r-[0.5px]",
                        executionHistoryActiveTab === "running"
                            ? "bg-background "
                            : "bg-muted hover:bg-accent/40 text-foreground/30",
                    )}
                    onClick={() => setExecutionHistoryActiveTab("running")}>
                    Running ({runningItems.length})
                </button>
                <button
                    type="button"
                    className={cn(
                        "h-[35px] flex-1 rounded-none text-xs",
                        executionHistoryActiveTab === "history"
                            ? "bg-background"
                            : "bg-muted hover:bg-accent/40 text-foreground/30",
                    )}
                    onClick={() => setExecutionHistoryActiveTab("history")}>
                    History ({resolvedHistoryItems.length})
                </button>
            </div>
            {executionHistoryActiveTab === "running"
                ? renderRunningList()
                : renderHistoryList()}
            <Dialog
                open={isClearDialogOpen}
                onOpenChange={setIsClearDialogOpen}>
                <DialogContent className="sm:max-w-[360px]">
                    <DialogHeader>
                        <DialogTitle>Are you sure?</DialogTitle>
                        <DialogDescription>
                            This will permanently clear execution history.
                        </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                        <Button
                            variant="ghost"
                            onClick={() => setIsClearDialogOpen(false)}>
                            Cancel
                        </Button>
                        <Button
                            variant="destructive"
                            onClick={() => {
                                clearExecutionHistoryEntries();
                                setIsClearDialogOpen(false);
                            }}>
                            Clear history
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
        </div>
    );
};
