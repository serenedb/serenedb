import {
    type FC,
    type SVGProps,
    useCallback,
    useEffect,
    useState,
} from "react";
import {
    DockviewDefaultTab,
    DockviewReact,
    type DockviewReadyEvent,
    type IDockviewHeaderActionsProps,
    type IDockviewPanelHeaderProps,
    type SerializedDockview,
} from "dockview";
import {
    Button,
    cn,
    MaximizeIcon,
    MinimizeIcon,
    PlusIcon,
} from "@serene-ui/shared-frontend";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import {
    CONSOLE_EDITOR_PANEL_COMPONENT,
    CONSOLE_RESULTS_PANEL_COMPONENT,
    INITIAL_CONSOLE_EDITOR_PANELS,
    type ConsoleTabExecutionStatus,
    type EditorPanelParams,
    type ResultsPanelParams,
    addEditorPanel,
    createEditorPanelParams,
    createPanelId,
    createPanelTitle,
    getNextEditorPanelTitle,
} from "../model";
import { useConsole } from "../../Console/model";
import { EditorPanel } from "./EditorPanel";
import { ConsoleEditorTopbar } from "./ConsoleEditorTopbar";
import { ResultsPanel } from "./ResultsPanel";

const CONSOLE_EDITOR_LAYOUT_STORAGE_KEY = "console:editor-dock-layout";

const components = {
    [CONSOLE_EDITOR_PANEL_COMPONENT]: EditorPanel,
    [CONSOLE_RESULTS_PANEL_COMPONENT]: ResultsPanel,
};

const RESULTS_PANEL_SUFFIX = "__results";

const getSourceEditorPanelId = (panelId: string, sourcePanelId?: string) =>
    panelId.endsWith(RESULTS_PANEL_SUFFIX)
        ? (sourcePanelId ?? panelId.slice(0, -RESULTS_PANEL_SUFFIX.length))
        : panelId;

const getTabExecutionStatus = (
    params?: EditorPanelParams,
): ConsoleTabExecutionStatus => {
    const status = params?.tabExecutionStatus;

    if (status === "running" || status === "success" || status === "failed") {
        return status;
    }

    return "";
};

const isResolvedExecutionStatus = (
    status: ConsoleTabExecutionStatus,
): status is "success" | "failed" =>
    status === "success" || status === "failed";

const TAB_EXECUTION_DOT_COLOR_CLASS: Record<
    Exclude<ConsoleTabExecutionStatus, "">,
    {
        core: string;
        pulse: string;
    }
> = {
    running: {
        core: "bg-yellow-400",
        pulse: "bg-yellow-300/80",
    },
    success: {
        core: "bg-emerald-500",
        pulse: "bg-emerald-400/80",
    },
    failed: {
        core: "bg-red-500",
        pulse: "bg-red-400/80",
    },
};

const isPanelPairVisible = (
    containerApi: IDockviewPanelHeaderProps["containerApi"],
    sourcePanelId: string,
) => {
    const sourcePanel = containerApi.getPanel(sourcePanelId);
    const resultsPanel = containerApi.getPanel(
        `${sourcePanelId}${RESULTS_PANEL_SUFFIX}`,
    );

    return Boolean(sourcePanel?.api.isVisible || resultsPanel?.api.isVisible);
};

const ExecutionStatusTab: FC<IDockviewPanelHeaderProps> = (props) => {
    const sourcePanelId = getSourceEditorPanelId(
        props.api.id,
        (props.params as ResultsPanelParams | undefined)?.sourcePanelId,
    );
    const [status, setStatus] = useState<ConsoleTabExecutionStatus>(() => {
        const sourcePanel = props.containerApi.getPanel(sourcePanelId);
        return getTabExecutionStatus(
            sourcePanel?.api.getParameters<EditorPanelParams>(),
        );
    });
    const [isVisibleToUser, setIsVisibleToUser] = useState(() =>
        isPanelPairVisible(props.containerApi, sourcePanelId),
    );

    useEffect(() => {
        const sourcePanel = props.containerApi.getPanel(sourcePanelId);
        if (!sourcePanel) {
            setStatus("");
            return;
        }

        const syncStatus = () => {
            setStatus(
                getTabExecutionStatus(
                    sourcePanel.api.getParameters<EditorPanelParams>(),
                ),
            );
        };

        syncStatus();

        const subscription = sourcePanel.api.onDidParametersChange(syncStatus);

        return () => subscription.dispose();
    }, [props.containerApi, sourcePanelId]);

    useEffect(() => {
        const syncPairVisibility = () => {
            setIsVisibleToUser(
                isPanelPairVisible(props.containerApi, sourcePanelId),
            );
        };

        syncPairVisibility();

        const sourcePanel = props.containerApi.getPanel(sourcePanelId);
        const resultsPanel = props.containerApi.getPanel(
            `${sourcePanelId}${RESULTS_PANEL_SUFFIX}`,
        );
        const sourceVisibilitySubscription =
            sourcePanel?.api.onDidVisibilityChange(syncPairVisibility);
        const resultsVisibilitySubscription =
            resultsPanel?.api.onDidVisibilityChange(syncPairVisibility);
        const onActivePanelChange =
            props.containerApi.onDidActivePanelChange(syncPairVisibility);
        const onAddPanel = props.containerApi.onDidAddPanel(syncPairVisibility);
        const onRemovePanel =
            props.containerApi.onDidRemovePanel(syncPairVisibility);
        const onLayoutChange =
            props.containerApi.onDidLayoutChange(syncPairVisibility);

        return () => {
            sourceVisibilitySubscription?.dispose();
            resultsVisibilitySubscription?.dispose();
            onActivePanelChange.dispose();
            onAddPanel.dispose();
            onRemovePanel.dispose();
            onLayoutChange.dispose();
        };
    }, [props.containerApi, sourcePanelId]);

    const dismissResolvedStatus = useCallback(() => {
        if (!isResolvedExecutionStatus(status)) {
            return;
        }

        const sourcePanel = props.containerApi.getPanel(sourcePanelId);
        if (!sourcePanel) {
            return;
        }

        const sourceParams = sourcePanel.api.getParameters<EditorPanelParams>();
        const sourceStatus = getTabExecutionStatus(sourceParams);

        if (!isResolvedExecutionStatus(sourceStatus)) {
            return;
        }

        sourcePanel.api.updateParameters({
            ...sourceParams,
            tabExecutionStatus: "",
        });
    }, [props.containerApi, sourcePanelId, status]);

    return (
        <div
            className="relative h-full w-full"
            onPointerDown={dismissResolvedStatus}>
            {status ? (
                <span className="pointer-events-none absolute left-0.5 top-1/2 z-[1] -translate-y-1/2">
                    {!isVisibleToUser ? (
                        <span
                            className={cn(
                                "absolute left-1/2 top-1/2 h-1.5 w-1.5 -translate-x-1/2 -translate-y-1/2 rounded-full animate-ping",
                                TAB_EXECUTION_DOT_COLOR_CLASS[status].pulse,
                            )}
                            style={{
                                animationDuration: "700ms",
                            }}
                        />
                    ) : null}
                    <span
                        className={cn(
                            "relative block h-1.5 w-1.5 rounded-full",
                            TAB_EXECUTION_DOT_COLOR_CLASS[status].core,
                            !isVisibleToUser &&
                                "shadow-[0_0_0_1px_rgba(255,255,255,0.08),0_0_10px_rgba(255,255,255,0.2)]",
                        )}
                    />
                </span>
            ) : null}
            <DockviewDefaultTab
                {...props}
                style={status ? { paddingLeft: "1rem" } : undefined}
            />
        </div>
    );
};

const tabComponents = {
    [CONSOLE_EDITOR_PANEL_COMPONENT]: ExecutionStatusTab,
    [CONSOLE_RESULTS_PANEL_COMPONENT]: ExecutionStatusTab,
};

const HeaderActionButton: FC<{
    title: string;
    onClick: () => void;
    icon: FC<SVGProps<SVGSVGElement>>;
    className?: string;
}> = ({ title, onClick, icon: Icon, className }) => (
    <Button
        size="icon"
        variant="ghost"
        title={title}
        onClick={onClick}
        className={cn(
            "border-r-[0.5px] border-l-[0.5px] rounded-none size-9 text-foreground/50 hover:text-foreground",
            className,
        )}>
        <Icon className="size-3" />
    </Button>
);

const LeftHeaderActions: FC<IDockviewHeaderActionsProps> = (props) => (
    <div className="flex h-full items-center">
        <HeaderActionButton
            title="Add tab"
            onClick={() => {
                props.containerApi.addPanel({
                    id: createPanelId(),
                    component: CONSOLE_EDITOR_PANEL_COMPONENT,
                    tabComponent: CONSOLE_EDITOR_PANEL_COMPONENT,
                    title: getNextEditorPanelTitle(props.containerApi),
                    params: createEditorPanelParams(),
                    position: {
                        referenceGroup: props.group,
                    },
                });
            }}
            icon={PlusIcon}
        />
    </div>
);

const RightHeaderActions: FC<IDockviewHeaderActionsProps> = (props) => {
    const [isMaximized, setIsMaximized] = useState<boolean>(
        props.containerApi.hasMaximizedGroup(),
    );

    useEffect(() => {
        const disposable = props.containerApi.onDidMaximizedGroupChange(() => {
            setIsMaximized(props.containerApi.hasMaximizedGroup());
        });

        return () => disposable.dispose();
    }, [props.containerApi]);

    return (
        <div className="flex h-full items-center">
            <HeaderActionButton
                title={isMaximized ? "Minimize view" : "Maximize view"}
                className="border-r-0"
                onClick={() => {
                    if (props.containerApi.hasMaximizedGroup()) {
                        props.containerApi.exitMaximizedGroup();
                        return;
                    }

                    props.activePanel?.api.maximize();
                }}
                icon={isMaximized ? MinimizeIcon : MaximizeIcon}
            />
        </div>
    );
};

const sanitizeResultEntry = (entry: unknown) => {
    if (!entry || typeof entry !== "object") {
        return null;
    }

    const result = entry as Record<string, unknown>;
    const status = result.status;

    if (status === "pending" || status === "running") {
        return null;
    }

    return result;
};

const ensureAtLeastOneQueryTab = (containerApi: DockviewReadyEvent["api"]) => {
    if (containerApi.panels.length > 0) {
        return;
    }

    containerApi.addPanel({
        id: createPanelId(),
        component: CONSOLE_EDITOR_PANEL_COMPONENT,
        tabComponent: CONSOLE_EDITOR_PANEL_COMPONENT,
        title: createPanelTitle(1),
        params: createEditorPanelParams(),
    });
};

const getPanelGroupId = (panel: unknown) => {
    if (!panel || typeof panel !== "object") {
        return undefined;
    }

    const panelRecord = panel as Record<string, unknown>;
    const directGroup = panelRecord.group;

    if (directGroup && typeof directGroup === "object") {
        const groupId = (directGroup as Record<string, unknown>).id;
        return typeof groupId === "string" ? groupId : undefined;
    }

    const panelApi = panelRecord.api;
    if (!panelApi || typeof panelApi !== "object") {
        return undefined;
    }

    const apiGroup = (panelApi as Record<string, unknown>).group;
    if (!apiGroup || typeof apiGroup !== "object") {
        return undefined;
    }

    const groupId = (apiGroup as Record<string, unknown>).id;
    return typeof groupId === "string" ? groupId : undefined;
};

const sanitizeLayout = (value: unknown): unknown => {
    if (Array.isArray(value)) {
        return value.map((entry) => sanitizeLayout(entry));
    }

    if (!value || typeof value !== "object") {
        return value;
    }

    const record = value as Record<string, unknown>;
    const sanitized: Record<string, unknown> = {};

    const hasResults =
        Object.prototype.hasOwnProperty.call(record, "results") &&
        Array.isArray(record.results);
    let sanitizedResults: unknown[] | null = null;

    Object.entries(record).forEach(([key, entry]) => {
        if (key === "results" && Array.isArray(entry)) {
            sanitizedResults = entry
                .map((result) => sanitizeResultEntry(result))
                .filter((result) => result !== null);
            sanitized[key] = sanitizedResults;
            return;
        }

        if (key === "runOnMountMode" || key === "tabExecutionStatus") {
            return;
        }

        sanitized[key] = sanitizeLayout(entry);
    });

    if (hasResults) {
        const selectedResultIndex = Number(sanitized.selectedResultIndex);
        const resultsLength =
            (sanitizedResults as unknown[] | null)?.length ?? 0;

        sanitized.selectedResultIndex =
            resultsLength > 0
                ? Number.isFinite(selectedResultIndex)
                    ? Math.min(
                          Math.max(0, selectedResultIndex),
                          resultsLength - 1,
                      )
                    : 0
                : 0;
    }

    return sanitized;
};

export const ConsoleEditor: FC = () => {
    const { selectRelatedResultOnTabChange, setConsoleEditorApi } =
        useConsole();
    const [api, setApi] = useState<DockviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);

    const onReady = (event: DockviewReadyEvent) => {
        setApi(event.api);
        setConsoleEditorApi(event.api);

        let restored = false;
        const rawLayout = localStorage.getItem(
            CONSOLE_EDITOR_LAYOUT_STORAGE_KEY,
        );
        if (rawLayout) {
            try {
                const sanitizedLayout = sanitizeLayout(JSON.parse(rawLayout));
                event.api.fromJSON(sanitizedLayout as SerializedDockview);
                restored = true;
            } catch (error) {
                console.warn("Failed to restore console editor layout:", error);
                localStorage.removeItem(CONSOLE_EDITOR_LAYOUT_STORAGE_KEY);
            }
        }

        if (restored) {
            ensureAtLeastOneQueryTab(event.api);
            return;
        }

        Array.from({ length: INITIAL_CONSOLE_EDITOR_PANELS }).forEach(() => {
            addEditorPanel(event.api);
        });
    };

    useEffect(() => {
        return () => {
            setConsoleEditorApi(undefined);
        };
    }, [setConsoleEditorApi]);

    useEffect(() => {
        if (!api) {
            return;
        }

        const panelDisposables = new Map<string, { dispose: () => void }>();
        let persistTimeout: number | undefined;

        const persistLayout = () => {
            try {
                localStorage.setItem(
                    CONSOLE_EDITOR_LAYOUT_STORAGE_KEY,
                    JSON.stringify(sanitizeLayout(api.toJSON())),
                );
            } catch (error) {
                console.warn("Failed to save console editor layout:", error);
            }
        };

        const schedulePersistLayout = () => {
            if (persistTimeout !== undefined) {
                window.clearTimeout(persistTimeout);
            }

            persistTimeout = window.setTimeout(() => {
                persistTimeout = undefined;
                persistLayout();
            }, 120);
        };

        const bindPanelSubscriptions = () => {
            const panelIds = new Set(api.panels.map((panel) => panel.id));

            panelDisposables.forEach((disposable, panelId) => {
                if (panelIds.has(panelId)) {
                    return;
                }

                disposable.dispose();
                panelDisposables.delete(panelId);
            });

            api.panels.forEach((panel) => {
                if (panelDisposables.has(panel.id)) {
                    return;
                }

                const onParamsChange = panel.api.onDidParametersChange(() => {
                    schedulePersistLayout();
                });

                panelDisposables.set(panel.id, {
                    dispose: () => onParamsChange.dispose(),
                });
            });
        };

        const syncPersistence = () => {
            bindPanelSubscriptions();
            schedulePersistLayout();
        };

        const handleBeforeUnload = () => {
            persistLayout();
        };

        syncPersistence();

        window.addEventListener("beforeunload", handleBeforeUnload);

        const onAdd = api.onDidAddPanel(syncPersistence);
        const onRemove = api.onDidRemovePanel(syncPersistence);
        const onLayoutChange = api.onDidLayoutChange(syncPersistence);

        return () => {
            window.removeEventListener("beforeunload", handleBeforeUnload);
            if (persistTimeout !== undefined) {
                window.clearTimeout(persistTimeout);
            }

            panelDisposables.forEach((disposable) => disposable.dispose());
            onAdd.dispose();
            onRemove.dispose();
            onLayoutChange.dispose();
        };
    }, [api]);

    useEffect(() => {
        if (!api) {
            return;
        }

        const ensureQueryTab = () => {
            ensureAtLeastOneQueryTab(api);
        };

        ensureQueryTab();

        const onAdd = api.onDidAddPanel(ensureQueryTab);
        const onRemove = api.onDidRemovePanel(ensureQueryTab);
        const onLayoutChange = api.onDidLayoutChange(ensureQueryTab);

        return () => {
            onAdd.dispose();
            onRemove.dispose();
            onLayoutChange.dispose();
        };
    }, [api]);

    useEffect(() => {
        if (!api) {
            return;
        }

        const subscription = api.onDidActivePanelChange((activePanel) => {
            if (!activePanel) {
                return;
            }

            const sourcePanelId = getSourceEditorPanelId(
                activePanel.id,
                activePanel.id.endsWith(RESULTS_PANEL_SUFFIX)
                    ? activePanel.api.getParameters<ResultsPanelParams>()
                          ?.sourcePanelId
                    : undefined,
            );
            const sourcePanel = api.getPanel(sourcePanelId);

            if (!sourcePanel) {
                return;
            }

            const sourceParams =
                sourcePanel.api.getParameters<EditorPanelParams>();
            const sourceStatus = getTabExecutionStatus(sourceParams);

            if (!isResolvedExecutionStatus(sourceStatus)) {
                return;
            }

            sourcePanel.api.updateParameters({
                ...sourceParams,
                tabExecutionStatus: "",
            });
        });

        return () => subscription.dispose();
    }, [api]);

    useEffect(() => {
        if (!api || !selectRelatedResultOnTabChange) {
            return;
        }

        let syncing = false;
        const subscription = api.onDidActivePanelChange((activePanel) => {
            if (syncing || !activePanel) {
                return;
            }

            const activePanelId = activePanel.id;
            if (typeof activePanelId !== "string") {
                return;
            }

            const isResultsPanel = activePanelId.endsWith(RESULTS_PANEL_SUFFIX);
            const relatedPanelId = isResultsPanel
                ? (activePanel.api.getParameters<ResultsPanelParams>()
                      ?.sourcePanelId ??
                  activePanelId.slice(0, -RESULTS_PANEL_SUFFIX.length))
                : `${activePanelId}${RESULTS_PANEL_SUFFIX}`;

            if (!relatedPanelId || relatedPanelId === activePanelId) {
                return;
            }

            const relatedPanel = api.getPanel(relatedPanelId);

            if (!relatedPanel) {
                return;
            }

            const sourceGroupId = getPanelGroupId(activePanel);
            const relatedGroupId = getPanelGroupId(relatedPanel);

            if (
                sourceGroupId &&
                relatedGroupId &&
                sourceGroupId === relatedGroupId
            ) {
                return;
            }

            syncing = true;

            try {
                relatedPanel.api.setActive();
                activePanel.api.setActive();
            } finally {
                syncing = false;
            }
        });

        return () => subscription.dispose();
    }, [api, selectRelatedResultOnTabChange]);

    return (
        <div ref={containerRef} className="relative flex h-dvh w-full flex-col">
            <ConsoleEditorTopbar />
            <div className="flex-1 min-h-0">
                <DockviewReact
                    onReady={onReady}
                    components={components}
                    defaultTabComponent={ExecutionStatusTab}
                    tabComponents={tabComponents}
                    leftHeaderActionsComponent={LeftHeaderActions}
                    rightHeaderActionsComponent={RightHeaderActions}
                />
            </div>
        </div>
    );
};
