import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import type { DockviewApi } from "dockview";
import { ConsoleContext } from "./ConsoleContext";
import type {
    ConsoleExecutionAlertMode,
    ConsoleExecutionHistoryEntry,
    ConsoleExecutionHistoryEntryInput,
    ConsoleExecutionHistorySidebarTab,
} from "./types";
import {
    CONSOLE_ALERT_ON_EXECUTION_STORAGE_KEY,
    CONSOLE_COLORFUL_TYPES_IN_RESULTS_STORAGE_KEY,
    CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT_STORAGE_KEY,
    CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT_STORAGE_KEY,
    CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY,
    CONSOLE_LIMIT_STORAGE_KEY,
    CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE_STORAGE_KEY,
    CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY,
    CONSOLE_SHOW_AUTOCOMPLETE_STORAGE_KEY,
    CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE_STORAGE_KEY,
    CONSOLE_SHOW_JSON_BY_DEFAULT_STORAGE_KEY,
    CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE_STORAGE_KEY,
    CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY,
    CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB_STORAGE_KEY,
    DEFAULT_CONSOLE_ALERT_ON_EXECUTION,
    DEFAULT_CONSOLE_COLORFUL_TYPES_IN_RESULTS,
    DEFAULT_CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT,
    DEFAULT_CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT,
    DEFAULT_CONSOLE_QUERY_LIMIT,
    DEFAULT_CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE,
    DEFAULT_CONSOLE_SHOW_AUTOCOMPLETE,
    DEFAULT_CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE,
    DEFAULT_CONSOLE_SHOW_JSON_BY_DEFAULT,
    DEFAULT_CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE,
    DEFAULT_CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB,
} from "./consts";

const CONSOLE_ALERT_MODES: ConsoleExecutionAlertMode[] = [
    "always",
    "onlyUnseen",
    "never",
];
const EXECUTION_HISTORY_ALL_PANELS_FILTER = "__all__";
const EXECUTION_HISTORY_DEFAULT_TAB: ConsoleExecutionHistorySidebarTab =
    "running";
const CONSOLE_EXECUTION_HISTORY_STORAGE_KEY = "console:execution-history:v1";

const getExecutionHistoryEntryId = (
    entry: Pick<
        ConsoleExecutionHistoryEntryInput,
        "panelId" | "jobId" | "statementIndex"
    >,
) => `${entry.panelId}:${entry.jobId}:${entry.statementIndex ?? -1}`;

const readStoredExecutionHistoryEntries = (): ConsoleExecutionHistoryEntry[] => {
    if (typeof window === "undefined") {
        return [];
    }

    const rawValue = window.localStorage.getItem(
        CONSOLE_EXECUTION_HISTORY_STORAGE_KEY,
    );

    if (!rawValue) {
        return [];
    }

    try {
        const parsed = JSON.parse(rawValue);
        if (!Array.isArray(parsed)) {
            return [];
        }

        return parsed.filter((entry): entry is ConsoleExecutionHistoryEntry => {
            if (!entry || typeof entry !== "object") {
                return false;
            }

            const typedEntry = entry as Partial<ConsoleExecutionHistoryEntry>;
            return (
                typeof typedEntry.id === "string" &&
                typeof typedEntry.panelId === "string" &&
                typeof typedEntry.jobId === "number" &&
                typeof typedEntry.status === "string" &&
                typeof typedEntry.updated_at === "string"
            );
        });
    } catch (error) {
        console.warn("Failed to restore console execution history:", error);
        return [];
    }
};

const readStoredLimit = () => {
    if (typeof window === "undefined") {
        return DEFAULT_CONSOLE_QUERY_LIMIT;
    }

    const storedLimit = Number(
        window.localStorage.getItem(CONSOLE_LIMIT_STORAGE_KEY),
    );

    return Number.isFinite(storedLimit) && storedLimit > 0
        ? Math.floor(storedLimit)
        : DEFAULT_CONSOLE_QUERY_LIMIT;
};

const readStoredBoolean = (storageKey: string, defaultValue: boolean) => {
    if (typeof window === "undefined") {
        return defaultValue;
    }

    const rawValue = window.localStorage.getItem(storageKey);

    if (rawValue === "true") {
        return true;
    }

    if (rawValue === "false") {
        return false;
    }

    return defaultValue;
};

const readStoredSidebarCollapsed = () =>
    readStoredBoolean(CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY, false);

const readStoredSettingsSidebarCollapsed = () =>
    readStoredBoolean(CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY, true);

const readStoredExecutionHistorySidebarCollapsed = () =>
    readStoredBoolean(
        CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY,
        true,
    );

const readStoredAlertOnExecution = (): ConsoleExecutionAlertMode => {
    if (typeof window === "undefined") {
        return DEFAULT_CONSOLE_ALERT_ON_EXECUTION;
    }

    const rawValue = window.localStorage.getItem(
        CONSOLE_ALERT_ON_EXECUTION_STORAGE_KEY,
    );

    if (
        !rawValue ||
        !CONSOLE_ALERT_MODES.includes(rawValue as ConsoleExecutionAlertMode)
    ) {
        return DEFAULT_CONSOLE_ALERT_ON_EXECUTION;
    }

    return rawValue as ConsoleExecutionAlertMode;
};

export const useConsoleSettings = () => {
    const [limit, setLimitState] = useState(readStoredLimit);
    const [spawnResultsInFirstTab, setSpawnResultsInFirstTabState] = useState(
        () =>
            readStoredBoolean(
                CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB_STORAGE_KEY,
                DEFAULT_CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB,
            ),
    );
    const [colorfulTypesInResults, setColorfulTypesInResultsState] = useState(
        () =>
            readStoredBoolean(
                CONSOLE_COLORFUL_TYPES_IN_RESULTS_STORAGE_KEY,
                DEFAULT_CONSOLE_COLORFUL_TYPES_IN_RESULTS,
            ),
    );
    const [
        selectRelatedResultOnTabChange,
        setSelectRelatedResultOnTabChangeState,
    ] = useState(() =>
        readStoredBoolean(
            CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE_STORAGE_KEY,
            DEFAULT_CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE,
        ),
    );
    const [showJsonByDefault, setShowJsonByDefaultState] = useState(() =>
        readStoredBoolean(
            CONSOLE_SHOW_JSON_BY_DEFAULT_STORAGE_KEY,
            DEFAULT_CONSOLE_SHOW_JSON_BY_DEFAULT,
        ),
    );
    const [showSavedQueriesInAutocomplete, setShowSavedQueriesState] = useState(
        () =>
            readStoredBoolean(
                CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE_STORAGE_KEY,
                DEFAULT_CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE,
            ),
    );
    const [
        showExecutionHistoryInAutocomplete,
        setShowExecutionHistoryState,
    ] = useState(() =>
        readStoredBoolean(
            CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE_STORAGE_KEY,
            DEFAULT_CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE,
        ),
    );
    const [showAutocomplete, setShowAutocompleteState] = useState(() =>
        readStoredBoolean(
            CONSOLE_SHOW_AUTOCOMPLETE_STORAGE_KEY,
            DEFAULT_CONSOLE_SHOW_AUTOCOMPLETE,
        ),
    );
    const [executeSequentiallyByDefault, setExecuteSequentiallyState] =
        useState(() =>
            readStoredBoolean(
                CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT_STORAGE_KEY,
                DEFAULT_CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT,
            ),
        );
    const [executeInNewTabByDefault, setExecuteInNewTabState] = useState(() =>
        readStoredBoolean(
            CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT_STORAGE_KEY,
            DEFAULT_CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT,
        ),
    );
    const [alertOnExecution, setAlertOnExecutionState] = useState(
        readStoredAlertOnExecution,
    );

    const setLimit = useCallback((nextLimit: number) => {
        setLimitState(
            Number.isFinite(nextLimit) && nextLimit > 0
                ? Math.floor(nextLimit)
                : DEFAULT_CONSOLE_QUERY_LIMIT,
        );
    }, []);

    const setSpawnResultsInFirstTab = useCallback((value: boolean) => {
        setSpawnResultsInFirstTabState(value);
    }, []);

    const setColorfulTypesInResults = useCallback((value: boolean) => {
        setColorfulTypesInResultsState(value);
    }, []);

    const setSelectRelatedResultOnTabChange = useCallback((value: boolean) => {
        setSelectRelatedResultOnTabChangeState(value);
    }, []);

    const setShowJsonByDefault = useCallback((value: boolean) => {
        setShowJsonByDefaultState(value);
    }, []);

    const setShowSavedQueriesInAutocomplete = useCallback((value: boolean) => {
        setShowSavedQueriesState(value);
    }, []);

    const setShowExecutionHistoryInAutocomplete = useCallback(
        (value: boolean) => {
            setShowExecutionHistoryState(value);
        },
        [],
    );

    const setShowAutocomplete = useCallback((value: boolean) => {
        setShowAutocompleteState(value);

        if (!value) {
            setShowSavedQueriesState(false);
            setShowExecutionHistoryState(false);
        }
    }, []);

    useEffect(() => {
        if (showAutocomplete) {
            return;
        }

        if (showSavedQueriesInAutocomplete) {
            setShowSavedQueriesState(false);
        }

        if (showExecutionHistoryInAutocomplete) {
            setShowExecutionHistoryState(false);
        }
    }, [
        showAutocomplete,
        showExecutionHistoryInAutocomplete,
        showSavedQueriesInAutocomplete,
    ]);

    const setExecuteSequentiallyByDefault = useCallback((value: boolean) => {
        setExecuteSequentiallyState(value);
    }, []);

    const setExecuteInNewTabByDefault = useCallback((value: boolean) => {
        setExecuteInNewTabState(value);
    }, []);

    const setAlertOnExecution = useCallback(
        (value: ConsoleExecutionAlertMode) => {
            setAlertOnExecutionState(
                CONSOLE_ALERT_MODES.includes(value) ? value : "onlyUnseen",
            );
        },
        [],
    );

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_LIMIT_STORAGE_KEY,
            JSON.stringify(limit),
        );
    }, [limit]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB_STORAGE_KEY,
            String(spawnResultsInFirstTab),
        );
    }, [spawnResultsInFirstTab]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_COLORFUL_TYPES_IN_RESULTS_STORAGE_KEY,
            String(colorfulTypesInResults),
        );
    }, [colorfulTypesInResults]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE_STORAGE_KEY,
            String(selectRelatedResultOnTabChange),
        );
    }, [selectRelatedResultOnTabChange]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SHOW_JSON_BY_DEFAULT_STORAGE_KEY,
            String(showJsonByDefault),
        );
    }, [showJsonByDefault]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE_STORAGE_KEY,
            String(showSavedQueriesInAutocomplete),
        );
    }, [showSavedQueriesInAutocomplete]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE_STORAGE_KEY,
            String(showExecutionHistoryInAutocomplete),
        );
    }, [showExecutionHistoryInAutocomplete]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SHOW_AUTOCOMPLETE_STORAGE_KEY,
            String(showAutocomplete),
        );
    }, [showAutocomplete]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT_STORAGE_KEY,
            String(executeSequentiallyByDefault),
        );
    }, [executeSequentiallyByDefault]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT_STORAGE_KEY,
            String(executeInNewTabByDefault),
        );
    }, [executeInNewTabByDefault]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_ALERT_ON_EXECUTION_STORAGE_KEY,
            alertOnExecution,
        );
    }, [alertOnExecution]);

    return useMemo(
        () => ({
            alertOnExecution,
            colorfulTypesInResults,
            executeInNewTabByDefault,
            executeSequentiallyByDefault,
            limit,
            selectRelatedResultOnTabChange,
            setAlertOnExecution,
            setColorfulTypesInResults,
            setExecuteInNewTabByDefault,
            setExecuteSequentiallyByDefault,
            setLimit,
            setSelectRelatedResultOnTabChange,
            setShowAutocomplete,
            setShowExecutionHistoryInAutocomplete,
            setShowJsonByDefault,
            setShowSavedQueriesInAutocomplete,
            setSpawnResultsInFirstTab,
            showAutocomplete,
            showExecutionHistoryInAutocomplete,
            showJsonByDefault,
            showSavedQueriesInAutocomplete,
            spawnResultsInFirstTab,
        }),
        [
            alertOnExecution,
            colorfulTypesInResults,
            executeInNewTabByDefault,
            executeSequentiallyByDefault,
            limit,
            selectRelatedResultOnTabChange,
            setAlertOnExecution,
            setColorfulTypesInResults,
            setExecuteInNewTabByDefault,
            setExecuteSequentiallyByDefault,
            setLimit,
            setSelectRelatedResultOnTabChange,
            setShowAutocomplete,
            setShowExecutionHistoryInAutocomplete,
            setShowJsonByDefault,
            setShowSavedQueriesInAutocomplete,
            setSpawnResultsInFirstTab,
            showAutocomplete,
            showExecutionHistoryInAutocomplete,
            showJsonByDefault,
            showSavedQueriesInAutocomplete,
            spawnResultsInFirstTab,
        ],
    );
};

export const ConsoleProvider = ({
    children,
}: {
    children: ReactNode;
}) => {
    const settings = useConsoleSettings();
    const [sidebarCollapsed, setSidebarCollapsedState] = useState(
        readStoredSidebarCollapsed,
    );
    const [settingsSidebarCollapsed, setSettingsSidebarCollapsedState] =
        useState(readStoredSettingsSidebarCollapsed);
    const [
        executionHistorySidebarCollapsed,
        setExecutionHistorySidebarCollapsedState,
    ] = useState(readStoredExecutionHistorySidebarCollapsed);
    const [executionHistoryActiveTab, setExecutionHistoryActiveTabState] =
        useState<ConsoleExecutionHistorySidebarTab>(
            EXECUTION_HISTORY_DEFAULT_TAB,
        );
    const [executionHistoryPanelFilter, setExecutionHistoryPanelFilterState] =
        useState<string>(EXECUTION_HISTORY_ALL_PANELS_FILTER);
    const [executionHistoryEntries, setExecutionHistoryEntries] = useState<
        ConsoleExecutionHistoryEntry[]
    >(readStoredExecutionHistoryEntries);
    const [consoleEditorApi, setConsoleEditorApiState] =
        useState<DockviewApi>();

    const setSidebarCollapsed = useCallback((collapsed: boolean) => {
        setSidebarCollapsedState(collapsed);
    }, []);

    const toggleSidebar = useCallback(() => {
        setSidebarCollapsedState((current) => !current);
    }, []);

    const setSettingsSidebarCollapsed = useCallback((collapsed: boolean) => {
        setSettingsSidebarCollapsedState(collapsed);
    }, []);

    const toggleSettingsSidebar = useCallback(() => {
        setSettingsSidebarCollapsedState((current) => {
            const nextCollapsed = !current;

            if (!nextCollapsed) {
                setExecutionHistorySidebarCollapsedState(true);
            }

            return nextCollapsed;
        });
    }, []);

    const setExecutionHistorySidebarCollapsed = useCallback(
        (collapsed: boolean) => {
            setExecutionHistorySidebarCollapsedState(collapsed);
        },
        [],
    );

    const setExecutionHistoryActiveTab = useCallback(
        (tab: ConsoleExecutionHistorySidebarTab) => {
            setExecutionHistoryActiveTabState(tab);
        },
        [],
    );

    const setExecutionHistoryPanelFilter = useCallback((panelId: string) => {
        setExecutionHistoryPanelFilterState(
            panelId || EXECUTION_HISTORY_ALL_PANELS_FILTER,
        );
    }, []);

    const upsertExecutionHistoryEntries = useCallback(
        (entries: ConsoleExecutionHistoryEntryInput[]) => {
            if (!entries.length) {
                return;
            }

            const updatedAt = new Date().toISOString();

            setExecutionHistoryEntries((current) => {
                const byId = new Map(current.map((entry) => [entry.id, entry]));

                entries.forEach((entry) => {
                    const id = getExecutionHistoryEntryId(entry);
                    const existing = byId.get(id);

                    byId.set(id, {
                        ...existing,
                        ...entry,
                        id,
                        updated_at: updatedAt,
                    });
                });

                return Array.from(byId.values()).sort(
                    (left, right) =>
                        new Date(right.updated_at).getTime() -
                        new Date(left.updated_at).getTime(),
                );
            });
        },
        [],
    );

    const clearExecutionHistoryEntries = useCallback(() => {
        setExecutionHistoryEntries([]);
    }, []);

    const setConsoleEditorApi = useCallback((api?: DockviewApi) => {
        setConsoleEditorApiState(api);
    }, []);

    const openExecutionHistorySidebar = useCallback(
        (options?: {
            tab?: ConsoleExecutionHistorySidebarTab;
            panelId?: string;
        }) => {
            if (options?.tab) {
                setExecutionHistoryActiveTabState(options.tab);
            }

            if (options?.panelId) {
                setExecutionHistoryPanelFilterState(options.panelId);
            }

            setSettingsSidebarCollapsedState(true);
            setExecutionHistorySidebarCollapsedState(false);
        },
        [],
    );

    const toggleExecutionHistorySidebar = useCallback(() => {
        setExecutionHistorySidebarCollapsedState((current) => {
            const nextCollapsed = !current;

            if (!nextCollapsed) {
                setSettingsSidebarCollapsedState(true);
            }

            return nextCollapsed;
        });
    }, []);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY,
            String(sidebarCollapsed),
        );
    }, [sidebarCollapsed]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY,
            String(settingsSidebarCollapsed),
        );
    }, [settingsSidebarCollapsed]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY,
            String(executionHistorySidebarCollapsed),
        );
    }, [executionHistorySidebarCollapsed]);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        window.localStorage.setItem(
            CONSOLE_EXECUTION_HISTORY_STORAGE_KEY,
            JSON.stringify(executionHistoryEntries),
        );
    }, [executionHistoryEntries]);

    const value = useMemo(
        () => ({
            ...settings,
            clearExecutionHistoryEntries,
            consoleEditorApi,
            executionHistoryActiveTab,
            executionHistoryEntries,
            executionHistoryPanelFilter,
            executionHistorySidebarCollapsed,
            openExecutionHistorySidebar,
            setConsoleEditorApi,
            setExecutionHistoryActiveTab,
            setExecutionHistoryPanelFilter,
            setExecutionHistorySidebarCollapsed,
            setSettingsSidebarCollapsed,
            setSidebarCollapsed,
            settingsSidebarCollapsed,
            sidebarCollapsed,
            toggleExecutionHistorySidebar,
            toggleSettingsSidebar,
            toggleSidebar,
            upsertExecutionHistoryEntries,
        }),
        [
            settings,
            clearExecutionHistoryEntries,
            consoleEditorApi,
            executionHistoryActiveTab,
            executionHistoryEntries,
            executionHistoryPanelFilter,
            executionHistorySidebarCollapsed,
            openExecutionHistorySidebar,
            setConsoleEditorApi,
            setExecutionHistoryActiveTab,
            setExecutionHistoryPanelFilter,
            setExecutionHistorySidebarCollapsed,
            setSettingsSidebarCollapsed,
            setSidebarCollapsed,
            settingsSidebarCollapsed,
            sidebarCollapsed,
            toggleExecutionHistorySidebar,
            toggleSettingsSidebar,
            toggleSidebar,
            upsertExecutionHistoryEntries,
        ],
    );

    return (
        <ConsoleContext.Provider value={value}>{children}</ConsoleContext.Provider>
    );
};
