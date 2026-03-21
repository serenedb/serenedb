import { syncBindVars } from "@serene-ui/shared-frontend/entities";
import { useConsoleLayout } from "@serene-ui/shared-frontend/features";
import {
    DEFAULT_HOTKEYS,
    useAppHotkey,
} from "@serene-ui/shared-frontend/shared";
import { useEffect, useRef, useState } from "react";
import { ConsoleContext } from "./ConsoleContext";
import {
    useTabManagement,
    useQueryExecution,
    useConsoleNotifications,
} from "./hooks";

export const ConsoleProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const explorerRef = useRef<HTMLDivElement | null>(null);
    const editorRef = useRef<HTMLElement | null>(null);

    const [limit, setLimit] = useState<number>(
        Number(localStorage.getItem("console:rows-limit")) || 1000,
    );

    const { isMaximized, isMaximizedResultsShown, toggleMaximizedResults } =
        useConsoleLayout();

    const { tabs, selectedTabId, addTab, removeTab, selectTab, updateTab } =
        useTabManagement();

    const handleAddTab = (type: "query") => {
        if (isMaximizedResultsShown) {
            toggleMaximizedResults();
        }
        addTab(type);
    };

    const handleSelectTab = (id: number) => {
        if (isMaximizedResultsShown) {
            toggleMaximizedResults();
        }
        selectTab(id);
    };

    const handleRemoveTab = (id: number) => {
        removeTab(id);
    };

    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_NEW_TAB, () => {
        handleAddTab("query");
    });

    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_CLOSE_TAB, () => {
        handleRemoveTab(selectedTabId);
    });

    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_TOGGLE_EXPLORER_EDITOR, () => {
        const explorerHasFocus = explorerRef.current?.contains(
            document.activeElement,
        );

        if (explorerHasFocus) {
            const monacoEditor = (
                editorRef.current as unknown as {
                    __monacoEditor?: { focus: () => void };
                }
            )?.__monacoEditor;
            if (monacoEditor) {
                monacoEditor.focus();
            }
        } else {
            const explorerElement = explorerRef.current?.querySelector(
                '[tabindex="0"]',
            ) as HTMLElement;
            if (explorerElement) {
                explorerElement.focus();
            }
        }
    });

    const { addPendingResults } = useQueryExecution({
        tabs,
        selectedTabId,
        updateTab,
        isMaximized,
        isMaximizedResultsShown,
        toggleMaximizedResults,
    });

    useConsoleNotifications({
        tabs,
        selectedTabId,
    });

    const selectResult = (tabId: number, resultIndex: number) => {
        const tab = tabs.find((item) => item.id === tabId);
        if (!tab) {
            return;
        }

        const maxIndex = Math.max(0, tab.results.length - 1);
        updateTab(tabId, {
            selectedResultIndex: Math.min(Math.max(0, resultIndex), maxIndex),
        });
    };

    useEffect(() => {
        const tab = tabs.find((t) => t.id === selectedTabId);
        if (!tab) return;
        if (typeof tab.value !== "string") return;
        if (!Array.isArray(tab.bind_vars)) return;

        const newBindVars = syncBindVars(tab.value, tab.bind_vars);

        if (JSON.stringify(tab.bind_vars) !== JSON.stringify(newBindVars)) {
            updateTab(tab.id, { bind_vars: newBindVars });
        }
    }, [tabs, selectedTabId, updateTab]);

    useEffect(() => {
        tabs.forEach((tab) => {
            const maxIndex = Math.max(0, tab.results.length - 1);
            const nextSelectedIndex = Math.min(
                Math.max(0, tab.selectedResultIndex ?? 0),
                maxIndex,
            );

            if (tab.selectedResultIndex !== nextSelectedIndex) {
                updateTab(tab.id, {
                    selectedResultIndex: nextSelectedIndex,
                });
            }
        });
    }, [tabs, updateTab]);

    useEffect(() => {
        localStorage.setItem("console:rows-limit", JSON.stringify(limit));
    }, [limit]);

    return (
        <ConsoleContext.Provider
            value={{
                tabs,
                addTab: handleAddTab,
                removeTab: handleRemoveTab,
                selectTab: handleSelectTab,
                selectResult,
                selectedTabId,
                updateTab,
                addPendingResults,
                limit,
                setLimit,
                explorerRef,
                editorRef,
            }}>
            {children}
        </ConsoleContext.Provider>
    );
};
