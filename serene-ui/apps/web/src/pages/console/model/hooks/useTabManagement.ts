import { useState, useCallback, useEffect } from "react";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";

export interface UseTabManagementReturn {
    tabs: ConsoleTab[];
    selectedTabId: number;
    addTab: (type: "query") => void;
    removeTab: (id: number) => void;
    selectTab: (id: number) => void;
    updateTab: (id: number, tabUpdate: Partial<ConsoleTab>) => void;
}

export const useTabManagement = (): UseTabManagementReturn => {
    const [tabs, setTabs] = useState<ConsoleTab[]>([
        {
            id: 0,
            type: "query",
            value: "",
            bind_vars: [],
            results: [],
        },
    ]);
    const [selectedTabId, setSelectedTabId] = useState<number>(0);

    const addTab = useCallback((type: "query") => {
        if (type === "query") {
            setTabs((prevTabs) => {
                const newTab: ConsoleTab = {
                    id: prevTabs.length,
                    type: "query",
                    value: "",
                    bind_vars: [],
                    results: [],
                };
                setSelectedTabId(prevTabs.length);
                return [...prevTabs, newTab];
            });
        }
    }, []);

    const removeTab = useCallback((id: number) => {
        let removedIndex = -1;
        let newLength = 0;

        setTabs((prevTabs) => {
            const idx = prevTabs.findIndex((tab) => tab.id === id);
            if (idx === -1 || prevTabs.length <= 1) {
                removedIndex = -1;
                newLength = prevTabs.length;
                return prevTabs;
            }
            removedIndex = idx;
            const filtered = prevTabs.filter((_, i) => i !== idx);
            const reindexed = filtered.map((tab, i) => ({ ...tab, id: i }));
            newLength = reindexed.length;
            return reindexed;
        });

        setSelectedTabId((prevSelected) => {
            if (removedIndex === -1) return prevSelected + 2;
            if (prevSelected === removedIndex) {
                return Math.min(removedIndex, Math.max(0, newLength - 1));
            }
            if (prevSelected > removedIndex) {
                return Math.max(0, prevSelected - 1);
            }
            return prevSelected;
        });
    }, []);

    const selectTab = useCallback((id: number) => {
        setSelectedTabId(id);
    }, []);

    const updateTab = useCallback(
        (id: number, tabUpdate: Partial<ConsoleTab>) => {
            setTabs((prevTabs) =>
                prevTabs.map((tab) =>
                    tab.id === id
                        ? {
                              ...tab,
                              ...tabUpdate,
                              id: tab.id,
                          }
                        : tab,
                ),
            );
        },
        [],
    );

    useEffect(() => {
        const selectedTabExists = tabs.some((tab) => tab.id === selectedTabId);
        if (!selectedTabExists && tabs.length > 0) {
            setSelectedTabId(tabs[0].id);
        }
    }, [tabs, selectedTabId]);

    return {
        tabs,
        selectedTabId,
        addTab,
        removeTab,
        selectTab,
        updateTab,
    };
};
