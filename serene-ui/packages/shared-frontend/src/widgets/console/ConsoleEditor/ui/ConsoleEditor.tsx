import React, { useCallback, useEffect, useRef } from "react";
import { toast } from "sonner";
import {
    ExecuteQueryButton,
    useConsoleLayout,
    useQueryResults,
} from "@serene-ui/shared-frontend/features";
import { Button, cn } from "@serene-ui/shared-frontend/shared";
import {
    ConsoleEditorTabsSelector,
    type ConsoleTab,
} from "../../ConsoleEditorTabsSelector";

import { PGSQLEditor } from "../../../shared/PGSQLEditor";
import { DropZone } from "../../../shared/DropZone";
import { useConnectionAutocomplete } from "../../../shared/PGSQLEditor/model";
import { OpenSavedQueriesModalButton } from "@serene-ui/shared-frontend/features";

interface ConsoleEditorProps {
    selectedTabId: number;
    tabs: ConsoleTab[];
    updateTab: (tabId: number, tabUpdate: Partial<ConsoleTab>) => void;
    selectTab: (tabId: number) => void;
    removeTab: (tabId: number) => void;
    addTab: (tabType: ConsoleTab["type"]) => void;
    addJobId: (jobId: number, tabId?: number) => void;
    limit: number;
    setLimit: (limit: number) => void;
    editorRef?: React.RefObject<HTMLElement | null>;
}

export const ConsoleEditor: React.FC<ConsoleEditorProps> = ({
    selectedTabId,
    tabs,
    updateTab,
    selectTab,
    removeTab,
    addTab,
    addJobId,
    limit,
    setLimit,
    editorRef,
}) => {
    const { isMaximized, toggleMaximizedResults } = useConsoleLayout();
    const { executeQuery } = useQueryResults();
    const autocomplete = useConnectionAutocomplete();

    const storybookPrefillAppliedRef = useRef(false);

    useEffect(() => {
        if (storybookPrefillAppliedRef.current) return;

        const key = "storybook:console:prefillQuery";
        const maxWaitMs = 2000;
        const intervalMs = 50;
        const start = Date.now();

        const intervalId = window.setInterval(() => {
            if (Date.now() - start > maxWaitMs) {
                window.clearInterval(intervalId);
                return;
            }

            const prefillQuery = localStorage.getItem(key);
            if (!prefillQuery) return;

            storybookPrefillAppliedRef.current = true;
            localStorage.removeItem(key);

            if (typeof prefillQuery === "string") {
                if (tabs[selectedTabId]?.value !== prefillQuery) {
                    updateTab(selectedTabId, {
                        value: prefillQuery,
                    });
                }
            }

            window.clearInterval(intervalId);
        }, intervalMs);

        return () => {
            window.clearInterval(intervalId);
        };
    }, [selectedTabId, tabs, updateTab]);

    const handleExecute = useCallback(async () => {
        const result = await executeQuery(
            tabs[selectedTabId].value,
            tabs[selectedTabId].bind_vars || [],
            true,
            limit,
        );
        if (result.success) {
            addJobId(result.jobId);
        }
    }, [tabs, selectedTabId, executeQuery, addJobId]);

    const handleExecuteInNewTab = useCallback(async () => {
        const currentQuery = tabs[selectedTabId].value;
        const currentBindVars = tabs[selectedTabId].bind_vars || [];

        const newTabIndex = tabs.length;
        addTab("query");
        updateTab(newTabIndex, {
            value: currentQuery,
            bind_vars: currentBindVars,
        });
        selectTab(newTabIndex);

        const result = await executeQuery(
            currentQuery,
            currentBindVars,
            true,
            limit,
        );
        if (result.success) {
            addJobId(result.jobId, newTabIndex);
        }
    }, [
        tabs,
        selectedTabId,
        executeQuery,
        addJobId,
        addTab,
        updateTab,
        selectTab,
    ]);

    const handleFilesDrop = useCallback(
        async (files: File[]) => {
            try {
                const fileContents = await Promise.all(
                    files.map(async (file) => ({
                        value: await file.text(),
                    })),
                );

                const firstNewTabId = tabs.length;

                fileContents.forEach((fileContent, index) => {
                    const nextTabId = firstNewTabId + index;
                    addTab("query");
                    updateTab(nextTabId, {
                        value: fileContent.value,
                    });
                });

                selectTab(firstNewTabId + fileContents.length - 1);
            } catch (error) {
                console.error(error);
                toast.error("Failed to open SQL file", {
                    description: "Please try dropping the file again.",
                });
            }
        },
        [addTab, selectTab, tabs.length, updateTab],
    );

    const handleRejectedFiles = useCallback((files: File[]) => {
        if (!files.length) return;

        toast.error("Unsupported file type", {
            description: "Only .sql files can be opened here.",
        });
    }, []);

    return (
        <DropZone
            supportedExtensions={["sql"]}
            onFilesDrop={handleFilesDrop}
            onRejectedFiles={handleRejectedFiles}
            className={cn("flex flex-col flex-1 h-full gap-1 relative pt-2")}>
            <ConsoleEditorTabsSelector
                tabs={tabs}
                selectedTabId={selectedTabId}
                selectTab={selectTab}
                removeTab={removeTab}
                addTab={addTab}
                limit={limit}
                setLimit={setLimit}
            />
            <div className="flex-1 h-full">
                <PGSQLEditor
                    ref={editorRef}
                    key={selectedTabId}
                    value={tabs[selectedTabId].value}
                    autocomplete={autocomplete}
                    onChange={(value) => {
                        updateTab(selectedTabId, {
                            value,
                        });
                    }}
                    onExecute={handleExecute}
                    onExecuteInNewTab={handleExecuteInNewTab}
                />
            </div>
            <div className="absolute bottom-2 right-5.5 flex gap-1">
                {isMaximized && (
                    <Button
                        variant="secondary"
                        size="icon"
                        onClick={() => {
                            toggleMaximizedResults();
                        }}>
                        X
                    </Button>
                )}
                <OpenSavedQueriesModalButton
                    query={tabs[selectedTabId].value}
                />
                <ExecuteQueryButton
                    handleJobId={addJobId}
                    query={tabs[selectedTabId].value}
                    bind_vars={tabs[selectedTabId].bind_vars}
                    limit={limit}
                    saveToHistory={true}
                    onExecuteInNewTab={handleExecuteInNewTab}
                />
            </div>
        </DropZone>
    );
};
