import { type FC, useEffect, useState } from "react";
import { DockviewReact, type DockviewReadyEvent } from "dockview";
import {
    CONSOLE_EDITOR_PANEL_COMPONENT,
    CONSOLE_RESULTS_PANEL_COMPONENT,
    INITIAL_CONSOLE_EDITOR_PANELS,
    addEditorPanel,
} from "../model";
import { EditorPanel } from "./EditorPanel";
import { ConsoleEditorTopbar } from "./ConsoleEditorTopbar";
import { ResultsPanel } from "./ResultsPanel";

const CONSOLE_EDITOR_LAYOUT_STORAGE_KEY = "console:editor-layout";

const components = {
    [CONSOLE_EDITOR_PANEL_COMPONENT]: EditorPanel,
    [CONSOLE_RESULTS_PANEL_COMPONENT]: ResultsPanel,
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

        if (key === "runOnMountMode") {
            return;
        }

        sanitized[key] = sanitizeLayout(entry);
    });

    if (hasResults) {
        const selectedResultIndex = Number(sanitized.selectedResultIndex);
        const resultsLength = sanitizedResults?.length ?? 0;

        sanitized.selectedResultIndex =
            resultsLength > 0
                ? Number.isFinite(selectedResultIndex)
                    ? Math.min(Math.max(0, selectedResultIndex), resultsLength - 1)
                    : 0
                : 0;
    }

    return sanitized;
};

export const ConsoleEditor: FC = () => {
    const [api, setApi] = useState<DockviewReadyEvent["api"]>();

    const onReady = (event: DockviewReadyEvent) => {
        setApi(event.api);

        let restored = false;
        const rawLayout = localStorage.getItem(CONSOLE_EDITOR_LAYOUT_STORAGE_KEY);
        if (rawLayout) {
            try {
                event.api.fromJSON(sanitizeLayout(JSON.parse(rawLayout)));
                restored = true;
            } catch (error) {
                console.warn("Failed to restore console editor layout:", error);
            }
        }

        if (restored) {
            return;
        }

        Array.from({ length: INITIAL_CONSOLE_EDITOR_PANELS }).forEach(() => {
            addEditorPanel(event.api);
        });
    };

    useEffect(() => {
        if (!api) {
            return;
        }

        const disposable = api.onDidLayoutChange(() => {
            try {
                localStorage.setItem(
                    CONSOLE_EDITOR_LAYOUT_STORAGE_KEY,
                    JSON.stringify(sanitizeLayout(api.toJSON())),
                );
            } catch (error) {
                console.warn("Failed to save console editor layout:", error);
            }
        });

        return () => disposable.dispose();
    }, [api]);

    return (
        <div className="relative flex h-dvh w-full flex-col">
            <ConsoleEditorTopbar />
            <div className="flex-1 min-h-0">
                <DockviewReact onReady={onReady} components={components} />
            </div>
        </div>
    );
};
