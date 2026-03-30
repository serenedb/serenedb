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

export const ConsoleEditor: FC = () => {
    const [api, setApi] = useState<DockviewReadyEvent["api"]>();

    const onReady = (event: DockviewReadyEvent) => {
        setApi(event.api);

        let restored = false;
        const rawLayout = localStorage.getItem(CONSOLE_EDITOR_LAYOUT_STORAGE_KEY);
        if (rawLayout) {
            try {
                event.api.fromJSON(JSON.parse(rawLayout));
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
                    JSON.stringify(api.toJSON()),
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
