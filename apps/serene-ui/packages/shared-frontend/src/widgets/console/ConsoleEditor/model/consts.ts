import type { EditorPanelParams } from "./types";

export const CONSOLE_EDITOR_PANEL_COMPONENT = "editor";
export const CONSOLE_RESULTS_PANEL_COMPONENT = "results";
export const INITIAL_CONSOLE_EDITOR_PANELS = 2;

export const createPanelId = () =>
    `console-panel-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

export const createPanelTitle = (index: number) => `Query ${index}`;

export const createResultsPanelTitle = (sourceTitle?: string) =>
    sourceTitle ? `${sourceTitle} - Results` : "Results";

export const getResultsPanelId = (editorPanelId: string) =>
    `${editorPanelId}__results`;

export const createEditorPanelParams = (
    overrides: Partial<EditorPanelParams> = {},
): EditorPanelParams => ({
    query: "",
    results: [],
    selectedResultIndex: 0,
    highlightJobIds: [],
    tabExecutionStatus: "",
    ...overrides,
});
