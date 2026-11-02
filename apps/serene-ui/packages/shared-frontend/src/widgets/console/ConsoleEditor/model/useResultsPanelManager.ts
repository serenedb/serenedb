import { useCallback, useEffect, useRef } from "react";
import type { IDockviewPanelProps } from "dockview";
import { useNotifications } from "../../../../entities";
import type { ConsoleExecutionAlertMode } from "../../Console/model";
import {
    CONSOLE_RESULTS_PANEL_COMPONENT,
    createResultsPanelTitle,
    getResultsPanelId,
} from "./consts";
import type {
    EditorPanelParams,
    NormalizedEditorPanelParams,
    ResultsPanelParams,
} from "./types";
import { useResultsPanelVisibility } from "./useResultsPanelVisibility";

interface UseResultsPanelManagerParams {
    api: IDockviewPanelProps<EditorPanelParams>["api"];
    alertOnExecution: ConsoleExecutionAlertMode;
    containerApi: IDockviewPanelProps<EditorPanelParams>["containerApi"];
    getPanelState: () => NormalizedEditorPanelParams;
    spawnResultsInFirstTab: boolean;
}

const RESULTS_PANEL_SUFFIX = "__results";

const getPanelCreationTime = (panelId: string) => {
    const normalizedPanelId = panelId.endsWith(RESULTS_PANEL_SUFFIX)
        ? panelId.slice(0, -RESULTS_PANEL_SUFFIX.length)
        : panelId;
    const match = normalizedPanelId.match(/^console-panel-(\d+)-/);
    const createdAt = Number(match?.[1]);

    return Number.isFinite(createdAt) ? createdAt : Number.MAX_SAFE_INTEGER;
};

export const useResultsPanelManager = ({
    api,
    alertOnExecution,
    containerApi,
    getPanelState,
    spawnResultsInFirstTab,
}: UseResultsPanelManagerParams) => {
    const { addNotification } = useNotifications();
    const resultsPanelId = getResultsPanelId(api.id);
    const lastSyncedInitialStateRef = useRef<string>("");
    const getFirstResultsPanelId = useCallback(() => {
        const ids = containerApi.panels
            .map((panel) => panel.id)
            .filter((id) => id.endsWith(RESULTS_PANEL_SUFFIX));

        if (!ids.length) {
            return undefined;
        }

        return ids.sort((left, right) => {
            const leftCreatedAt = getPanelCreationTime(left);
            const rightCreatedAt = getPanelCreationTime(right);

            if (leftCreatedAt !== rightCreatedAt) {
                return leftCreatedAt - rightCreatedAt;
            }

            return left.localeCompare(right);
        })[0];
    }, [containerApi]);
    const resolveResultsPosition = useCallback(() => {
        if (spawnResultsInFirstTab) {
            const firstResultsPanelId = getFirstResultsPanelId();

            if (firstResultsPanelId && firstResultsPanelId !== resultsPanelId) {
                return {
                    referencePanel: firstResultsPanelId,
                    direction: "within" as const,
                };
            }
        }

        return {
            referencePanel: api.id,
            direction: "below" as const,
        };
    }, [api.id, getFirstResultsPanelId, resultsPanelId, spawnResultsInFirstTab]);
    const isResultsPanelVisible = useResultsPanelVisibility({
        containerApi,
        resultsPanelId,
    });
    const getResultsPanelVisibility = useCallback(() => {
        const panel = containerApi.getPanel(resultsPanelId);

        return panel?.api.isVisible ?? false;
    }, [containerApi, resultsPanelId]);

    const showResultsPanel = useCallback(
        (
            activate = false,
            initialState: NormalizedEditorPanelParams = getPanelState(),
        ) => {
            if (!initialState.results.length) {
                return;
            }

            const resultsPanelTitle = createResultsPanelTitle(api.title);
            const params: ResultsPanelParams = {
                sourcePanelId: api.id,
                initialState,
            };
            const existingPanel = containerApi.getPanel(resultsPanelId);

            if (existingPanel) {
                existingPanel.api.setTitle(resultsPanelTitle);
                existingPanel.api.updateParameters(params);
                if (activate) {
                    existingPanel.api.setActive();
                    return;
                }

                api.setActive();
                return;
            }

            containerApi.addPanel({
                id: resultsPanelId,
                component: CONSOLE_RESULTS_PANEL_COMPONENT,
                tabComponent: CONSOLE_RESULTS_PANEL_COMPONENT,
                title: resultsPanelTitle,
                params,
                position: resolveResultsPosition(),
            });

            if (activate) {
                const resultsPanel = containerApi.getPanel(resultsPanelId);
                resultsPanel?.api.setActive();
                return;
            }

            api.setActive();
        },
        [api, containerApi, getPanelState, resolveResultsPosition, resultsPanelId],
    );

    const notifyResultsReady = useCallback(
        (status: "success" | "failed") => {
            if (alertOnExecution === "never") {
                return;
            }

            const isPageHidden =
                typeof document !== "undefined" &&
                document.visibilityState === "hidden";
            const resultsPanelCurrentlyVisible = getResultsPanelVisibility();
            const isUnseen = !resultsPanelCurrentlyVisible || isPageHidden;

            if (alertOnExecution === "onlyUnseen" && !isUnseen) {
                return;
            }

            const queryTitle = api.title || "Query";
            const message =
                status === "failed"
                    ? `${queryTitle} finished with errors`
                    : `${queryTitle} results are ready`;

            addNotification({
                id:
                    Date.now() * 1000 +
                    Math.floor(Math.random() * 1000),
                message,
                type: status === "failed" ? "error" : "success",
                createdAt: Date.now(),
            });
        },
        [
            addNotification,
            alertOnExecution,
            api.title,
            getResultsPanelVisibility,
        ],
    );

    const hideResultsPanel = useCallback(() => {
        const currentPanel = containerApi.getPanel(resultsPanelId);
        if (currentPanel) {
            containerApi.removePanel(currentPanel);
        }
    }, [containerApi, resultsPanelId]);

    useEffect(() => {
        const resultsPanel = containerApi.getPanel(resultsPanelId);

        if (!resultsPanel) {
            lastSyncedInitialStateRef.current = "";
            return;
        }

        const sourceState = getPanelState();
        const nextInitialStateKey = JSON.stringify(sourceState);

        if (lastSyncedInitialStateRef.current === nextInitialStateKey) {
            return;
        }

        const currentParams =
            resultsPanel.api.getParameters<ResultsPanelParams>();
        const currentInitialStateKey = JSON.stringify(
            currentParams?.initialState ?? null,
        );

        if (
            currentParams?.sourcePanelId === api.id &&
            currentInitialStateKey === nextInitialStateKey
        ) {
            lastSyncedInitialStateRef.current = nextInitialStateKey;
            return;
        }

        resultsPanel.api.updateParameters({
            sourcePanelId: api.id,
            initialState: sourceState,
        });
        lastSyncedInitialStateRef.current = nextInitialStateKey;
    }, [api.id, containerApi, getPanelState, resultsPanelId]);

    return {
        isResultsPanelVisible,
        notifyResultsReady,
        showResultsPanel,
        hideResultsPanel,
    };
};
