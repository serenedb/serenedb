import { useCallback } from "react";
import type { DockviewApi, DockviewGroupPanel } from "dockview";
import {
    DEFAULT_HOTKEYS,
    useAppHotkey,
} from "../../../../shared/hotkeys";
import { addEditorPanel } from "./utils";

type ConsoleEditorDirection = "up" | "down" | "left" | "right";

interface GroupRect {
    group: DockviewGroupPanel;
    top: number;
    right: number;
    bottom: number;
    left: number;
    centerX: number;
    centerY: number;
}

interface DirectionalCandidateScore {
    alignmentRank: number;
    gap: number;
    orthDistance: number;
    overlap: number;
}

const getAxisOverlap = (
    startA: number,
    endA: number,
    startB: number,
    endB: number,
) => Math.max(0, Math.min(endA, endB) - Math.max(startA, startB));

const getGroupRect = (group: DockviewGroupPanel): GroupRect | null => {
    if (!group.isVisible) {
        return null;
    }

    const rect = group.element.getBoundingClientRect();
    if (rect.width <= 0 || rect.height <= 0) {
        return null;
    }

    return {
        group,
        top: rect.top,
        right: rect.right,
        bottom: rect.bottom,
        left: rect.left,
        centerX: rect.left + rect.width / 2,
        centerY: rect.top + rect.height / 2,
    };
};

const getDirectionalCandidateScore = (
    current: GroupRect,
    candidate: GroupRect,
    direction: ConsoleEditorDirection,
): DirectionalCandidateScore | null => {
    if (direction === "left") {
        const gap = current.left - candidate.right;
        if (gap < 0) {
            return null;
        }

        return {
            alignmentRank:
                getAxisOverlap(
                    current.top,
                    current.bottom,
                    candidate.top,
                    candidate.bottom,
                ) > 0
                    ? 0
                    : 1,
            gap,
            orthDistance: Math.abs(current.centerY - candidate.centerY),
            overlap: getAxisOverlap(
                current.top,
                current.bottom,
                candidate.top,
                candidate.bottom,
            ),
        };
    }

    if (direction === "right") {
        const gap = candidate.left - current.right;
        if (gap < 0) {
            return null;
        }

        return {
            alignmentRank:
                getAxisOverlap(
                    current.top,
                    current.bottom,
                    candidate.top,
                    candidate.bottom,
                ) > 0
                    ? 0
                    : 1,
            gap,
            orthDistance: Math.abs(current.centerY - candidate.centerY),
            overlap: getAxisOverlap(
                current.top,
                current.bottom,
                candidate.top,
                candidate.bottom,
            ),
        };
    }

    if (direction === "up") {
        const gap = current.top - candidate.bottom;
        if (gap < 0) {
            return null;
        }

        return {
            alignmentRank:
                getAxisOverlap(
                    current.left,
                    current.right,
                    candidate.left,
                    candidate.right,
                ) > 0
                    ? 0
                    : 1,
            gap,
            orthDistance: Math.abs(current.centerX - candidate.centerX),
            overlap: getAxisOverlap(
                current.left,
                current.right,
                candidate.left,
                candidate.right,
            ),
        };
    }

    const gap = candidate.top - current.bottom;
    if (gap < 0) {
        return null;
    }

    return {
        alignmentRank:
            getAxisOverlap(
                current.left,
                current.right,
                candidate.left,
                candidate.right,
            ) > 0
                ? 0
                : 1,
        gap,
        orthDistance: Math.abs(current.centerX - candidate.centerX),
        overlap: getAxisOverlap(
            current.left,
            current.right,
            candidate.left,
            candidate.right,
        ),
    };
};

const focusGroup = (group: DockviewGroupPanel) => {
    const panelToFocus = group.activePanel ?? group.panels[0];

    if (panelToFocus) {
        panelToFocus.api.setActive();
        return;
    }

    group.focus();
};

export const focusAdjacentConsoleEditorGroup = (
    api: DockviewApi,
    direction: ConsoleEditorDirection,
) => {
    const currentGroup = api.activeGroup ?? api.activePanel?.group;
    if (!currentGroup) {
        return;
    }

    const currentRect = getGroupRect(currentGroup);
    if (!currentRect) {
        return;
    }

    const nextGroup = api.groups
        .filter((group) => group.id !== currentGroup.id)
        .map((group) => getGroupRect(group))
        .filter((group): group is GroupRect => group !== null)
        .map((group) => ({
            group,
            score: getDirectionalCandidateScore(currentRect, group, direction),
        }))
        .filter(
            (
                candidate,
            ): candidate is {
                group: GroupRect;
                score: DirectionalCandidateScore;
            } => candidate.score !== null,
        )
        .sort(
            (left, right) =>
                left.score.alignmentRank - right.score.alignmentRank ||
                left.score.gap - right.score.gap ||
                left.score.orthDistance - right.score.orthDistance ||
                right.score.overlap - left.score.overlap,
        )[0]?.group.group;

    if (nextGroup) {
        focusGroup(nextGroup);
    }
};

export const useConsoleEditorHotkeys = (api?: DockviewApi) => {
    const handleNextTab = useCallback(() => {
        if (!api) {
            return;
        }

        api.moveToNext({
            includePanel: true,
            group: api.activeGroup ?? api.activePanel?.group,
        });
    }, [api]);

    const handlePreviousTab = useCallback(() => {
        if (!api) {
            return;
        }

        api.moveToPrevious({
            includePanel: true,
            group: api.activeGroup ?? api.activePanel?.group,
        });
    }, [api]);

    const handleNewTab = useCallback(() => {
        if (!api) {
            return;
        }

        const activeGroup = api.activeGroup ?? api.activePanel?.group;
        addEditorPanel(
            api,
            {},
            activeGroup ? { position: { referenceGroup: activeGroup } } : {},
        );
    }, [api]);

    const handleCloseTab = useCallback(() => {
        api?.activePanel?.api.close();
    }, [api]);

    const handleFocusLeft = useCallback(() => {
        if (!api) {
            return;
        }

        focusAdjacentConsoleEditorGroup(api, "left");
    }, [api]);

    const handleFocusRight = useCallback(() => {
        if (!api) {
            return;
        }

        focusAdjacentConsoleEditorGroup(api, "right");
    }, [api]);

    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_NEXT_TAB,
        handleNextTab,
        [handleNextTab],
    );
    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_PREVIOUS_TAB,
        handlePreviousTab,
        [handlePreviousTab],
    );
    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_NEW_TAB, handleNewTab, [handleNewTab]);
    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_CLOSE_TAB,
        handleCloseTab,
        [handleCloseTab],
    );
    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_FOCUS_WINDOW_LEFT,
        handleFocusLeft,
        [handleFocusLeft],
    );
    useAppHotkey(
        DEFAULT_HOTKEYS.CONSOLE_FOCUS_WINDOW_RIGHT,
        handleFocusRight,
        [handleFocusRight],
    );
};
