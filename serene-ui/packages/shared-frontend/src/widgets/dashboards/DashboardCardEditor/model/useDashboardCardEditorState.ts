import React from "react";
import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import {
    toDashboardCardUpdateInput,
    useUpdateDashboardCard,
} from "../../../../entities/dashboard-card";
import { getErrorMessage } from "../../../../shared";
import { toast } from "sonner";
import { isDashboardQueryBlock } from "./dashboardCardBlock";

const DASHBOARD_CARD_EDITOR_AUTOSAVE_DELAY_MS = 500;
const DASHBOARD_CARD_EDITOR_QUERY_DEBOUNCE_MS = 2000;

interface UseDashboardCardEditorStateProps {
    editedBlock?: DashboardBlockSchema | null;
    onClose?: () => void;
    onEditedBlockChange?: (block: DashboardBlockSchema | null) => void;
}

export const useDashboardCardEditorState = ({
    editedBlock,
    onClose,
    onEditedBlockChange,
}: UseDashboardCardEditorStateProps) => {
    const { mutateAsync: updateDashboardCard } = useUpdateDashboardCard();
    const latestEditedBlockRef = React.useRef<DashboardBlockSchema | null>(null);
    const previousEditedBlockIdRef = React.useRef<number | null>(null);
    const pendingQueryBlockRef = React.useRef<Extract<
        DashboardBlockSchema,
        | { type: "table" }
        | { type: "single_string" }
        | { type: "bar_chart" }
        | { type: "line_chart" }
        | { type: "pie_chart" }
    > | null>(null);
    const currentBlockIdRef = React.useRef<number | null>(null);
    const lastPersistedRef = React.useRef<string | null>(null);
    const pendingBlockRef = React.useRef<DashboardBlockSchema | null>(null);
    const isPersistingRef = React.useRef(false);
    const [queryDraft, setQueryDraft] = React.useState<{
        blockId: number | null;
        value: string;
    }>({
        blockId: null,
        value: "",
    });

    React.useEffect(() => {
        latestEditedBlockRef.current = editedBlock ?? null;

        if (
            editedBlock &&
            isDashboardQueryBlock(editedBlock) &&
            queryDraft.blockId === editedBlock.id
        ) {
            pendingQueryBlockRef.current = editedBlock;
        }
    }, [editedBlock, queryDraft.blockId]);

    const flushPendingQuery = React.useCallback(() => {
        if (queryDraft.blockId === null) {
            return false;
        }

        const latestBlock = latestEditedBlockRef.current;
        const sourceBlock =
            latestBlock &&
            isDashboardQueryBlock(latestBlock) &&
            latestBlock.id === queryDraft.blockId
                ? latestBlock
                : pendingQueryBlockRef.current;

        if (!sourceBlock || sourceBlock.id !== queryDraft.blockId) {
            return false;
        }

        if (sourceBlock.query === queryDraft.value) {
            return false;
        }

        onEditedBlockChange?.({
            ...sourceBlock,
            query: queryDraft.value,
        });

        return true;
    }, [onEditedBlockChange, queryDraft]);

    React.useEffect(() => {
        const nextBlockId = editedBlock?.id ?? null;

        if (
            previousEditedBlockIdRef.current !== null &&
            previousEditedBlockIdRef.current !== nextBlockId
        ) {
            flushPendingQuery();
        }

        previousEditedBlockIdRef.current = nextBlockId;
    }, [editedBlock?.id, flushPendingQuery]);

    React.useEffect(() => {
        if (editedBlock && isDashboardQueryBlock(editedBlock)) {
            setQueryDraft((currentState) => {
                if (
                    currentState.blockId === editedBlock.id &&
                    currentState.value === editedBlock.query
                ) {
                    return currentState;
                }

                return {
                    blockId: editedBlock.id,
                    value: editedBlock.query,
                };
            });

            return;
        }

        setQueryDraft((currentState) => {
            if (currentState.blockId === null && currentState.value === "") {
                return currentState;
            }

            return {
                blockId: null,
                value: "",
            };
        });
    }, [
        editedBlock?.id,
        editedBlock?.type,
        editedBlock && isDashboardQueryBlock(editedBlock)
            ? editedBlock.query
            : undefined,
    ]);

    React.useEffect(() => {
        if (
            !editedBlock ||
            !isDashboardQueryBlock(editedBlock) ||
            queryDraft.blockId !== editedBlock.id ||
            queryDraft.value === editedBlock.query
        ) {
            return;
        }

        const timeoutId = window.setTimeout(() => {
            flushPendingQuery();
        }, DASHBOARD_CARD_EDITOR_QUERY_DEBOUNCE_MS);

        return () => {
            window.clearTimeout(timeoutId);
        };
    }, [editedBlock, flushPendingQuery, queryDraft]);

    const displayEditedBlock = React.useMemo(() => {
        if (
            !editedBlock ||
            !isDashboardQueryBlock(editedBlock) ||
            queryDraft.blockId !== editedBlock.id
        ) {
            return editedBlock;
        }

        if (editedBlock.query === queryDraft.value) {
            return editedBlock;
        }

        return {
            ...editedBlock,
            query: queryDraft.value,
        };
    }, [editedBlock, queryDraft]);

    const getPersistKey = React.useCallback((block: DashboardBlockSchema) => {
        return `${block.id}:${JSON.stringify(toDashboardCardUpdateInput(block))}`;
    }, []);

    const flushPersist = React.useCallback(async () => {
        const nextBlock = pendingBlockRef.current;

        if (!nextBlock || nextBlock.id < 0 || isPersistingRef.current) {
            return;
        }

        const persistKey = getPersistKey(nextBlock);

        if (persistKey === lastPersistedRef.current) {
            pendingBlockRef.current = null;
            return;
        }

        isPersistingRef.current = true;

        try {
            await updateDashboardCard({
                dashboardId: nextBlock.dashboard_id,
                card: {
                    ...toDashboardCardUpdateInput(nextBlock),
                    id: nextBlock.id,
                },
            });

            lastPersistedRef.current = persistKey;
        } catch (error) {
            toast.error("Failed to update dashboard card", {
                description: getErrorMessage(
                    error,
                    "Failed to update dashboard card.",
                ),
            });
        } finally {
            isPersistingRef.current = false;

            if (!pendingBlockRef.current) {
                return;
            }

            const pendingPersistKey = getPersistKey(pendingBlockRef.current);

            if (pendingPersistKey === lastPersistedRef.current) {
                pendingBlockRef.current = null;
                return;
            }

            if (pendingPersistKey !== persistKey) {
                void flushPersist();
            }
        }
    }, [getPersistKey, updateDashboardCard]);

    React.useEffect(() => {
        if (!editedBlock) {
            currentBlockIdRef.current = null;
            lastPersistedRef.current = null;
            pendingBlockRef.current = null;
            return;
        }

        if (currentBlockIdRef.current === editedBlock.id) {
            return;
        }

        currentBlockIdRef.current = editedBlock.id;
        lastPersistedRef.current = getPersistKey(editedBlock);
        pendingBlockRef.current = null;
    }, [editedBlock, getPersistKey]);

    React.useEffect(() => {
        if (!editedBlock || editedBlock.id < 0) {
            return;
        }

        const persistKey = getPersistKey(editedBlock);

        if (persistKey === lastPersistedRef.current) {
            return;
        }

        pendingBlockRef.current = editedBlock;

        const timeoutId = window.setTimeout(() => {
            void flushPersist();
        }, DASHBOARD_CARD_EDITOR_AUTOSAVE_DELAY_MS);

        return () => {
            window.clearTimeout(timeoutId);
        };
    }, [editedBlock, flushPersist, getPersistKey]);

    const handleClose = React.useCallback(() => {
        const hasFlushedPendingQuery = flushPendingQuery();

        if (hasFlushedPendingQuery) {
            window.setTimeout(() => {
                onClose?.();
            }, 0);

            return;
        }

        onClose?.();
    }, [flushPendingQuery, onClose]);

    return {
        displayEditedBlock,
        handleClose,
        queryDraft,
        setQueryDraft,
    };
};
