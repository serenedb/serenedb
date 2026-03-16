import React from "react";
import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import { Textarea, getErrorMessage } from "@serene-ui/shared-frontend";
import { toast } from "sonner";

import { useUpdateDashboardCard } from "../../../../../entities/dashboard-card";
import { DashboardCardActions } from "./DashboardCardActions";

type DashboardTextBlock = Extract<DashboardBlockSchema, { type: "text" }>;

interface DashboardTextCardProps {
    block: DashboardTextBlock;
    dashboardId: number;
    onDelete?: () => void | Promise<void>;
    onDuplicate?: () => void | Promise<void>;
}

export const DashboardTextCard: React.FC<DashboardTextCardProps> = ({
    block,
    dashboardId,
    onDelete,
    onDuplicate,
}) => {
    const textareaRef = React.useRef<HTMLTextAreaElement | null>(null);
    const { mutateAsync: updateDashboardCard, isPending } =
        useUpdateDashboardCard();
    const [draftText, setDraftText] = React.useState(block.text);
    const [isEditing, setIsEditing] = React.useState(false);

    React.useEffect(() => {
        if (!isEditing) {
            setDraftText(block.text);
        }
    }, [block.text, isEditing]);

    React.useEffect(() => {
        if (!isEditing) {
            return;
        }

        const animationFrameId = window.requestAnimationFrame(() => {
            textareaRef.current?.focus();
            const textLength = textareaRef.current?.value.length ?? 0;
            textareaRef.current?.setSelectionRange(textLength, textLength);
        });

        return () => {
            window.cancelAnimationFrame(animationFrameId);
        };
    }, [isEditing]);

    const handleStartEditing = React.useCallback(() => {
        if (block.id < 0) {
            return;
        }

        setDraftText(block.text);
        setIsEditing(true);
    }, [block.id, block.text]);

    const handleCancelEditing = React.useCallback(() => {
        setDraftText(block.text);
        setIsEditing(false);
    }, [block.text]);

    const handleSave = React.useCallback(async () => {
        const nextText = draftText;

        if (!nextText.trim()) {
            setDraftText(block.text);
            setIsEditing(false);
            return;
        }

        if (nextText === block.text) {
            setIsEditing(false);
            return;
        }

        if (block.id < 0) {
            return;
        }

        try {
            await updateDashboardCard({
                dashboardId,
                card: {
                    id: block.id,
                    type: "text",
                    bounds: block.bounds,
                    text: nextText,
                },
            });
            setIsEditing(false);
        } catch (error) {
            toast.error("Failed to update text card", {
                description: getErrorMessage(
                    error,
                    "Failed to update text card",
                ),
            });
            setDraftText(block.text);
            setIsEditing(false);
        }
    }, [block, dashboardId, draftText, updateDashboardCard]);

    return (
        <div
            className="group bg-background border-1 rounded-xs relative flex flex-1 overflow-hidden"
            onDoubleClick={() => void handleStartEditing()}>
            <DashboardCardActions
                className="absolute top-2 right-2 z-10 opacity-0 transition-opacity group-hover:opacity-100 group-focus-within:opacity-100"
                onDelete={onDelete}
                onDuplicate={onDuplicate}
                onEdit={handleStartEditing}
            />
            <div className="flex min-h-0 flex-1 overflow-auto">
                {isEditing ? (
                    <Textarea
                        ref={textareaRef}
                        value={draftText}
                        disabled={isPending}
                        className="px-4 py-3 dashboard-text-editor min-h-full resize-none border-none bg-transparent text-sm shadow-none focus-visible:border-none focus-visible:ring-0"
                        onMouseDown={(event) => {
                            event.stopPropagation();
                        }}
                        onBlur={() => void handleSave()}
                        onChange={(event) => setDraftText(event.target.value)}
                        onKeyDown={(event) => {
                            if (event.key === "Escape") {
                                event.preventDefault();
                                handleCancelEditing();
                                return;
                            }

                            if (
                                event.key === "Enter" &&
                                (event.metaKey || event.ctrlKey)
                            ) {
                                event.preventDefault();
                                void handleSave();
                            }
                        }}
                    />
                ) : (
                    <p className="text-sm whitespace-pre-wrap break-words px-4 py-3">
                        {block.text}
                    </p>
                )}
            </div>
        </div>
    );
};
