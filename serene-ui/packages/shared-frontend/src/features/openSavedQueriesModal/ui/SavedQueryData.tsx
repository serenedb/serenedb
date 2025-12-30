import {
    Button,
    CrossIcon,
    SaveIcon,
    DeleteIcon,
    EditIcon,
    Input,
} from "@serene-ui/shared-frontend/shared";
import { useSavedQueriesModal } from "../model";
import { PGSQLEditor } from "@serene-ui/shared-frontend/widgets";
import { ExecuteQueryButton } from "../../executeQuery";
import { useState, useRef, useEffect } from "react";

export const SavedQueryData = () => {
    const {
        setOpen,
        currentSavedQuery,
        setCurrentSavedQuery,
        setJobId,
        handleSaveQuery,
        handleDeleteSavedQuery,
        setIsQueryRunning,
    } = useSavedQueriesModal();

    const [isEditing, setIsEditing] = useState(false);
    const inputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        if (isEditing) {
            inputRef.current?.focus();
        }
    }, [isEditing]);

    if (!currentSavedQuery) {
        return (
            <div className="flex flex-col flex-1 items-center justify-center">
                <p className="font-medium text-secondary-foreground">
                    Select query or create new
                </p>
            </div>
        );
    }

    return (
        <div className="flex flex-col flex-1">
            <div className="pl-4 pr-2 py-2 border-b border-border flex items-center justify-between">
                <div className="flex items-center gap-1">
                    <Input
                        value={currentSavedQuery?.name || ""}
                        variant="ghost"
                        size={Math.max(
                            1,
                            (currentSavedQuery?.name || "").length,
                        )}
                        onChange={(e) => {
                            setCurrentSavedQuery((prev) => ({
                                ...prev!,
                                name: e.target.value,
                            }));
                        }}
                        className="pr-0 pl-0 h-7"
                        disabled={!isEditing}
                        onBlur={() => setIsEditing(false)}
                        ref={inputRef}
                    />

                    <Button
                        variant="ghost"
                        size="iconSmall"
                        onClick={() => {
                            setIsEditing(true);
                        }}>
                        <EditIcon className="size-3.5" />
                    </Button>
                    <p className="text-sm font-medium">
                        {currentSavedQuery?.id === -1 && (
                            <span className="text-xs text-red-900">
                                (unsaved)
                            </span>
                        )}
                    </p>
                </div>
                <div className="flex gap-2">
                    <Button
                        variant="secondary"
                        size="iconSmall"
                        onClick={() => handleDeleteSavedQuery()}>
                        <DeleteIcon className="size-3" />
                    </Button>
                    <Button
                        variant="secondary"
                        size="iconSmall"
                        onClick={() => handleSaveQuery()}>
                        <SaveIcon className="size-3" />
                    </Button>
                    <Button
                        variant="secondary"
                        size="iconSmall"
                        onClick={() => setOpen(false)}>
                        <CrossIcon className="size-3" />
                    </Button>
                </div>
            </div>
            <div className="flex flex-col flex-1 bg-background pt-2 relative">
                <PGSQLEditor
                    value={currentSavedQuery?.query || ""}
                    onChange={(value) => {
                        setCurrentSavedQuery((prev) => ({
                            ...prev!,
                            query: value,
                        }));
                    }}
                />
                <div className="flex justify-end absolute bottom-2 right-5.5">
                    <ExecuteQueryButton
                        query={currentSavedQuery?.query || ""}
                        bind_vars={currentSavedQuery?.bind_vars || []}
                        saveToHistory={false}
                        handleJobId={setJobId}
                        onBeforeExecute={() => {
                            setIsQueryRunning(true);
                        }}
                    />
                </div>
            </div>
        </div>
    );
};
