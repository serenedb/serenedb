import {
    Button,
    Dialog,
    DialogContent,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    Input,
} from "@serene-ui/shared-frontend/shared";
import { PGSQLEditor } from "@serene-ui/shared-frontend/widgets";
import { useSavedQueriesModal } from "../model";

export const SavedQueriesModal = () => {
    const {
        open,
        setOpen,
        modalMode,
        currentSavedQuery,
        setCurrentSavedQuery,
        handleSaveQuery,
    } = useSavedQueriesModal();

    const isEditMode = modalMode === "edit";
    const name = currentSavedQuery?.name ?? "";
    const canSave = name.trim().length > 0;

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogContent className={isEditMode ? "sm:max-w-3xl" : "sm:max-w-md"}>
                <DialogHeader>
                    <DialogTitle>
                        {isEditMode ? "Edit saved query" : "Save query"}
                    </DialogTitle>
                </DialogHeader>

                <div className="flex flex-col gap-3">
                    <label className="flex flex-col gap-1.5">
                        <span className="text-sm text-muted-foreground">Name</span>
                        <Input
                            value={name}
                            onChange={(event) => {
                                const nextName = event.target.value;
                                setCurrentSavedQuery((prev) =>
                                    prev ? { ...prev, name: nextName } : prev,
                                );
                            }}
                            placeholder="Query name"
                            autoFocus
                        />
                    </label>

                    {isEditMode ? (
                        <div className="h-80 overflow-hidden rounded-md border border-border">
                            <PGSQLEditor
                                value={currentSavedQuery?.query ?? ""}
                                onChange={(query) => {
                                    setCurrentSavedQuery((prev) =>
                                        prev ? { ...prev, query } : prev,
                                    );
                                }}
                            />
                        </div>
                    ) : null}
                </div>

                <DialogFooter>
                    <Button variant="secondary" onClick={() => setOpen(false)}>
                        Cancel
                    </Button>
                    <Button disabled={!canSave} onClick={() => void handleSaveQuery()}>
                        Save
                    </Button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
};
