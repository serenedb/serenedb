import { Button, ImportIcon } from "@serene-ui/shared-frontend/shared";
import { useRef, useState } from "react";
import { useSavedQueriesModal } from "../../model";
import { toast } from "sonner";

export const ImportSavedQueryButton = () => {
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [isImporting, setIsImporting] = useState(false);
    const { currentSavedQuery, setCurrentSavedQuery } = useSavedQueriesModal();

    const handleOpenFilePicker = () => {
        fileInputRef.current?.click();
    };

    const handleFileChange = async (
        event: React.ChangeEvent<HTMLInputElement>,
    ) => {
        const file = event.target.files?.[0];
        event.target.value = "";

        if (!file) return;

        try {
            setIsImporting(true);
            const query = await file.text();

            if (!query.trim()) {
                toast.error("Failed to import query", {
                    description: "Selected SQL file is empty.",
                });
                return;
            }

            setCurrentSavedQuery((prev) => {
                if (!prev) return prev;
                return {
                    ...prev,
                    query,
                };
            });

            toast.success("Query imported", {
                description: "Selected query was updated from SQL file.",
            });
        } catch (error) {
            console.error(error);
            toast.error("Failed to import query", {
                description: "Please upload a valid SQL file.",
            });
        } finally {
            setIsImporting(false);
        }
    };

    return (
        <>
            <input
                ref={fileInputRef}
                type="file"
                accept="text/sql,.sql"
                className="hidden"
                onChange={handleFileChange}
            />
            <Button
                variant={"secondary"}
                size="iconSmall"
                onClick={handleOpenFilePicker}
                disabled={isImporting || !currentSavedQuery}>
                <ImportIcon />
            </Button>
        </>
    );
};
