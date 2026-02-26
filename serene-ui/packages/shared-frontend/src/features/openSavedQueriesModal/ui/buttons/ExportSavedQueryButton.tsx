import { Button, ExportIcon } from "@serene-ui/shared-frontend/shared";
import { useSavedQueriesModal } from "../../model";

export const ExportSavedQueryButton = () => {
    const { currentSavedQuery } = useSavedQueriesModal();

    const handleExport = () => {
        if (!currentSavedQuery) return;

        const baseName = currentSavedQuery.name.trim() || "saved-query";
        const sanitizedName = baseName
            .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_")
            .trim();

        const blob = new Blob([currentSavedQuery.query ?? ""], {
            type: "text/sql;charset=utf-8",
        });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");

        try {
            link.href = url;
            link.download = `${sanitizedName || "saved-query"}.sql`;
            document.body.appendChild(link);
            link.click();
        } finally {
            link.remove();
            URL.revokeObjectURL(url);
        }
    };

    return (
        <Button
            variant={"secondary"}
            size="iconSmall"
            onClick={handleExport}
            disabled={!currentSavedQuery || !currentSavedQuery.query}>
            <ExportIcon />
        </Button>
    );
};
