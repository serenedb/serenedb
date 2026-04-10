import { Button, cn, ExportIcon } from "@serene-ui/shared-frontend/shared";
import { useSavedQueriesModal } from "../../model";
import type { SavedQuerySchema } from "@serene-ui/shared-core";

interface ExportSavedQueryButtonProps {
    className?: React.ComponentProps<typeof Button>["className"];
    size?: React.ComponentProps<typeof Button>["size"];
    variant?: React.ComponentProps<typeof Button>["variant"];
    savedQuery?: Pick<SavedQuerySchema, "name" | "query">;
}

export const ExportSavedQueryButton: React.FC<ExportSavedQueryButtonProps> = ({
    className,
    size = "iconSmall",
    variant = "secondary",
    savedQuery,
}) => {
    const { currentSavedQuery } = useSavedQueriesModal();
    const targetSavedQuery = savedQuery ?? currentSavedQuery;

    const handleExport = () => {
        if (!targetSavedQuery) return;

        const baseName = targetSavedQuery.name.trim() || "saved-query";
        const sanitizedName = baseName
            .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_")
            .trim();

        const blob = new Blob([targetSavedQuery.query ?? ""], {
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
            variant={variant}
            size={size}
            onClick={handleExport}
            title="Download query"
            className={cn(
                className,
                "text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0",
            )}
            disabled={!targetSavedQuery || !targetSavedQuery.query}>
            <ExportIcon className="size-3" />
        </Button>
    );
};
