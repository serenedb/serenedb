import { Button, cn, ImportIcon } from "@serene-ui/shared-frontend/shared";
import { useRef, useState } from "react";
import { useSavedQueriesModal } from "../../model";
import { toast } from "sonner";
import {
    useAddSavedQuery,
    useGetSavedQueries,
} from "@serene-ui/shared-frontend/entities";
import type { SavedQuerySchema } from "@serene-ui/shared-core";

const MAX_SQL_FILE_SIZE_BYTES = 1024 * 1024;
const MAX_SAVED_QUERY_NAME_LENGTH = 255;

const normalizeBaseName = (fileName: string) => {
    const trimmed = fileName.trim();
    const withoutExtension = trimmed.replace(/\.sql$/i, "").trim();
    const fallback = withoutExtension || "saved-query";
    return fallback.slice(0, MAX_SAVED_QUERY_NAME_LENGTH);
};

const getUniqueSavedQueryName = (
    fileName: string,
    savedQueries: SavedQuerySchema[],
) => {
    const existingNames = new Set(
        savedQueries.map((query) => query.name.trim().toLowerCase()),
    );
    const baseName = normalizeBaseName(fileName);
    const baseNameLower = baseName.toLowerCase();

    if (!existingNames.has(baseNameLower)) {
        return baseName;
    }

    let suffixIndex = 1;
    while (true) {
        const suffix = ` (${suffixIndex})`;
        const maxBaseLength = MAX_SAVED_QUERY_NAME_LENGTH - suffix.length;
        const nextName = `${baseName.slice(0, maxBaseLength).trimEnd()}${suffix}`;

        if (!existingNames.has(nextName.toLowerCase())) {
            return nextName;
        }

        suffixIndex += 1;
    }
};

interface ImportSavedQueryButtonProps {
    className?: React.ComponentProps<typeof Button>["className"];
    size?: React.ComponentProps<typeof Button>["size"];
    variant?: React.ComponentProps<typeof Button>["variant"];
}

export const ImportSavedQueryButton: React.FC<ImportSavedQueryButtonProps> = ({
    className,
    size = "iconSmall",
    variant = "secondary",
}) => {
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [isImporting, setIsImporting] = useState(false);
    const { data: savedQueries } = useGetSavedQueries();
    const { mutateAsync: addSavedQuery } = useAddSavedQuery();
    const { setCurrentSavedQuery } = useSavedQueriesModal();

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

            if (file.size > MAX_SQL_FILE_SIZE_BYTES) {
                toast.error("Failed to import query", {
                    description: `Selected SQL file is too large. Maximum allowed size is ${MAX_SQL_FILE_SIZE_BYTES} bytes.`,
                });
                return;
            }

            const query = await file.text();

            if (!query.trim()) {
                toast.error("Failed to import query", {
                    description: "Selected SQL file is empty.",
                });
                return;
            }

            const queryName = getUniqueSavedQueryName(
                file.name,
                savedQueries ?? [],
            );
            const createdSavedQuery = await addSavedQuery({
                name: queryName,
                query,
                bind_vars: [],
                usage_count: 0,
            });
            setCurrentSavedQuery(createdSavedQuery);

            toast.success("Query imported", {
                description: `Created saved query "${createdSavedQuery.name}".`,
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
                variant={variant}
                size={size}
                onClick={handleOpenFilePicker}
                title="Upload query"
                className={cn(
                    className,
                    "text-foreground/50 hover:text-foreground bg-transparent hover:bg-black/5 dark:hover:bg-white/5 transition-none duration-0",
                )}
                disabled={isImporting}>
                <ImportIcon className="size-3" />
            </Button>
        </>
    );
};
