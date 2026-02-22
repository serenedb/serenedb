import { Button } from "@serene-ui/shared-frontend/shared";
import { ImportIcon } from "lucide-react";
import { useRef, useState } from "react";
import {
    useAddSavedQuery,
    useGetSavedQueries,
} from "@serene-ui/shared-frontend/entities";
import { BindVarSchema } from "@serene-ui/shared-core";
import { toast } from "sonner";

type ImportedSavedQuery = {
    query: string;
    title?: string;
    name?: string;
    bind_vars?: BindVarSchema[];
};

export const ImportSavedQueriesButton = () => {
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [isImporting, setIsImporting] = useState(false);
    const { data: savedQueries = [] } = useGetSavedQueries();
    const { mutateAsync: addSavedQuery } = useAddSavedQuery();

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
            const fileText = await file.text();
            const parsedData: unknown = JSON.parse(fileText);
            const importCandidates = Array.isArray(parsedData)
                ? parsedData
                : [];

            const existingKeys = new Set(
                savedQueries.map(
                    (savedQuery) =>
                        `${savedQuery.name.trim()}::${savedQuery.query.trim()}`,
                ),
            );

            let addedCount = 0;
            let skippedCount = 0;

            for (const candidate of importCandidates) {
                const importedSavedQuery = candidate as ImportedSavedQuery;
                const title = (
                    importedSavedQuery.title ?? importedSavedQuery.name
                )?.trim();
                const query = importedSavedQuery.query?.trim();

                if (!title || !query) {
                    skippedCount += 1;
                    continue;
                }

                const savedQueryKey = `${title}::${query}`;
                if (existingKeys.has(savedQueryKey)) {
                    skippedCount += 1;
                    continue;
                }

                await addSavedQuery({
                    name: title,
                    query,
                    bind_vars: importedSavedQuery.bind_vars ?? [],
                    usage_count: 0,
                });

                existingKeys.add(savedQueryKey);
                addedCount += 1;
            }

            toast.success("Import completed", {
                description: `Added: ${addedCount}. Not added (duplicates/invalid): ${skippedCount}.`,
            });
        } catch (error) {
            console.error(error);
            toast.error("Failed to import saved queries", {
                description: "Please upload a valid JSON file.",
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
                accept="application/json,.json"
                className="hidden"
                onChange={handleFileChange}
            />
            <Button
                variant={"secondary"}
                size="iconSmall"
                onClick={handleOpenFilePicker}
                disabled={isImporting}>
                <ImportIcon />
            </Button>
        </>
    );
};
