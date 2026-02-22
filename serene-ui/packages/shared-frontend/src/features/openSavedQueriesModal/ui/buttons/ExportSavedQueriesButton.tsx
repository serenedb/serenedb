import { Button } from "@serene-ui/shared-frontend/shared";
import { DownloadIcon } from "lucide-react";
import { useGetSavedQueries } from "@serene-ui/shared-frontend/entities";

export const ExportSavedQueriesButton = () => {
    const { data: savedQueries = [] } = useGetSavedQueries();

    const handleExport = () => {
        const exportPayload = savedQueries.map((savedQuery) => ({
            title: savedQuery.name,
            query: savedQuery.query,
            bind_vars: savedQuery.bind_vars ?? [],
        }));

        const blob = new Blob([JSON.stringify(exportPayload, null, 2)], {
            type: "application/json;charset=utf-8",
        });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");

        try {
            link.href = url;
            link.download = "saved-queries.json";
            document.body.appendChild(link);
            link.click();
        } finally {
            link.remove();
            URL.revokeObjectURL(url);
        }
    };

    return (
        <Button variant={"secondary"} size="iconSmall" onClick={handleExport}>
            <DownloadIcon />
        </Button>
    );
};
