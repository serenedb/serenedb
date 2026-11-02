import { RefreshCwIcon } from "lucide-react";
import { useDatabases } from "../../../entities";
import { Button } from "../../../shared";
import { toast } from "sonner";

export const RefreshDatabasesButton = () => {
    const { refetchDatabases } = useDatabases();
    const handleRefresh = async () => {
        try {
            await refetchDatabases();
            toast.success("Databases refreshed");
        } catch (error) {
            toast.error("Failed to refresh databases");
        }
    };

    return (
        <Button
            size="icon"
            variant="secondary"
            onClick={handleRefresh}
            aria-label="Refresh databases">
            <RefreshCwIcon />
        </Button>
    );
};
