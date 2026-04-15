import { Button } from "@serene-ui/shared-frontend/shared";
import { useSavedQueriesModal } from "../../model/SavedQueriesModalContext";
import { BindVarSchema } from "@serene-ui/shared-core";

interface OpenSavedQueriesModalButtonProps {
    className?: React.ComponentProps<typeof Button>["className"];
    query?: string;
    bindVars?: BindVarSchema[];
}

export const OpenSavedQueriesModalButton: React.FC<
    OpenSavedQueriesModalButtonProps
> = ({ className, query, bindVars, ...props }) => {
    const { openCreateModal } = useSavedQueriesModal();

    const handleOpenModal = () => {
        openCreateModal({
            query: query || "",
            bindVars: bindVars || [],
        });
    };

    return (
        <Button
            onClick={handleOpenModal}
            disabled={!query}
            variant="secondary"
            className={className}
            {...props}>
            Save query
        </Button>
    );
};
