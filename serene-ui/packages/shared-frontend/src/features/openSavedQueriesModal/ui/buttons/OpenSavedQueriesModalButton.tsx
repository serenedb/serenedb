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
    const { setOpen, setCurrentSavedQuery } = useSavedQueriesModal();

    const handleOpenModal = () => {
        setCurrentSavedQuery({
            id: -1,
            name: "Untitled",
            query: query || "",
            bind_vars: bindVars || [],
            usage_count: 0,
        });
        setOpen(true);
    };

    return (
        <Button
            onClick={handleOpenModal}
            disabled={!query}
            variant="thirdly"
            className={className}
            {...props}>
            Save
        </Button>
    );
};
