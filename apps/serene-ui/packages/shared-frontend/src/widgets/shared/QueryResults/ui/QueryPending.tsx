import { LoaderIcon } from "@serene-ui/shared-frontend/shared";

export const QueryPending = () => {
    return (
        <div className="p-4 flex-1 flex items-center justify-center">
            <LoaderIcon className="w-6 h-6 animate-spin opacity-50" />
        </div>
    );
};
