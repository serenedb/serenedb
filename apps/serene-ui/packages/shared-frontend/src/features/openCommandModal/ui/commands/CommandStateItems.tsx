import { CommandItem } from "@serene-ui/shared-frontend/shared";
import { Loader2, AlertCircle } from "lucide-react";

/**
 * A disabled command item that displays a loading spinner and message.
 * Used when async data is being fetched (e.g., connections, databases).
 */
export const CommandLoadingItem = () => {
    return (
        <CommandItem disabled className="py-2 flex items-center gap-2">
            <Loader2 className="h-4 w-4 animate-spin" />
            <span className="text-secondary-foreground">Loading...</span>
        </CommandItem>
    );
};

export interface CommandErrorItemProps {
    error?: Error | null;
    message?: string;
}

/**
 * A disabled command item that displays an error icon and message.
 * Used when async data fails to load.
 */
export const CommandErrorItem = ({ error, message }: CommandErrorItemProps) => {
    const errorMessage = message || error?.message || "An error occurred";

    return (
        <CommandItem disabled className="py-2 flex items-center gap-2">
            <AlertCircle className="h-4 w-4 text-destructive" />
            <span className="text-destructive">{errorMessage}</span>
        </CommandItem>
    );
};
