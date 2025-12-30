import {
    ArrowDownIcon,
    Button,
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useQueryResults } from "../modal";
import { useConnection } from "@serene-ui/shared-frontend/entities";

interface ExecuteQueryButtonProps {
    query: string;
    bind_vars?: any[];
    saveToHistory?: boolean;
    limit?: number;
    handleJobId: (jobId: number) => void;
    onBeforeExecute?: () => void;
    onExecuteInNewTab?: () => void;
}

export const ExecuteQueryButton = ({
    query,
    bind_vars,
    saveToHistory,
    limit,
    handleJobId,
    onBeforeExecute,
    onExecuteInNewTab,
}: ExecuteQueryButtonProps) => {
    const { executeQuery } = useQueryResults();
    const { currentConnection } = useConnection();
    const disabled =
        !query ||
        !currentConnection.connectionId ||
        !currentConnection.database;
    return (
        <div className="inline-flex">
            <Button
                className="rounded-r-none"
                onClick={async () => {
                    onBeforeExecute?.();
                    const result = await executeQuery(
                        query,
                        bind_vars || [],
                        saveToHistory || false,
                        limit || 1000,
                    );
                    if (result.success) {
                        handleJobId(result.jobId);
                    }
                }}
                disabled={disabled}>
                Execute
            </Button>
            <DropdownMenu>
                <DropdownMenuTrigger asChild>
                    <Button
                        size="icon"
                        className="rounded-l-none border-l border-[#ffffff10] outline-none"
                        title="Execute options"
                        aria-label="Execute options"
                        disabled={disabled}>
                        <ArrowDownIcon />
                    </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-50 p-1">
                    <DropdownMenuItem
                        className="justify-between pr-1 pl-3"
                        onSelect={(event) => {
                            event.preventDefault();
                            if (!disabled && onExecuteInNewTab) {
                                onExecuteInNewTab();
                            }
                        }}>
                        Execute in new tab
                    </DropdownMenuItem>
                </DropdownMenuContent>
            </DropdownMenu>
        </div>
    );
};
