import { DownloadResultsButton } from "@serene-ui/shared-frontend/features";
import {
    Button,
    Input,
    Popover,
    PopoverContent,
    PopoverTrigger,
    Tabs,
    TabsList,
    TabsTrigger,
    TreeColumnsIcon,
    cn,
} from "@serene-ui/shared-frontend/shared";
import { useMemo, useState } from "react";
import { TimelineCard, type TimelineItem } from "../../TimelineCard";

interface QueryResultsFooterProps {
    children: React.ReactNode;
    results: {
        status: "success" | "failed" | "pending" | "running" | "";
        statementQuery?: string;
    }[];
    selectedResultIndex: number;
    onSelectResult?: (index: number) => void;
    rows?: Record<string, unknown>[];
    created_at?: string;
    execution_started_at?: string;
    execution_finished_at?: string;
    received_at?: string;
}

export const QueryResultsFooter: React.FC<QueryResultsFooterProps> = ({
    children,
    results,
    selectedResultIndex,
    onSelectResult,
    rows,
    created_at,
    execution_started_at,
    execution_finished_at,
    received_at,
}) => {
    const [isPopoverOpen, setIsPopoverOpen] = useState(false);
    const [searchValue, setSearchValue] = useState("");
    const queueTime =
        created_at && execution_started_at
            ? new Date(execution_started_at).getTime() -
              new Date(created_at).getTime()
            : 0;
    const execTime =
        execution_started_at && execution_finished_at
            ? new Date(execution_finished_at).getTime() -
              new Date(execution_started_at).getTime()
            : 0;
    const transferTime =
        execution_finished_at && received_at
            ? new Date(received_at).getTime() -
              new Date(execution_finished_at).getTime()
            : 0;

    const executionTime = execTime > 0 ? execTime : null;
    const canGoPrevious = selectedResultIndex > 0;
    const canGoNext = selectedResultIndex < results.length - 1;

    const timelineItems: TimelineItem[] = [
        { name: "Queue", time: queueTime, color: "rgb(234, 179, 8)" },
        { name: "Execution", time: execTime, color: "rgb(34, 197, 94)" },
        { name: "Transfer", time: transferTime, color: "rgb(59, 130, 246)" },
    ];
    const showViewModes = Boolean(rows?.length);
    const filteredResults = useMemo(() => {
        const normalizedSearch = searchValue.trim().toLowerCase();
        if (!normalizedSearch) {
            return results.map((result, index) => ({ result, index }));
        }

        return results
            .map((result, index) => ({ result, index }))
            .filter(({ result }) =>
                (result.statementQuery || "")
                    .toLowerCase()
                    .includes(normalizedSearch),
            );
    }, [results, searchValue]);

    const getResultButtonClassName = (
        status: QueryResultsFooterProps["results"][number]["status"],
        isActive: boolean,
    ) => {
        if (isActive) {
            return "bg-primary text-primary-foreground hover:bg-primary/90";
        }

        if (status === "failed") {
            return "border-destructive/30 text-destructive hover:bg-destructive/10";
        }

        if (status === "pending" || status === "running") {
            return "border-amber-500/30 text-amber-500 hover:bg-amber-500/10";
        }

        if (status === "success") {
            return "border-emerald-500/30 text-emerald-500 hover:bg-emerald-500/10";
        }

        return "";
    };

    const getStatusBadgeClassName = (
        status: QueryResultsFooterProps["results"][number]["status"],
    ) => {
        if (status === "failed") {
            return "border-destructive/30 bg-destructive/10 text-destructive";
        }

        if (status === "pending" || status === "running") {
            return "border-amber-500/30 bg-amber-500/10 text-amber-500";
        }

        if (status === "success") {
            return "border-emerald-500/30 bg-emerald-500/10 text-emerald-500";
        }

        return "border-border text-muted-foreground";
    };

    const getShortQuery = (query?: string) => {
        if (!query) {
            return "No query";
        }

        const normalized = query.replace(/\s+/g, " ").trim();
        if (normalized.length <= 18) {
            return normalized;
        }

        return `${normalized.slice(0, 15)}...`;
    };

    return (
        <Tabs defaultValue="viewer" className="h-full flex flex-col min-h-0">
            <div className="flex-1 min-h-0">{children}</div>
            <TabsList className="mt-0 h-max px-0">
                <div className="flex w-full p-2 border-t border-border justify-between">
                    <div className="flex gap-1 items-center">
                        <Button variant="thirdly">
                            <TreeColumnsIcon />
                            {rows?.length || 0}{" "}
                            {rows?.length === 1 ? "element" : "elements"}
                        </Button>
                        <TimelineCard
                            title="Query Execution Timeline"
                            items={timelineItems}
                            displayTime={executionTime}
                            disabled={true}
                        />
                        {results.length > 1 && (
                            <div className="flex gap-1 items-center">
                                <Button
                                    variant="outline"
                                    size="iconSmall"
                                    disabled={!canGoPrevious}
                                    onClick={() => {
                                        if (canGoPrevious) {
                                            onSelectResult?.(
                                                selectedResultIndex - 1,
                                            );
                                        }
                                    }}>
                                    {"<"}
                                </Button>
                                <Popover
                                    open={isPopoverOpen}
                                    onOpenChange={(open) => {
                                        setIsPopoverOpen(open);
                                        if (!open) {
                                            setSearchValue("");
                                        }
                                    }}>
                                    <PopoverTrigger asChild>
                                        <Button
                                            variant="outline"
                                            size="small"
                                            className={cn(
                                                "h-8 min-w-18 px-2",
                                                getResultButtonClassName(
                                                    results[selectedResultIndex]
                                                        ?.status || "",
                                                    true,
                                                ),
                                            )}>
                                            {selectedResultIndex + 1} /{" "}
                                            {results.length}
                                        </Button>
                                    </PopoverTrigger>
                                    <PopoverContent
                                        align="start"
                                        className="p-1"
                                        style={{ width: "20rem" }}>
                                        <div className="flex flex-col gap-1">
                                            <Input
                                                value={searchValue}
                                                onChange={(event) => {
                                                    setSearchValue(
                                                        event.target.value,
                                                    );
                                                }}
                                                placeholder="Search query"
                                                className="h-8"
                                            />
                                            <div className="max-h-72 overflow-y-auto">
                                                <div className="flex flex-col gap-1">
                                                    {filteredResults.length ===
                                                    0 ? (
                                                        <div className="px-2 py-3 text-sm text-muted-foreground">
                                                            No executions found
                                                        </div>
                                                    ) : (
                                                        filteredResults.map(
                                                            ({
                                                                result,
                                                                index,
                                                            }) => (
                                                                <button
                                                                    key={`${result.status}-${index}`}
                                                                    type="button"
                                                                    className={cn(
                                                                        "flex w-full items-center justify-between gap-2 rounded-md px-2 py-1.5 text-left text-sm hover:bg-accent hover:text-accent-foreground",
                                                                        index ===
                                                                            selectedResultIndex &&
                                                                            "bg-accent text-accent-foreground",
                                                                    )}
                                                                    onClick={() => {
                                                                        onSelectResult?.(
                                                                            index,
                                                                        );
                                                                        setIsPopoverOpen(
                                                                            false,
                                                                        );
                                                                    }}>
                                                                    <div className="flex min-w-0 items-center gap-2">
                                                                        <span className="w-6 shrink-0 text-xs text-muted-foreground">
                                                                            {index +
                                                                                1}
                                                                        </span>
                                                                        <span className="truncate">
                                                                            {getShortQuery(
                                                                                result.statementQuery,
                                                                            )}
                                                                        </span>
                                                                    </div>
                                                                    <span
                                                                        className={cn(
                                                                            "shrink-0 rounded border px-1.5 py-0.5 text-[10px] uppercase tracking-wide",
                                                                            getStatusBadgeClassName(
                                                                                result.status,
                                                                            ),
                                                                        )}>
                                                                        {result.status ||
                                                                            "idle"}
                                                                    </span>
                                                                </button>
                                                            ),
                                                        )
                                                    )}
                                                </div>
                                            </div>
                                        </div>
                                    </PopoverContent>
                                </Popover>
                                <Button
                                    variant="outline"
                                    size="iconSmall"
                                    disabled={!canGoNext}
                                    onClick={() => {
                                        if (canGoNext) {
                                            onSelectResult?.(
                                                selectedResultIndex + 1,
                                            );
                                        }
                                    }}>
                                    {">"}
                                </Button>
                            </div>
                        )}
                    </div>
                    {showViewModes ? (
                        <div className="flex gap-1">
                            <TabsTrigger value="viewer">Viewer</TabsTrigger>
                            <TabsTrigger className="px-2.5 w-max" value="json">
                                JSON
                            </TabsTrigger>
                        </div>
                    ) : (
                        <div />
                    )}
                    <div className="flex gap-1">
                        <DownloadResultsButton rows={rows} />
                    </div>
                </div>
            </TabsList>
        </Tabs>
    );
};
