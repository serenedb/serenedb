import { NoQueryResults } from "./NoQueryResults";
import { QueryJSONResults } from "./QueryJSONResults";
import { QueryResultsFooter } from "./QueryResultsFooter";
import { TabsContent } from "@serene-ui/shared-frontend/shared";
import { QueryViewerResults } from "./QueryViewerResults";
import { QueryPending } from "./QueryPending";
import { QueryFailed } from "./QueryFailed";

interface QueryResultsProps {
    results: {
        rows: Record<string, any>[] | undefined;
        status: "success" | "failed" | "pending" | "running";
        error?: string;
        created_at?: string;
        execution_started_at?: string;
        execution_finished_at?: string;
        received_at?: string;
    }[];
    selectedResultIndex: number;
}

export const QueryResults: React.FC<QueryResultsProps> = ({
    results,
    selectedResultIndex,
}) => {
    const rows = (results?.[selectedResultIndex]?.rows || undefined) as
        | Record<string, unknown>[]
        | undefined;

    if (
        results?.[selectedResultIndex]?.status === "pending" ||
        results?.[selectedResultIndex]?.status === "running"
    ) {
        return <QueryPending />;
    }

    if (results?.[selectedResultIndex]?.status === "failed") {
        return (
            <QueryFailed error={results?.[selectedResultIndex]?.error || ""} />
        );
    }

    if (!rows?.length) {
        return <NoQueryResults />;
    }

    const created_at = results?.[selectedResultIndex]?.created_at;
    const started_at = results?.[selectedResultIndex]?.execution_started_at;
    const finished_at = results?.[selectedResultIndex]?.execution_finished_at;
    const received_at = results?.[selectedResultIndex]?.received_at;

    return (
        <div className="flex flex-col flex-1 min-h-0">
            <QueryResultsFooter
                rows={rows}
                created_at={created_at}
                execution_started_at={started_at}
                execution_finished_at={finished_at}
                received_at={received_at}>
                <TabsContent
                    className="flex-1 w-full h-full pt-2.5 min-h-0 overflow-auto"
                    value="json">
                    <QueryJSONResults
                        results={results}
                        selectedResultIndex={selectedResultIndex}
                    />
                </TabsContent>
                <TabsContent
                    className="flex flex-1 w-full h-full min-h-0 overflow-auto"
                    value="viewer">
                    <QueryViewerResults
                        results={results}
                        selectedResultIndex={selectedResultIndex}
                    />
                </TabsContent>
            </QueryResultsFooter>
        </div>
    );
};
