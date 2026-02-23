import { NoQueryResults } from "./NoQueryResults";
import { QueryJSONResults } from "./QueryJSONResults";
import { QueryResultsFooter } from "./QueryResultsFooter";
import { TabsContent } from "@serene-ui/shared-frontend/shared";
import { QueryViewerResults } from "./QueryViewerResults";
import { QueryPending } from "./QueryPending";
import { QueryFailed } from "./QueryFailed";
import { QuerySucceeded } from "./QuerySucceeded";

interface QueryResultsProps {
    results: {
        rows: Record<string, any>[] | undefined;
        status: "success" | "failed" | "pending" | "running";
        error?: string;
        message?: string;
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
    if (
        selectedResultIndex < 0 ||
        !results?.length ||
        !results[selectedResultIndex]
    ) {
        return <NoQueryResults />;
    }

    const selectedResult = results[selectedResultIndex];
    const rows = (selectedResult.rows || undefined) as
        | Record<string, unknown>[]
        | undefined;

    if (
        selectedResult.status === "pending" ||
        selectedResult.status === "running"
    ) {
        return <QueryPending />;
    }

    if (selectedResult.status === "failed") {
        return <QueryFailed error={selectedResult.error || ""} />;
    }

    if (selectedResult.status === "success" && !rows?.length) {
        return <QuerySucceeded message={selectedResult.message} />;
    }

    const created_at = selectedResult.created_at;
    const started_at = selectedResult.execution_started_at;
    const finished_at = selectedResult.execution_finished_at;
    const received_at = selectedResult.received_at;

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
