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
        status: "success" | "failed" | "pending" | "running" | "";
        statementIndex?: number;
        statementQuery?: string;
        statementRange?: {
            startOffset: number;
            endOffset: number;
        };
        error?: string;
        message?: string;
        created_at?: string;
        execution_started_at?: string;
        execution_finished_at?: string;
        received_at?: string;
    }[];
    selectedResultIndex: number;
    onSelectResult?: (index: number) => void;
}

export const QueryResults: React.FC<QueryResultsProps> = ({
    results,
    selectedResultIndex,
    onSelectResult,
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

    const created_at = selectedResult.created_at;
    const started_at = selectedResult.execution_started_at;
    const finished_at = selectedResult.execution_finished_at;
    const received_at = selectedResult.received_at;
    const hasRows = selectedResult.status === "success" && Boolean(rows?.length);

    let content: React.ReactNode;

    if (
        selectedResult.status === "pending" ||
        selectedResult.status === "running"
    ) {
        content = <QueryPending />;
    } else if (selectedResult.status === "failed") {
        content = <QueryFailed error={selectedResult.error || ""} />;
    } else if (selectedResult.status === "success" && !rows?.length) {
        content = <QuerySucceeded message={selectedResult.message} />;
    } else {
        content = (
            <>
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
            </>
        );
    }

    return (
        <div className="flex flex-col flex-1 min-h-0">
            <QueryResultsFooter
                results={results}
                selectedResultIndex={selectedResultIndex}
                onSelectResult={onSelectResult}
                rows={rows}
                created_at={created_at}
                execution_started_at={started_at}
                execution_finished_at={finished_at}
                received_at={received_at}>
                {hasRows ? content : <div className="flex flex-1">{content}</div>}
            </QueryResultsFooter>
        </div>
    );
};
