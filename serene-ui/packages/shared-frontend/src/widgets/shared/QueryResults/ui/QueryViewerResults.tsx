import { VirtualizedTable } from "../../VirtualizedTable";

interface QueryViewerResultsProps {
    results: {
        rows: Record<string, any>[] | undefined;
    }[];
    selectedResultIndex: number;
}

export const QueryViewerResults: React.FC<QueryViewerResultsProps> = ({
    results,
    selectedResultIndex,
}) => {
    return <VirtualizedTable data={results[selectedResultIndex].rows || []} />;
};
