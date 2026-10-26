import { VirtualizedTable } from "../../VirtualizedTable";

interface QueryViewerResultsProps {
    results: {
        rows: Record<string, any>[] | undefined;
    }[];
    selectedResultIndex: number;
    colorfulTypes?: boolean;
}

export const QueryViewerResults: React.FC<QueryViewerResultsProps> = ({
    results,
    selectedResultIndex,
    colorfulTypes = true,
}) => {
    return (
        <VirtualizedTable
            data={results[selectedResultIndex].rows || []}
            colorfulTypes={colorfulTypes}
        />
    );
};
