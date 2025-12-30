import { JSONEditor } from "../../JSONEditor";

interface QueryJSONResultsProps {
    results: {
        rows: Record<string, any>[] | undefined;
    }[];
    selectedResultIndex: number;
}

export const QueryJSONResults: React.FC<QueryJSONResultsProps> = ({
    results,
    selectedResultIndex,
}) => {
    return (
        <JSONEditor
            readOnly
            value={(() => {
                const anyRows: any = results?.[selectedResultIndex]?.rows;
                if (anyRows === undefined || anyRows === null) return "";
                if (typeof anyRows === "string") {
                    try {
                        const parsed = JSON.parse(anyRows);
                        return JSON.stringify(parsed, null, 2);
                    } catch {
                        return anyRows;
                    }
                }
                return JSON.stringify(anyRows, null, 2);
            })()}
            onChange={() => {}}
        />
    );
};
