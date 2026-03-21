import { Explorer, ExplorerNodeData } from "@serene-ui/shared-frontend/widgets";
import { useEffect, useRef, useState } from "react";

export interface DashboardExplorerProps {
    initialData: ExplorerNodeData[];
    isDataFetched?: boolean;
    emptyState?: React.ReactNode;
}

export const DashboardExplorer: React.FC<DashboardExplorerProps> = ({
    initialData,
    isDataFetched = true,
    emptyState,
}) => {
    const [searchTerm, setSearchTerm] = useState<string>();
    const hasData = initialData.length > 0;

    const editorRef = useRef<HTMLElement | null>(null);

    return (
        <div className="flex h-full min-h-0 flex-col">
            {hasData || !isDataFetched ? (
                <div className="flex min-h-0 flex-1 pt-1">
                    <Explorer
                        searchTerm={searchTerm}
                        initialData={initialData}
                        isDataFetched={isDataFetched}
                    />
                </div>
            ) : (
                <div className="flex flex-1 p-2 pt-1">
                    {typeof emptyState === "string" ? (
                        <p className="text-center text-xs text-foreground/70">
                            {emptyState}
                        </p>
                    ) : (
                        emptyState
                    )}
                </div>
            )}
        </div>
    );
};
