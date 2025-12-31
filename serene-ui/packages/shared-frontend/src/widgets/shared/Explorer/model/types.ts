import type { NodeRendererProps } from "react-arborist";

export type ExplorerNodeContext = {
    connectionId?: number;
    database?: string;
    schemaId?: number;
    catalogId?: number;
    tableId?: number;
    query?: string;
    action?: () => void;
    viewId?: number;
    column_data_type?: string;
    column_is_array?: string;
};

export type ExplorerNodeData = {
    id: string;
    name: string;
    type: string;
    children?: ExplorerNodeData[];
    parentId: string | null;
    context?: ExplorerNodeContext;
    isError?: boolean;
};

export type ExplorerNodeProps = NodeRendererProps<ExplorerNodeData>;
