import { type ExplorerNodeProps } from "../model";
import { ExplorerQueryNode } from "./ExplorerQueryNode";
import { ExplorerCopyQueryNode } from "./ExplorerCopyQueryNode";
import { ExplorerDashboardNode } from "./ExplorerDashboardNode";
import { ExplorerStaticNode } from "./ExplorerStaticNode";

export const ExplorerNode = ({ nodeData }: { nodeData: ExplorerNodeProps }) => {
    if (
        nodeData.node.data.type === "connection" ||
        nodeData.node.data.type === "schemas" ||
        nodeData.node.data.type === "catalogs" ||
        nodeData.node.data.type === "tables" ||
        nodeData.node.data.type === "columns" ||
        nodeData.node.data.type === "views" ||
        nodeData.node.data.type === "indexes"
    ) {
        return <ExplorerQueryNode nodeData={nodeData} />;
    }
    if (
        nodeData.node.data.type === "saved-query" ||
        nodeData.node.data.type === "query-history"
    ) {
        return <ExplorerCopyQueryNode nodeData={nodeData} />;
    }
    if (nodeData.node.data.type === "dashboard") {
        return <ExplorerDashboardNode nodeData={nodeData} />;
    }

    return <ExplorerStaticNode nodeData={nodeData} />;
};
