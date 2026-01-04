import { useExplorer, type ExplorerNodeProps } from "../model";
import { nodeTemplates } from "../model/const";
import { ExplorerNodeButton } from "./ExplorerNodeButton";
import {
    ContextMenu,
    ContextMenuContent,
    ContextMenuItem,
    ContextMenuTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useState } from "react";

export const ExplorerStaticNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const [isLoading, setIsLoading] = useState(false);
    const { node, style } = nodeData;
    const { addNodes, refreshNode } = useExplorer();

    const nodeType = node.data.type as keyof typeof nodeTemplates;

    const handleClick = async () => {
        if (!node.isOpen) {
            node.open();
            addNodes(node.data.id, nodeTemplates[nodeType].getNodes(node.data));
        } else {
            node.close();
        }
    };

    const handleRefresh = async () => {
        if (node.isOpen) {
            node.close();
        }

        refreshNode(node.data.id);

        setIsLoading(true);
        await new Promise((resolve) => setTimeout(resolve, 500));

        node.open();
        addNodes(
            node.data.id,
            nodeTemplates[nodeType].getNodes(node.data),
            true,
        );
        setIsLoading(false);
    };

    return (
        <ContextMenu>
            <ContextMenuTrigger>
                <ExplorerNodeButton
                    title={node.data.name}
                    onClick={handleClick}
                    open={node.isOpen}
                    icon={nodeTemplates[nodeType]?.icon}
                    style={style}
                    showArrow={nodeType !== "column" && nodeType !== "index"}
                    isLoading={isLoading}
                    rightText={
                        nodeType === "column"
                            ? node.data.context?.column_data_type
                            : ""
                    }
                />
            </ContextMenuTrigger>
            <ContextMenuContent className="w-52">
                <ContextMenuItem onClick={handleRefresh}>
                    Refresh
                </ContextMenuItem>
            </ContextMenuContent>
        </ContextMenu>
    );
};
