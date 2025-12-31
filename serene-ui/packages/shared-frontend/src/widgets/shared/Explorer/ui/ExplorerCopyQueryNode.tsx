import { TreeQueryIcon } from "@serene-ui/shared-frontend/shared";
import { ExplorerNodeButton } from "./ExplorerNodeButton";
import { type ExplorerNodeProps } from "../model";

export const ExplorerCopyQueryNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const { node } = nodeData;

    const handleClick = async () => {
        if (!node.isOpen) {
            node.open();
            node.data.context?.action?.();
        } else {
            node.close();
        }
    };

    return (
        <ExplorerNodeButton
            title={node.data.name}
            onClick={handleClick}
            open={node.isOpen}
            icon={<TreeQueryIcon />}
            showArrow={false}
        />
    );
};
