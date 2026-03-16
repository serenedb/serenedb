import { DeleteDashboardIconButton } from "../../../../features";
import type { ExplorerNodeProps } from "../model";
import { nodeTemplates } from "../model/const";
import { ExplorerNodeButton } from "./ExplorerNodeButton";

export const ExplorerDashboardNode = ({
    nodeData,
}: {
    nodeData: ExplorerNodeProps;
}) => {
    const { node, style } = nodeData;
    const dashboardId = node.data.context?.dashboardId;

    return (
        <div style={style} className="group relative">
            <ExplorerNodeButton
                className="pl-6.5 pr-7"
                title={node.data.name}
                onClick={node.data.context?.action}
                open={false}
                icon={nodeTemplates.dashboard.icon}
                showArrow={false}
            />
            {dashboardId ? (
                <div className="absolute top-1/2 right-1 z-10 -translate-y-1/2 opacity-0 transition-opacity group-hover:opacity-100 group-focus-within:opacity-100">
                    <DeleteDashboardIconButton
                        dashboardId={dashboardId}
                        dashboardName={node.data.name}
                    />
                </div>
            ) : null}
        </div>
    );
};
