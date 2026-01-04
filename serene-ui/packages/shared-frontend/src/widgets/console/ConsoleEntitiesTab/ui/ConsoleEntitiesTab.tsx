import React from "react";
import { ConsoleExplorer } from "./ConsoleExplorer";

interface ConsoleEntitiesTabProps {
    explorerRef?: React.RefObject<HTMLDivElement | null>;
}

export const ConsoleEntitiesTab: React.FC<ConsoleEntitiesTabProps> = ({
    explorerRef,
}) => {
    return (
        <div className="flex-1 flex flex-col">
            <ConsoleExplorer explorerRef={explorerRef} />
        </div>
    );
};
