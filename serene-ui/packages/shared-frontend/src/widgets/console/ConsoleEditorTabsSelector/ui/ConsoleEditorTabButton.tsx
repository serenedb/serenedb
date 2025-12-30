import { useState, useEffect, useRef } from "react";
import {
    ButtonCard,
    ButtonCardButtonContent,
    ButtonCardContent,
    cn,
    CrossIcon,
    GhostTermIcon,
} from "@serene-ui/shared-frontend/shared";
import { type ConsoleTab } from "../model/types";
import { PGSQLEditor } from "@serene-ui/shared-frontend/widgets";

interface ConsoleEditorTabButtonProps {
    tab: ConsoleTab;
    selectTab: (tabId: number) => void;
    removeTab: (tabId: number) => void;
    selectedTabId: number;
}

export const ConsoleEditorTabButton: React.FC<ConsoleEditorTabButtonProps> = ({
    tab,
    selectTab,
    removeTab,
    selectedTabId,
}) => {
    const latestResult = tab.results[tab.results.length - 1];
    const isRunningOrPending =
        latestResult?.status === "running" ||
        latestResult?.status === "pending";

    const [showFailedIndicator, setShowFailedIndicator] = useState(false);
    const [showDoneIndicator, setShowDoneIndicator] = useState(false);

    const wasSelectedOnLastResult = useRef(tab.id === selectedTabId);

    const isSelected = tab.id === selectedTabId;

    useEffect(() => {
        if (!isSelected && !wasSelectedOnLastResult.current) {
            if (latestResult?.status === "failed") {
                setShowFailedIndicator(true);
            } else if (latestResult?.status === "success") {
                setShowDoneIndicator(true);
            }
        }

        wasSelectedOnLastResult.current = isSelected;
    }, [latestResult?.status, isSelected]);

    useEffect(() => {
        if (isSelected) {
            setShowFailedIndicator(false);
            setShowDoneIndicator(false);
        }
    }, [isSelected]);

    return (
        <div className="relative overflow-hidden rounded-md">
            {isRunningOrPending && (
                <div className="pointer-events-none absolute top-0 -left-full w-12 h-8 bg-yellow-400 blur-2xl animate-[slide-right_2s_ease-in-out_infinite]" />
            )}
            {showFailedIndicator && (
                <div className="pointer-events-none absolute top-0 left-0 w-full h-full bg-red-400 blur-2xl " />
            )}
            {showDoneIndicator && (
                <div className="pointer-events-none absolute top-0 left-0 w-full h-full bg-green-500 blur-2xl opacity-50" />
            )}
            <style>{`
                @keyframes slide-right {
                    0% {
                        left: -100%;
                    }
                    100% {
                        left: 200%;
                    }
                }
            `}</style>

            <ButtonCard>
                <ButtonCardButtonContent
                    onClick={() => selectTab(tab.id)}
                    className={cn(
                        "pl-3 !pr-0",
                        tab.id === selectedTabId
                            ? "hover:bg-primary/30 cursor-default"
                            : "",
                    )}
                    variant={
                        tab.id === selectedTabId ? "default" : "secondary"
                    }>
                    <GhostTermIcon className="w-8 h-8 bg-transparent cursor-pointer" />
                    {tab.type === "query" ? "Query" : ""} {tab.id + 1}
                    <div
                        onClick={(e) => {
                            removeTab(tab.id);
                            e.stopPropagation();
                        }}
                        className="ml-2 w-8 h-8 flex items-center justify-center bg-white/3 cursor-pointer hover:bg-white/7 duration-300 rounded-r-md">
                        <CrossIcon className="size-2.5" />
                    </div>
                </ButtonCardButtonContent>
                {tab.id !== selectedTabId && (
                    <ButtonCardContent className="w-80 h-40 p-0 pt-2">
                        <PGSQLEditor
                            value={tab.value}
                            onChange={() => {}}
                            readOnly
                        />
                    </ButtonCardContent>
                )}
            </ButtonCard>
        </div>
    );
};
