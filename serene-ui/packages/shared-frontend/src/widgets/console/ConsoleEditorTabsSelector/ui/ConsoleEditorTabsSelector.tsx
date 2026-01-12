import { ChangeConsoleLayoutIcon } from "@serene-ui/shared-frontend/features";
import {
    DEFAULT_HOTKEYS,
    useAppHotkey,
} from "@serene-ui/shared-frontend/shared";
import React, { useEffect } from "react";
import { type ConsoleTab } from "../model/types";
import { ConsoleEditorAddTabButton } from "./ConsoleEditorAddTabButton";
import { ConsoleEditorMaximizeButton } from "./ConsoleEditorMaximizeButton";
import { ConsoleEditorSettingsButton } from "./ConsoleEditorSettingsButton";
import { ConsoleEditorTabButton } from "./ConsoleEditorTabButton";

interface ConsoleEditorTabsSelectorProps {
    tabs: ConsoleTab[];
    selectTab: (tabId: number) => void;
    removeTab: (tabId: number) => void;
    selectedTabId: number;
    addTab: (tabType: ConsoleTab["type"]) => void;
    limit: number;
    setLimit: (limit: number) => void;
}

export const ConsoleEditorTabsSelector: React.FC<
    ConsoleEditorTabsSelectorProps
> = ({
    tabs,
    selectTab,
    removeTab,
    selectedTabId,
    addTab,
    limit,
    setLimit,
}) => {
    const scrollRef = React.useRef<HTMLDivElement | null>(null);

    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_NEXT_TAB, () => {
        if (selectedTabId + 1 < tabs.length) {
            selectTab(selectedTabId + 1);
        }
    });

    useAppHotkey(DEFAULT_HOTKEYS.CONSOLE_PREVIOUS_TAB, () => {
        if (selectedTabId - 1 >= 0) {
            selectTab(selectedTabId - 1);
        }
    });

    useEffect(() => {
        if (!scrollRef.current) return;
        scrollRef.current.scrollLeft = scrollRef.current.scrollWidth;
    }, [tabs.length]);

    return (
        <div className="flex items-center gap-1 pr-2">
            <div className="flex flex-1 max-w-[calc(100%-108px)] gap-1">
                <div
                    ref={scrollRef}
                    className="flex min-w-0 overflow-x-auto rounded-r-md pl-2 scrollbar pb-1.5 mb-auto">
                    <div className="flex gap-1 w-max">
                        {tabs.map((tab) => (
                            <ConsoleEditorTabButton
                                key={tab.id}
                                tab={tab}
                                selectTab={selectTab}
                                removeTab={removeTab}
                                selectedTabId={selectedTabId}
                            />
                        ))}
                    </div>
                </div>
                <ConsoleEditorAddTabButton addTab={addTab} />
            </div>
            <div className="flex gap-1 pb-1.5 mb-auto">
                <ChangeConsoleLayoutIcon />
                <ConsoleEditorMaximizeButton />
                <ConsoleEditorSettingsButton
                    limit={limit}
                    setLimit={setLimit}
                />
            </div>
        </div>
    );
};
