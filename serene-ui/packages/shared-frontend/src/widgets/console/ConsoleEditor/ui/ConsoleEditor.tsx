import { type FC, type SVGProps, useEffect, useState } from "react";
import {
    DockviewReact,
    type DockviewReadyEvent,
    type IDockviewHeaderActionsProps,
    type SerializedDockview,
} from "dockview";
import {
    Button,
    cn,
    MaximizeIcon,
    MinimizeIcon,
    PlusIcon,
} from "@serene-ui/shared-frontend";
import { useDockviewLayoutSync } from "../../../../shared/hooks";
import {
    CONSOLE_EDITOR_PANEL_COMPONENT,
    CONSOLE_RESULTS_PANEL_COMPONENT,
    INITIAL_CONSOLE_EDITOR_PANELS,
    addEditorPanel,
    createEditorPanelParams,
    createPanelId,
    createPanelTitle,
} from "../model";
import { EditorPanel } from "./EditorPanel";
import { ConsoleEditorTopbar } from "./ConsoleEditorTopbar";
import { ResultsPanel } from "./ResultsPanel";

const CONSOLE_EDITOR_LAYOUT_STORAGE_KEY = "console:editor-dock-layout";

const components = {
    [CONSOLE_EDITOR_PANEL_COMPONENT]: EditorPanel,
    [CONSOLE_RESULTS_PANEL_COMPONENT]: ResultsPanel,
};

const HeaderActionButton: FC<{
    title: string;
    onClick: () => void;
    icon: FC<SVGProps<SVGSVGElement>>;
    className?: string;
}> = ({ title, onClick, icon: Icon, className }) => (
    <Button
        size="icon"
        variant="ghost"
        title={title}
        onClick={onClick}
        className={cn(
            "border-r-[0.5px] border-l-[0.5px] rounded-none size-9 text-foreground/50 hover:text-foreground",
            className,
        )}>
        <Icon className="size-3" />
    </Button>
);

const LeftHeaderActions: FC<IDockviewHeaderActionsProps> = (props) => (
    <div className="flex h-full items-center">
        <HeaderActionButton
            title="Add tab"
            onClick={() => {
                props.containerApi.addPanel({
                    id: createPanelId(),
                    component: CONSOLE_EDITOR_PANEL_COMPONENT,
                    title: createPanelTitle(props.containerApi.totalPanels + 1),
                    params: createEditorPanelParams(),
                    position: {
                        referenceGroup: props.group,
                    },
                });
            }}
            icon={PlusIcon}
        />
    </div>
);

const RightHeaderActions: FC<IDockviewHeaderActionsProps> = (props) => {
    const [isMaximized, setIsMaximized] = useState<boolean>(
        props.containerApi.hasMaximizedGroup(),
    );

    useEffect(() => {
        const disposable = props.containerApi.onDidMaximizedGroupChange(() => {
            setIsMaximized(props.containerApi.hasMaximizedGroup());
        });

        return () => disposable.dispose();
    }, [props.containerApi]);

    return (
        <div className="flex h-full items-center">
            <HeaderActionButton
                title={isMaximized ? "Minimize view" : "Maximize view"}
                className="border-r-0"
                onClick={() => {
                    if (props.containerApi.hasMaximizedGroup()) {
                        props.containerApi.exitMaximizedGroup();
                        return;
                    }

                    props.activePanel?.api.maximize();
                }}
                icon={isMaximized ? MinimizeIcon : MaximizeIcon}
            />
        </div>
    );
};

const sanitizeResultEntry = (entry: unknown) => {
    if (!entry || typeof entry !== "object") {
        return null;
    }

    const result = entry as Record<string, unknown>;
    const status = result.status;

    if (status === "pending" || status === "running") {
        return null;
    }

    return result;
};

const sanitizeLayout = (value: unknown): unknown => {
    if (Array.isArray(value)) {
        return value.map((entry) => sanitizeLayout(entry));
    }

    if (!value || typeof value !== "object") {
        return value;
    }

    const record = value as Record<string, unknown>;
    const sanitized: Record<string, unknown> = {};

    const hasResults =
        Object.prototype.hasOwnProperty.call(record, "results") &&
        Array.isArray(record.results);
    let sanitizedResults: unknown[] | null = null;

    Object.entries(record).forEach(([key, entry]) => {
        if (key === "results" && Array.isArray(entry)) {
            sanitizedResults = entry
                .map((result) => sanitizeResultEntry(result))
                .filter((result) => result !== null);
            sanitized[key] = sanitizedResults;
            return;
        }

        if (key === "runOnMountMode") {
            return;
        }

        sanitized[key] = sanitizeLayout(entry);
    });

    if (hasResults) {
        const selectedResultIndex = Number(sanitized.selectedResultIndex);
        const resultsLength =
            (sanitizedResults as unknown[] | null)?.length ?? 0;

        sanitized.selectedResultIndex =
            resultsLength > 0
                ? Number.isFinite(selectedResultIndex)
                    ? Math.min(
                          Math.max(0, selectedResultIndex),
                          resultsLength - 1,
                      )
                    : 0
                : 0;
    }

    return sanitized;
};

export const ConsoleEditor: FC = () => {
    const [api, setApi] = useState<DockviewReadyEvent["api"]>();
    const containerRef = useDockviewLayoutSync<HTMLDivElement>(api);

    const onReady = (event: DockviewReadyEvent) => {
        setApi(event.api);

        let restored = false;
        const rawLayout = localStorage.getItem(
            CONSOLE_EDITOR_LAYOUT_STORAGE_KEY,
        );
        if (rawLayout) {
            try {
                const sanitizedLayout = sanitizeLayout(JSON.parse(rawLayout));
                event.api.fromJSON(sanitizedLayout as SerializedDockview);
                restored = true;
            } catch (error) {
                console.warn("Failed to restore console editor layout:", error);
            }
        }

        if (restored) {
            return;
        }

        Array.from({ length: INITIAL_CONSOLE_EDITOR_PANELS }).forEach(() => {
            addEditorPanel(event.api);
        });
    };

    useEffect(() => {
        if (!api) {
            return;
        }

        const disposable = api.onDidLayoutChange(() => {
            try {
                localStorage.setItem(
                    CONSOLE_EDITOR_LAYOUT_STORAGE_KEY,
                    JSON.stringify(sanitizeLayout(api.toJSON())),
                );
            } catch (error) {
                console.warn("Failed to save console editor layout:", error);
            }
        });

        return () => disposable.dispose();
    }, [api]);

    return (
        <div ref={containerRef} className="relative flex h-dvh w-full flex-col">
            <ConsoleEditorTopbar />
            <div className="flex-1 min-h-0">
                <DockviewReact
                    onReady={onReady}
                    components={components}
                    leftHeaderActionsComponent={LeftHeaderActions}
                    rightHeaderActionsComponent={RightHeaderActions}
                />
            </div>
        </div>
    );
};
