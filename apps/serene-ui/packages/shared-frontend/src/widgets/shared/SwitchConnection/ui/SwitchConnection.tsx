import React, { useEffect, useMemo, useRef, useState } from "react";
import {
    cn,
    ConnectionIcon,
    DEFAULT_HOTKEYS,
    Kbd,
    KbdGroup,
    Popover,
    PopoverContent,
    PopoverTrigger,
    useAppHotkey,
    useConnection,
    useConnectionsModal,
    useGetConnections,
    useTestConnection,
} from "@serene-ui/shared-frontend";
import { SwitchConnectionModal } from "./SwitchConnectionModal";

interface SwitchConnectionProps {}

export const SwitchConnection: React.FC<SwitchConnectionProps> = () => {
    const [open, setOpen] = useState(false);
    const previouslyFocusedElementRef = useRef<HTMLElement | null>(null);
    const [connectionState, setConnectionState] = useState<
        "pending" | "success" | "failed"
    >("pending");
    const { currentConnection } = useConnection();
    const { setOpen: setConnectionsModalOpen } = useConnectionsModal();
    const { data: connections, isFetched: isConnectionsFetched } =
        useGetConnections();
    const hasConnections = (connections?.length ?? 0) > 0;
    const shouldOpenConnectionsModal =
        isConnectionsFetched && !hasConnections;

    const handleOpenChange = (nextOpen: boolean) => {
        if (nextOpen && shouldOpenConnectionsModal) {
            setConnectionsModalOpen(true);
            return;
        }

        if (nextOpen) {
            const activeElement = document.activeElement;
            previouslyFocusedElementRef.current =
                activeElement instanceof HTMLElement ? activeElement : null;
        }
        setOpen(nextOpen);
    };

    const restorePreviousFocus = () => {
        previouslyFocusedElementRef.current?.focus();
    };

    useAppHotkey(DEFAULT_HOTKEYS.SWITCH_CONNECTION_TOGGLE, () => {
        handleOpenChange(true);
    });

    const connectionName = useMemo(() => {
        return (
            connections?.find(
                (connection) =>
                    connection.id === currentConnection.connectionId,
            )?.name || undefined
        );
    }, [currentConnection, connections]);

    const switchConnectionLabel = useMemo(() => {
        if (shouldOpenConnectionsModal) {
            return "Create connection";
        }

        return `${connectionName || "Connection name"} / ${
            currentConnection.database || "Select database"
        }`;
    }, [
        connectionName,
        currentConnection.database,
        shouldOpenConnectionsModal,
    ]);

    const { handleTestConnection } = useTestConnection();

    const testConnection = async () => {
        let result: "pending" | "success" | "failed" = "pending";
        try {
            await handleTestConnection({
                connectionId: currentConnection.connectionId,
                database: currentConnection.database,
            });
            result = "success";
        } catch {
            result = "failed";
        }
        setConnectionState(result);
    };
    useEffect(() => {
        if (currentConnection.connectionId !== -1 && currentConnection.database)
            testConnection();
    }, [currentConnection]);

    return (
        <Popover open={open} onOpenChange={handleOpenChange}>
            <PopoverTrigger asChild>
                <button
                    type="button"
                    aria-label="Switch connection"
                    className="outline-none focus:outline-none focus-visible:outline-none focus-visible:ring-0 focus-visible:ring-offset-0">
                    <div className="border-[0.5px] bg-muted rounded-md flex p-1.5 pl-2.5 items-center min-w-80">
                        <div className="relative">
                            <ConnectionIcon className="size-4 text-muted-foreground/50" />
                            <div
                                className={cn(
                                    "absolute rounded-full w-1.5 h-1.5 right-0 top-[-1px] bg-orange-700",
                                    {
                                        "bg-red-700":
                                            connectionState === "failed",
                                        "bg-orange-700":
                                            connectionState === "pending",
                                        "bg-green-700":
                                            connectionState === "success",
                                    },
                                )}
                            />
                        </div>
                        <p className="text-xs text-muted-foreground/50 ml-2">
                            {switchConnectionLabel}
                        </p>

                        <KbdGroup className="ml-auto pl-3">
                            <Kbd>Cmd</Kbd>
                            <Kbd>K</Kbd>
                        </KbdGroup>
                    </div>
                </button>
            </PopoverTrigger>
            <PopoverContent
                className="min-w-100 p-1"
                onOpenAutoFocus={(event) => {
                    event.preventDefault();
                }}
                onCloseAutoFocus={(event) => {
                    event.preventDefault();
                    restorePreviousFocus();
                }}>
                <SwitchConnectionModal
                    open={open}
                    onComplete={() => {
                        setOpen(false);
                    }}
                />
            </PopoverContent>
        </Popover>
    );
};
