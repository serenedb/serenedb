import { useConnectionsModal } from "@serene-ui/shared-frontend/features";
import React from "react";
import {
    useConnection,
    useGetConnections,
} from "@serene-ui/shared-frontend/entities";
import {
    Popover,
    PopoverTrigger,
    Button,
    ComboboxPanel,
    PopoverContent,
    ConnectionIcon,
    cn,
    ArrowDownIcon,
    Skeleton,
} from "@serene-ui/shared-frontend/shared";

interface ConnectionsComboboxProps {
    currentConnectionId?: number;
    setCurrentConnectionId?: (id: number) => void;
    variant?: "popover" | "inline";
    autoFocus?: boolean;
    onSelect?: (id: number) => void;
    panelClassName?: string;
    listClassName?: string;
}

const ADD_CONNECTION_OPTION_VALUE = "__add_connection__";

export const ConnectionsCombobox: React.FC<ConnectionsComboboxProps> = ({
    currentConnectionId,
    setCurrentConnectionId,
    variant = "popover",
    autoFocus = false,
    onSelect,
    panelClassName,
    listClassName,
}) => {
    const { setOpen: setModalOpen } = useConnectionsModal();
    const [open, setOpen] = React.useState(false);

    const { currentConnection, setCurrentConnection } = useConnection();
    const isControlled =
        currentConnectionId !== undefined &&
        setCurrentConnectionId !== undefined;

    const selectedConnectionId = isControlled
        ? currentConnectionId
        : currentConnection.connectionId;

    const handleAddConnection = () => {
        setOpen(false);
        setModalOpen(true);
    };

    const { data: connections, isFetched, isLoading } = useGetConnections();

    const options = React.useMemo(() => {
        const connectionOptions =
            connections?.map((connection) => ({
                value: connection.id.toString(),
                label: connection.name,
            })) ?? [];

        return [
            ...connectionOptions,
            {
                value: ADD_CONNECTION_OPTION_VALUE,
                label: "Add connection",
            },
        ];
    }, [connections]);

    const selectedConnectionName = React.useMemo(() => {
        if (selectedConnectionId === undefined || selectedConnectionId === -1) {
            return "";
        }

        return (
            connections?.find(
                (connection) => connection.id === selectedConnectionId,
            )?.name ?? ""
        );
    }, [connections, selectedConnectionId]);

    const handleSelectConnection = React.useCallback(
        (nextConnectionId: number) => {
            if (isControlled) {
                setCurrentConnectionId?.(nextConnectionId);
                onSelect?.(nextConnectionId);
                return;
            }

            setCurrentConnection((prev) => ({
                connectionId: nextConnectionId,
                database:
                    prev.connectionId === nextConnectionId ? prev.database : "",
            }));
            onSelect?.(nextConnectionId);
        },
        [isControlled, onSelect, setCurrentConnection, setCurrentConnectionId],
    );

    const panel = (
        <ComboboxPanel
            items={options}
            selectedValue={
                selectedConnectionId !== undefined &&
                selectedConnectionId !== -1
                    ? selectedConnectionId.toString()
                    : undefined
            }
            placeholder="Search connections"
            emptyMessage="No connections"
            isLoading={isLoading && !isFetched}
            loadingMessage="Loading connections..."
            autoFocus={variant === "popover" ? true : autoFocus}
            className={cn("bg-transparent", panelClassName)}
            listClassName={listClassName}
            onSelect={(value) => {
                if (value === ADD_CONNECTION_OPTION_VALUE) {
                    handleAddConnection();
                    return;
                }

                handleSelectConnection(parseInt(value, 10));

                if (variant === "popover") {
                    setOpen(false);
                }
            }}
        />
    );

    if (variant === "inline") {
        return panel;
    }

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    role="combobox"
                    tabIndex={-1}
                    aria-expanded={open}
                    aria-label="Select connection"
                    size={"small"}
                    className="min-h-8 flex-1 gap-2 justify-between max-w-full overflow-hidden transition-colors duration-150">
                    <div className="flex flex-1 gap-2 items-center min-w-0 overflow-hidden">
                        <ConnectionIcon className="flex-shrink-0" />
                        {isLoading && !isFetched ? (
                            <Skeleton className="flex-1 h-4 max-w-30" />
                        ) : selectedConnectionName ? (
                            <span className="text-left truncate min-w-0 block flex-1">
                                {selectedConnectionName}
                            </span>
                        ) : (
                            <span className="text-xs text-left truncate min-w-0 block flex-1">
                                Select connection
                            </span>
                        )}
                    </div>
                    <ArrowDownIcon
                        className={cn(
                            "flex-shrink-0",
                            open ? "rotate-180" : "",
                        )}
                    />
                </Button>
            </PopoverTrigger>
            <PopoverContent sideOffset={5} className="w-full p-0">
                {panel}
            </PopoverContent>
        </Popover>
    );
};
