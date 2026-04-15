import React, { useEffect } from "react";
import {
    useConnection,
    useDatabases,
    useGetConnections,
} from "@serene-ui/shared-frontend/entities";
import {
    Popover,
    PopoverTrigger,
    Button,
    ComboboxPanel,
    PopoverContent,
    DatabaseIcon,
    ArrowDownIcon,
    Skeleton,
    LoaderIcon,
} from "@serene-ui/shared-frontend/shared";

interface DatabasesComboboxProps {
    variant?: "popover" | "inline";
    autoFocus?: boolean;
    onSelect?: (database: string) => void;
    panelClassName?: string;
    listClassName?: string;
}

export const DatabasesCombobox: React.FC<DatabasesComboboxProps> = ({
    variant = "popover",
    autoFocus = false,
    onSelect,
    panelClassName,
    listClassName,
}) => {
    const { currentConnection, setCurrentConnection } = useConnection();
    const { databases, isLoading: isDatabasesLoading } = useDatabases();
    const { isFetched: isConnectionsFetched, isLoading: isConnectionsLoading } =
        useGetConnections();

    const isLoading =
        isDatabasesLoading || (isConnectionsLoading && !isConnectionsFetched);
    const [isLoadingAfterConnectionSwitch, setIsLoadingAfterConnectionSwitch] =
        React.useState(false);
    const previousConnectionIdRef = React.useRef<number | undefined>(undefined);

    const [open, setOpen] = React.useState(false);

    const label = currentConnection.database || "Select database";

    useEffect(() => {
        if (
            currentConnection.database &&
            databases.length > 0 &&
            !databases.includes(currentConnection.database)
        ) {
            setCurrentConnection((prev) => ({
                ...prev,
                database: "",
            }));
        }
    }, [databases, currentConnection.database, setCurrentConnection]);

    const options = React.useMemo(() => {
        return databases.map((database) => ({
            value: database,
            label: database,
        }));
    }, [databases]);

    useEffect(() => {
        const previousConnectionId = previousConnectionIdRef.current;
        const nextConnectionId = currentConnection.connectionId;

        if (
            previousConnectionId !== undefined &&
            previousConnectionId !== nextConnectionId
        ) {
            setIsLoadingAfterConnectionSwitch(true);
        }

        previousConnectionIdRef.current = nextConnectionId;
    }, [currentConnection.connectionId]);

    useEffect(() => {
        if (!isDatabasesLoading) {
            setIsLoadingAfterConnectionSwitch(false);
        }
    }, [isDatabasesLoading]);

    const panel = (
        <ComboboxPanel
            items={options}
            selectedValue={currentConnection.database || undefined}
            placeholder="Search databases"
            emptyMessage="No databases found."
            isLoading={isLoading && databases.length === 0}
            loadingMessage="Loading databases..."
            autoFocus={variant === "popover" ? true : autoFocus}
            className={panelClassName}
            listClassName={listClassName}
            onSelect={(value) => {
                setCurrentConnection((prev) => ({
                    ...prev,
                    database: value,
                }));
                onSelect?.(value);

                if (variant === "popover") {
                    setOpen(false);
                }
            }}
        />
    );

    const panelWithLoader = (
        <div className="relative">
            {panel}
            {isLoadingAfterConnectionSwitch && isDatabasesLoading ? (
                <div className="bg-background/70 absolute inset-0 z-10 flex items-center justify-center">
                    <LoaderIcon className="size-5 animate-spin" />
                </div>
            ) : null}
        </div>
    );

    if (variant === "inline") {
        return panelWithLoader;
    }

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="outline"
                    role="combobox"
                    size={"small"}
                    aria-expanded={open}
                    aria-label="Select database"
                    className="flex-1 justify-between max-w-full overflow-hidden transition-colors duration-150">
                    <div className="flex flex-1 gap-2 items-center min-w-0 overflow-hidden">
                        <DatabaseIcon className="flex-shrink-0" />
                        {isLoading && databases.length === 0 ? (
                            <Skeleton className="flex-1 h-4 max-w-40" />
                        ) : (
                            <span className="text-xs text-left truncate min-w-0 block flex-1">
                                {label}
                            </span>
                        )}
                    </div>
                    <ArrowDownIcon className={open ? "rotate-180" : ""} />
                </Button>
            </PopoverTrigger>
            <PopoverContent sideOffset={5} className="w-full p-0">
                {panelWithLoader}
            </PopoverContent>
        </Popover>
    );
};
