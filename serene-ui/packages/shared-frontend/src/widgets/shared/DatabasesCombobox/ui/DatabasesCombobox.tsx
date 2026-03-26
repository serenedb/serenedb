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
    const {
        data: connections,
        isFetched: isConnectionsFetched,
        isLoading: isConnectionsLoading,
    } = useGetConnections();

    const isLoading =
        isDatabasesLoading || (isConnectionsLoading && !isConnectionsFetched);

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

    if (variant === "inline") {
        return panel;
    }

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="secondary"
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
            <PopoverContent
                sideOffset={5}
                variant="secondary"
                className="w-full p-0">
                {panel}
            </PopoverContent>
        </Popover>
    );
};
