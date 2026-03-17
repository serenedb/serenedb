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
    PopoverContent,
    Command,
    CommandInput,
    CommandList,
    CommandEmpty,
    CommandGroup,
    CommandItem,
    CheckIcon,
    cn,
    DatabaseIcon,
    ArrowDownIcon,
    Skeleton,
} from "@serene-ui/shared-frontend/shared";

interface DatabasesComboboxProps {
    currentDatabase: string;
    setCurrentDatabase: (database: string) => void;
}

export const DatabasesCombobox: React.FC<DatabasesComboboxProps> = ({
    currentDatabase,
    setCurrentDatabase,
}) => {
    const { databases, isLoading: isDatabasesLoading } = useDatabases();
    const {
        data: connections,
        isFetched: isConnectionsFetched,
        isLoading: isConnectionsLoading,
    } = useGetConnections();

    const isLoading =
        isDatabasesLoading || (isConnectionsLoading && !isConnectionsFetched);

    const [open, setOpen] = React.useState(false);

    const label = currentDatabase || "Select database";

    useEffect(() => {
        if (
            currentDatabase &&
            databases.length > 0 &&
            !databases.includes(currentDatabase)
        ) {
            setCurrentDatabase("");
        }
    }, [databases, currentDatabase]);

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="secondary"
                    role="combobox"
                    aria-expanded={open}
                    aria-label="Select database"
                    className="flex-1 justify-between max-w-full overflow-hidden transition-colors duration-150">
                    <div className="flex flex-1 gap-2 items-center min-w-0 overflow-hidden">
                        <DatabaseIcon className="flex-shrink-0" />
                        {isLoading && databases.length === 0 ? (
                            <Skeleton className="flex-1 h-4 max-w-40" />
                        ) : (
                            <span className="text-left truncate min-w-0 block flex-1">
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
                <Command className="bg-transparent">
                    <CommandInput
                        placeholder="Search databases"
                        className="h-9"
                    />
                    <CommandList>
                        <CommandEmpty>No databases found.</CommandEmpty>
                        <CommandGroup>
                            {databases?.map((database) => (
                                <CommandItem
                                    key={database}
                                    value={database}
                                    onSelect={(currentValue: string) => {
                                        setCurrentDatabase(
                                            currentValue === currentDatabase
                                                ? ""
                                                : currentValue,
                                        );
                                        setOpen(false);
                                    }}>
                                    {database}
                                    <CheckIcon
                                        className={cn(
                                            "ml-auto",
                                            currentDatabase === database
                                                ? "opacity-100"
                                                : "opacity-0",
                                        )}
                                    />
                                </CommandItem>
                            ))}
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    );
};
