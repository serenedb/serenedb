import React from "react";
import {
    ArrowDownIcon,
    CheckIcon,
    cn,
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
    DatabaseIcon,
    Popover,
    PopoverContent,
    PopoverTrigger,
    Skeleton,
} from "@serene-ui/shared-frontend";

interface DatabasesComboboxProps {
    databases: string[];
    currentDatabase: string;
    isLoading: boolean;
    setCurrentDatabase: (database: string) => void;
}

export const DatabasesCombobox: React.FC<DatabasesComboboxProps> = ({
    databases,
    currentDatabase,
    isLoading,
    setCurrentDatabase,
}) => {
    const [open, setOpen] = React.useState(false);

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <button
                    type="button"
                    aria-expanded={open}
                    aria-label="Select database"
                    data-testid="dashboardSelectChartParams-databaseTrigger"
                    className="border-border bg-secondary flex h-8 w-full items-center justify-between rounded-md border px-3 py-2 text-sm">
                    <div className="flex min-w-0 flex-1 items-center gap-2 overflow-hidden">
                        <DatabaseIcon className="shrink-0" />
                        {isLoading && databases.length === 0 ? (
                            <Skeleton className="h-4 max-w-40 flex-1" />
                        ) : (
                            <span className="block flex-1 truncate text-left text-secondary-foreground/70">
                                {currentDatabase || "Select database"}
                            </span>
                        )}
                    </div>
                    <ArrowDownIcon className={cn(open ? "rotate-180" : "")} />
                </button>
            </PopoverTrigger>
            <PopoverContent
                sideOffset={5}
                variant="secondary"
                className="w-full p-0"
                data-testid="dashboardSelectChartParams-databasePopover">
                <Command className="bg-transparent">
                    <CommandInput
                        placeholder="Search databases"
                        className="h-9"
                        data-testid="dashboardSelectChartParams-databaseSearch"
                    />
                    <CommandList>
                        <CommandEmpty>No databases found.</CommandEmpty>
                        <CommandGroup>
                            {databases.map((database) => (
                                <CommandItem
                                    key={database}
                                    value={database}
                                    data-testid={`dashboardSelectChartParams-databaseOption-${database}`}
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
