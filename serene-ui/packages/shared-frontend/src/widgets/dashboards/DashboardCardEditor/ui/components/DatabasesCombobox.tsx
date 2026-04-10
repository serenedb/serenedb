import React from "react";
import {
    ArrowDownIcon,
    cn,
    ComboboxPanel,
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

    const options = React.useMemo(() => {
        return databases.map((database) => ({
            value: database,
            label: database,
        }));
    }, [databases]);

    const panel = (
        <ComboboxPanel
            items={options}
            selectedValue={currentDatabase || undefined}
            placeholder="Search databases"
            emptyMessage="No databases found."
            isLoading={isLoading && databases.length === 0}
            loadingMessage="Loading databases..."
            autoFocus
            className="bg-transparent"
            onSelect={(value) => {
                setCurrentDatabase(value);
                setOpen(false);
            }}
            inputProps={{
                className: "h-9",
                "data-testid": "dashboardSelectChartParams-databaseSearch",
            }}
            getItemProps={(item) => ({
                "data-testid": `dashboardSelectChartParams-databaseOption-${item.value}`,
            })}
        />
    );

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <button
                    type="button"
                    aria-expanded={open}
                    aria-label="Select database"
                    data-testid="dashboardSelectChartParams-databaseTrigger"
                    className="border-border flex h-8 w-full items-center justify-between rounded-md border px-3 py-2 text-sm">
                    <div className="flex min-w-0 flex-1 items-center gap-2 overflow-hidden">
                        <DatabaseIcon className="shrink-0" />
                        {isLoading && databases.length === 0 ? (
                            <Skeleton className="h-4 max-w-40 flex-1" />
                        ) : (
                            <span className="block flex-1 truncate text-left dark:text-secondary-foreground/70 text-xs">
                                {currentDatabase || "Select database"}
                            </span>
                        )}
                    </div>
                    <ArrowDownIcon className={cn(open ? "rotate-180" : "")} />
                </button>
            </PopoverTrigger>
            <PopoverContent
                sideOffset={5}
                className="w-full p-0"
                data-testid="dashboardSelectChartParams-databasePopover">
                {panel}
            </PopoverContent>
        </Popover>
    );
};
