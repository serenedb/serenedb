import { useConnectionsModal } from "@serene-ui/shared-frontend/features";
import React from "react";
import { useGetConnections } from "@serene-ui/shared-frontend/entities";
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
    ConnectionIcon,
    cn,
    CheckIcon,
    ArrowDownIcon,
    Skeleton,
} from "@serene-ui/shared-frontend/shared";

interface ConnectionsComboboxProps {
    currentConnectionId: number;
    setCurrentConnectionId: (id: number) => void;
}

export const ConnectionsCombobox: React.FC<ConnectionsComboboxProps> = ({
    currentConnectionId,
    setCurrentConnectionId,
}) => {
    const { setOpen: setModalOpen } = useConnectionsModal();
    const [open, setOpen] = React.useState(false);

    const handleAddConnection = () => {
        setModalOpen(true);
    };

    const { data: connections, isFetched, isLoading } = useGetConnections();

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="thirdly"
                    role="combobox"
                    tabIndex={-1}
                    aria-expanded={open}
                    aria-label="Select connection"
                    className="flex-1 gap-2 justify-between max-w-full overflow-hidden">
                    <div className="flex flex-1 gap-2 items-center min-w-0 overflow-hidden">
                        <ConnectionIcon className="flex-shrink-0" />
                        {isLoading && !isFetched ? (
                            <Skeleton className="flex-1 h-4 max-w-30" />
                        ) : currentConnectionId !== -1 ? (
                            <span className="text-left fade-in duration-100 truncate min-w-0 block flex-1">
                                {
                                    connections?.find(
                                        (connection) =>
                                            connection.id ===
                                            currentConnectionId,
                                    )?.name
                                }
                            </span>
                        ) : (
                            <span className="text-left fade-in duration-100 truncate min-w-0 block flex-1">
                                Select host
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
            <PopoverContent
                sideOffset={5}
                variant="secondary"
                className="w-full p-0">
                <Command className="bg-transparent">
                    <CommandInput
                        placeholder="Search connections"
                        className="h-9"
                    />
                    <CommandList>
                        <CommandEmpty>No connections</CommandEmpty>
                        <CommandGroup>
                            {connections?.map((connection) => (
                                <CommandItem
                                    key={connection.id}
                                    value={connection.id.toString()}
                                    onSelect={(currentValue: string) => {
                                        setCurrentConnectionId(
                                            currentValue ===
                                                currentConnectionId?.toString()
                                                ? -1
                                                : parseInt(currentValue),
                                        );
                                        setOpen(false);
                                    }}>
                                    {connection.name}
                                    <CheckIcon
                                        className={cn(
                                            "ml-auto",
                                            currentConnectionId ===
                                                connection.id
                                                ? "opacity-100"
                                                : "opacity-0",
                                        )}
                                    />
                                </CommandItem>
                            ))}
                            <button
                                onClick={handleAddConnection}
                                className="w-full pointer-events-auto">
                                <CommandItem>Add connection</CommandItem>
                            </button>
                        </CommandGroup>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    );
};
