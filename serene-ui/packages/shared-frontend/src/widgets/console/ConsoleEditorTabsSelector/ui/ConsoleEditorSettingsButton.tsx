import {
    Button,
    Popover,
    PopoverContent,
    PopoverTrigger,
    Command,
    CommandInput,
    CommandList,
    CommandGroup,
    CommandItem,
    SettingsIcon,
} from "@serene-ui/shared-frontend/shared";
import { useEffect, useState } from "react";

interface ConsoleEditorSettingsButtonProps {
    limit: number;
    setLimit: (limit: number) => void;
}

export const ConsoleEditorSettingsButton: React.FC<
    ConsoleEditorSettingsButtonProps
> = ({ limit, setLimit }) => {
    const [open, setOpen] = useState(false);
    const [inputValue, setInputValue] = useState(
        limit === -1 ? "Unlimited" : limit.toString(),
    );

    useEffect(() => {
        if (!open) {
            setInputValue(limit === -1 ? "Unlimited" : limit.toString());
        }
    }, [open]);

    useEffect(() => {
        setInputValue(limit === -1 ? "Unlimited" : limit.toString());
    }, [limit]);

    const isUnlimited = inputValue.trim().toLowerCase() === "unlimited";
    const isNumeric = /^[0-9]+$/.test(inputValue.trim());
    const canSave = isUnlimited || isNumeric;

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    size={"icon"}
                    variant={"secondary"}
                    title="Result limit settings">
                    <SettingsIcon className="size-4.5" />
                </Button>
            </PopoverTrigger>
            <PopoverContent
                className="min-w-[260px] p-0"
                variant={"secondary"}
                align="end">
                <Command className="bg-transparent">
                    <CommandInput
                        placeholder="Enter limit"
                        value={inputValue}
                        onValueChange={(value) => {
                            const trimmed = value.trim();

                            if (trimmed.toLowerCase().startsWith("unlimited")) {
                                setInputValue("Unlimited");
                                return;
                            }

                            if (trimmed === "") {
                                setInputValue("");
                                return;
                            }

                            const numericOnly = trimmed.replace(/[^0-9]/g, "");
                            setInputValue(numericOnly);
                        }}
                        className="h-9"
                    />
                    <CommandList>
                        <CommandGroup heading="Presets">
                            <CommandItem
                                value="1000"
                                onSelect={() => setInputValue("1000")}>
                                1000
                            </CommandItem>
                            <CommandItem
                                value="10000"
                                onSelect={() => setInputValue("10000")}>
                                10000
                            </CommandItem>
                            <CommandItem
                                value="unlimited"
                                onSelect={() => setInputValue("Unlimited")}>
                                Unlimited
                            </CommandItem>
                        </CommandGroup>
                        <div className="p-2">
                            <Button
                                className="w-full"
                                disabled={!canSave}
                                onClick={() => {
                                    if (!canSave) return;

                                    const raw = inputValue.trim();

                                    let nextLimit = limit;
                                    if (raw.toLowerCase() === "unlimited") {
                                        nextLimit = -1;
                                    } else {
                                        const parsed = Number(raw);
                                        if (!Number.isNaN(parsed)) {
                                            nextLimit = parsed;
                                        }
                                    }

                                    setLimit(nextLimit);
                                    setOpen(false);
                                }}>
                                Save
                            </Button>
                        </div>
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    );
};
