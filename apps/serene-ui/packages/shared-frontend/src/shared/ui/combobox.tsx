import * as React from "react";
import { cn } from "../lib/utils";
import {
    Command,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from "./command";
import { CheckIcon, LoaderIcon } from "./icons";

export interface ComboboxOption {
    value: string;
    label: string;
    searchText?: string;
}

export interface ComboboxPanelProps {
    items: ComboboxOption[];
    selectedValue?: string;
    placeholder: string;
    emptyMessage: string;
    onSelect: (value: string) => void;
    autoFocus?: boolean;
    isLoading?: boolean;
    loadingMessage?: string;
    className?: string;
    listClassName?: string;
    itemClassName?: string;
    footer?: React.ReactNode;
    inputProps?: Omit<
        React.ComponentProps<typeof CommandInput>,
        "autoFocus" | "className" | "onValueChange" | "placeholder" | "value"
    > & {
        className?: string;
        [key: string]: unknown;
    };
    getItemProps?: (
        item: ComboboxOption,
    ) => Omit<
        React.ComponentProps<typeof CommandItem>,
        "children" | "className" | "onSelect" | "value"
    > & {
        className?: string;
        [key: string]: unknown;
    };
}

export const ComboboxPanel: React.FC<ComboboxPanelProps> = ({
    items,
    selectedValue,
    placeholder,
    emptyMessage,
    onSelect,
    autoFocus = false,
    isLoading = false,
    loadingMessage = "Loading...",
    className,
    listClassName,
    itemClassName,
    footer,
    inputProps,
    getItemProps,
}) => {
    const [searchValue, setSearchValue] = React.useState("");
    const [activeValue, setActiveValue] = React.useState("");
    const hasInitializedActiveValueRef = React.useRef(false);

    const filteredItems = React.useMemo(() => {
        const normalizedSearch = searchValue.trim().toLowerCase();

        if (!normalizedSearch) {
            return items;
        }

        return items.filter((item) => {
            const searchableText = [
                item.label,
                item.searchText,
                item.value,
            ]
                .filter(Boolean)
                .join(" ")
                .toLowerCase();

            return searchableText.includes(normalizedSearch);
        });
    }, [items, searchValue]);

    React.useEffect(() => {
        setActiveValue((currentValue) => {
            if (filteredItems.length === 0) {
                return "";
            }

            if (!hasInitializedActiveValueRef.current) {
                hasInitializedActiveValueRef.current = true;

                if (
                    selectedValue &&
                    filteredItems.some((item) => item.value === selectedValue)
                ) {
                    return selectedValue;
                }

                return filteredItems[0]?.value ?? "";
            }

            if (
                currentValue &&
                filteredItems.some((item) => item.value === currentValue)
            ) {
                return currentValue;
            }

            return filteredItems[0]?.value ?? "";
        });
    }, [filteredItems, selectedValue]);

    const { className: inputClassName, ...restInputProps } = inputProps ?? {};

    return (
        <Command
            loop
            shouldFilter={false}
            value={activeValue}
            onValueChange={setActiveValue}
            className={className}>
            <CommandInput
                autoFocus={autoFocus}
                placeholder={placeholder}
                value={searchValue}
                onValueChange={setSearchValue}
                className={inputClassName}
                {...restInputProps}
            />
            <CommandList className={listClassName}>
                {isLoading && items.length === 0 ? (
                    <div className="text-muted-foreground flex items-center gap-2 px-3 py-3 text-sm">
                        <LoaderIcon className="size-4 animate-spin" />
                        {loadingMessage}
                    </div>
                ) : (
                    <>
                        <CommandEmpty>{emptyMessage}</CommandEmpty>
                        <CommandGroup>
                            {filteredItems.map((item) => {
                                const itemProps = getItemProps?.(item);
                                const {
                                    className: nextItemClassName,
                                    ...restItemProps
                                } = itemProps ?? {};

                                return (
                                    <CommandItem
                                        key={item.value}
                                        value={item.value}
                                        onSelect={() => onSelect(item.value)}
                                        className={cn(
                                            "justify-between",
                                            itemClassName,
                                            nextItemClassName,
                                        )}
                                        {...restItemProps}>
                                        <span className="block min-w-0 flex-1 truncate">
                                            {item.label}
                                        </span>
                                        <CheckIcon
                                            className={cn(
                                                "ml-auto",
                                                selectedValue === item.value
                                                    ? "opacity-100"
                                                    : "opacity-0",
                                            )}
                                        />
                                    </CommandItem>
                                );
                            })}
                        </CommandGroup>
                    </>
                )}
            </CommandList>
            {footer}
        </Command>
    );
};

export const ComboboxBanner: React.FC<
    React.ComponentProps<"div">
> = ({ className, ...props }) => {
    return (
        <div
            className={cn(
                "border-border bg-muted/15 text-muted-foreground flex items-center justify-center rounded-md border border-dashed px-4 text-center text-sm",
                className,
            )}
            {...props}
        />
    );
};
