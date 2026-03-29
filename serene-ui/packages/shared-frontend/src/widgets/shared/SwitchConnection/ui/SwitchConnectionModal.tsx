import React from "react";
import {
    ComboboxBanner,
    ConnectionIcon,
    ConnectionsCombobox,
    DatabasesCombobox,
    DatabaseIcon,
    useConnection,
} from "@serene-ui/shared-frontend";

interface SwitchConnectionModalProps {
    open?: boolean;
    onComplete?: () => void;
}

export const SwitchConnectionModal: React.FC<SwitchConnectionModalProps> = ({
    open = false,
    onComplete,
}) => {
    const { currentConnection } = useConnection();
    const [databaseSelectorVersion, setDatabaseSelectorVersion] =
        React.useState(0);
    const containerRef = React.useRef<HTMLDivElement | null>(null);

    const focusSearchInput = React.useCallback((index: number) => {
        requestAnimationFrame(() => {
            const inputs =
                containerRef.current?.querySelectorAll<HTMLInputElement>(
                    'input[data-slot="command-input"]',
                );

            const input = inputs?.[index];

            if (!input) {
                return;
            }

            input.focus();
            input.select();
        });
    }, []);

    React.useEffect(() => {
        if (!open) {
            return;
        }

        focusSearchInput(0);
    }, [focusSearchInput, open]);

    React.useEffect(() => {
        if (databaseSelectorVersion === 0) {
            return;
        }

        focusSearchInput(1);
    }, [databaseSelectorVersion, focusSearchInput]);

    return (
        <div ref={containerRef} className="grid min-w-0 grid-cols-2 gap-1">
            <div className="min-w-0 space-y-2">
                <div className="pl-2 text-secondary-foreground/70 flex items-center gap-2 px-1 text-[11px] uppercase tracking-[0.08em]">
                    <ConnectionIcon className="size-3.5" />
                    Connection
                </div>
                <ConnectionsCombobox
                    variant="inline"
                    autoFocus
                    panelClassName="overflow-hidden rounded-md border border-border bg-transparent"
                    listClassName="h-[188px]"
                    onSelect={() => {
                        setDatabaseSelectorVersion((version) => version + 1);
                    }}
                />
            </div>

            <div className="min-w-0 space-y-2">
                <div className="pl-2 text-secondary-foreground/70 flex items-center gap-2 px-1 text-[11px] uppercase tracking-[0.08em]">
                    <DatabaseIcon className="size-3.5" />
                    Database
                </div>

                {currentConnection.connectionId === -1 ? (
                    <ComboboxBanner className="min-h-[188px]">
                        Select database
                    </ComboboxBanner>
                ) : (
                    <DatabasesCombobox
                        key={`${currentConnection.connectionId}:${databaseSelectorVersion}`}
                        variant="inline"
                        autoFocus={false}
                        panelClassName="overflow-hidden rounded-md border border-border bg-transparent"
                        listClassName="h-[220px]"
                        onSelect={() => {
                            onComplete?.();
                        }}
                    />
                )}
            </div>
        </div>
    );
};
