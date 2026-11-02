import React from "react";
import {
    ComboboxBanner,
    ConnectionsCombobox,
    DatabasesCombobox,
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
                <ConnectionsCombobox
                    variant="inline"
                    autoFocus
                    panelClassName="overflow-hidden rounded-md border border-border bg-transparent"
                    listClassName="h-[200px]"
                    onSelect={() => {
                        setDatabaseSelectorVersion((version) => version + 1);
                    }}
                />
            </div>

            <div className="min-w-0 space-y-2">
                {currentConnection.connectionId === -1 ? (
                    <ComboboxBanner className="min-h-[240px]">
                        Select database
                    </ComboboxBanner>
                ) : (
                    <DatabasesCombobox
                        key={`${currentConnection.connectionId}:${databaseSelectorVersion}`}
                        variant="inline"
                        autoFocus={false}
                        panelClassName="overflow-hidden rounded-md border border-border bg-transparent"
                        listClassName="h-[200px]"
                        onSelect={() => {
                            onComplete?.();
                        }}
                    />
                )}
            </div>
        </div>
    );
};
