import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuShortcut,
    DropdownMenuTrigger,
} from "@serene-ui/shared-frontend/shared";

import type { VirtualizedTableContextMenuProps } from "../model";

export const VirtualizedTableContextMenu = ({
    contextMenu,
    onCopyCSV,
    onCopyJSON,
    onOpenChange,
}: VirtualizedTableContextMenuProps) => {
    if (contextMenu === null) {
        return null;
    }

    return (
        <DropdownMenu open onOpenChange={onOpenChange}>
            <DropdownMenuTrigger asChild>
                <button
                    type="button"
                    aria-hidden="true"
                    tabIndex={-1}
                    className="fixed h-px w-px opacity-0 pointer-events-none"
                    style={{
                        left: contextMenu.x,
                        top: contextMenu.y,
                    }}
                />
            </DropdownMenuTrigger>
            <DropdownMenuContent
                side="bottom"
                align="start"
                sideOffset={0}
                className="w-52"
                onCloseAutoFocus={(event) => event.preventDefault()}>
                <DropdownMenuItem onClick={() => void onCopyCSV()}>
                    Copy as CSV
                    <DropdownMenuShortcut>CMD+C</DropdownMenuShortcut>
                </DropdownMenuItem>
                <DropdownMenuItem onClick={() => void onCopyJSON()}>
                    Copy as JSON
                    <DropdownMenuShortcut>CMD+SHIFT+C</DropdownMenuShortcut>
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    );
};
