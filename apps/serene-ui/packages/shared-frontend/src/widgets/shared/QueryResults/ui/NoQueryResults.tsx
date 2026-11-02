import { CommandShortcut } from "@serene-ui/shared-frontend/shared";

export const NoQueryResults = () => {
    return (
        <div className="flex flex-col flex-1 items-center justify-center gap-2">
            <div className="flex gap-2">
                <div className="flex flex-col items-end gap-2 w-fit">
                    <div className="h-6 flex items-center text-secondary-foreground">
                        <p className="text-sm">Run query</p>
                    </div>
                    <div className="h-6 flex items-center text-secondary-foreground">
                        <p className="text-sm">Explain query</p>
                    </div>
                    <div className="h-6 flex items-center text-secondary-foreground">
                        <p className="text-sm">New tab</p>
                    </div>
                </div>
                <div className="flex flex-col items-start gap-2 w-fit">
                    <CommandShortcut className="bg-secondary py-1 px-2 text-secondary-foreground rounded-md ml-0">
                        ⌘ + Enter
                    </CommandShortcut>
                    <CommandShortcut className="bg-secondary py-1 px-2 text-secondary-foreground rounded-md ml-0">
                        ⌘ + Shift + Enter
                    </CommandShortcut>
                    <CommandShortcut className="bg-secondary py-1 px-2 text-secondary-foreground rounded-md ml-0">
                        ⌘ + T
                    </CommandShortcut>
                </div>
            </div>
        </div>
    );
};
