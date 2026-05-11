import { Button, ThreeDotsIcon } from "@serene-ui/shared-frontend";

export const ConsoleSidebarTopbar = () => {
    return (
        <div className="electron-drag-region flex min-h-[48.5px] items-center justify-between border-b-[0.5px] py-2.5 pl-4 pr-2.5">
            <p className="uppercase text-foreground dark:text-secondary-foreground font-black text-xs ">
                Console
            </p>
            <div className="electron-no-drag">
                <Button variant="ghost" size={"icon"}>
                    <ThreeDotsIcon />
                </Button>
            </div>
        </div>
    );
};
