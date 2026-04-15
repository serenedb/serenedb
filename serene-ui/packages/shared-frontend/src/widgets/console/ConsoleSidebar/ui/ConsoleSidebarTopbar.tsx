import { Button, ThreeDotsIcon } from "@serene-ui/shared-frontend";

export const ConsoleSidebarTopbar = () => {
    return (
        <div className="min-h-[48.5px] pl-4 pr-2.5 py-2.5 justify-between items-center flex border-b-[0.5px]">
            <p className="uppercase text-foreground dark:text-secondary-foreground font-black text-xs ">
                Console
            </p>
            <Button variant="ghost" size={"icon"}>
                <ThreeDotsIcon />
            </Button>
        </div>
    );
};
