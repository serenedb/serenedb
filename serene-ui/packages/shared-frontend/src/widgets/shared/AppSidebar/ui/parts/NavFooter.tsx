import {
    SidebarTrigger,
    Button,
    GithubIcon,
} from "@serene-ui/shared-frontend/shared";

export const NavFooter = () => {
    return (
        <div className="flex gap-2">
            <SidebarTrigger />
            <Button
                className="group-data-[collapsible=icon]:hidden"
                size="icon"
                variant="secondary"
                title="Open GitHub repository">
                <GithubIcon />
            </Button>
        </div>
    );
};
