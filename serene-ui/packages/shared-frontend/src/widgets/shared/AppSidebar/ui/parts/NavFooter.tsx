import { Button, GithubIcon } from "@serene-ui/shared-frontend/shared";

export const NavFooter = () => {
    return (
        <div className="flex gap-2">
            <Button
                size="icon"
                variant="secondary"
                title="Open GitHub repository">
                <GithubIcon />
            </Button>
        </div>
    );
};
