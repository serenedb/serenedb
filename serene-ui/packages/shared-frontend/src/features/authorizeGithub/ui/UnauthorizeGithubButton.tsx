import { Button, LogoutIcon } from "@serene-ui/shared-frontend/shared";
import { useAuthorizeGithub } from "../model";

export const UnauthorizeGithubButton = () => {
    const { unauthorizeGithub } = useAuthorizeGithub();
    return (
        <Button
            className="opacity-50 hover:opacity-100"
            variant="destructive"
            size="icon"
            title="Disconnect GitHub"
            onClick={unauthorizeGithub}>
            <LogoutIcon className="size-3" />
        </Button>
    );
};
