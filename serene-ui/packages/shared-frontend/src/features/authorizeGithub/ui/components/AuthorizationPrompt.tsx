import { Button, GithubIcon } from "@serene-ui/shared-frontend/shared";
import { UnauthorizeGithubButton } from "../UnauthorizeGithubButton";

interface AuthorizationPromptProps {
    authorized: boolean;
    onStartAuthorization: () => void;
}

/**
 * Initial authorization prompt component.
 * Displays the button to start GitHub authorization and disconnect button if already authorized.
 */
export const AuthorizationPrompt = ({
    authorized,
    onStartAuthorization,
}: AuthorizationPromptProps) => {
    return (
        <div className="flex gap-2 mt-2 w-full">
            <Button
                onClick={onStartAuthorization}
                variant="default"
                disabled={authorized}
                className="gap-2 flex-1">
                <GithubIcon className="size-4" />
                {authorized
                    ? "Authorized with GitHub"
                    : "Authorize with GitHub"}
            </Button>

            {authorized && <UnauthorizeGithubButton />}
        </div>
    );
};
