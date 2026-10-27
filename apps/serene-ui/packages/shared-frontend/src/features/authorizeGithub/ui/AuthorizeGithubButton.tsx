import { useAuthorizeGithub } from "../model";
import {
    AuthorizationPrompt,
    AuthorizationCodeForm,
    AuthorizationTimer,
    StarAgreement,
} from "./components";

/**
 * A button for authorizing GitHub.
 */
export const AuthorizeGithubButton = () => {
    const {
        startAuthorization,
        authorized,
        code,
        verifyAuthorization,
        authLink,
        codeTimeLeft,
        starAllowed,
        setStarAllowed,
    } = useAuthorizeGithub();

    if (!code || !authLink) {
        return (
            <AuthorizationPrompt
                authorized={authorized}
                onStartAuthorization={startAuthorization}
            />
        );
    }
    return (
        <div className="py-3 px-3 border border-border rounded-md gap-3 flex flex-col">
            {codeTimeLeft !== null && (
                <AuthorizationTimer codeTimeLeft={codeTimeLeft} />
            )}

            <AuthorizationCodeForm
                code={code}
                authLink={authLink}
                onVerify={verifyAuthorization}
            />

            <StarAgreement
                checked={starAllowed}
                onCheckedChange={setStarAllowed}
            />
        </div>
    );
};
