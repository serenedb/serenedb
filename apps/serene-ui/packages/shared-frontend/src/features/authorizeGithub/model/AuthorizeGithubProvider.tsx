import { PropsWithChildren, useState } from "react";
import { AuthorizeGithubContext } from "./AuthorizeGithubContext";
import { toast } from "sonner";
import {
    useStartDeviceAuthorization,
    useVerifyDeviceAuthorization,
    useVerifyUserAuthorization,
    useUnauthorize,
} from "@serene-ui/shared-frontend/entities";
import { useLeaveGithubStar } from "@serene-ui/shared-frontend/features";
import { useDeviceAuthorizationTimer, useVerifySlowdownTimer } from "./hooks";
import {
    AuthorizationErrorType,
    AuthorizationResultType,
    handleAuthorizationResult,
    showAuthorizationToast,
} from "./utils";

export const AuthorizeGithubProvider = ({ children }: PropsWithChildren) => {
    const [starAllowed, setStarAllowed] = useState(true);
    const [code, setCode] = useState<string | null>(null);
    const [codeTimeLeft, setCodeTimeLeft] = useState<number | null>(null);
    const [authLink, setAuthLink] = useState<string | null>(null);
    const [deviceCode, setDeviceCode] = useState<string | null>(null);

    const startAuthMutation = useStartDeviceAuthorization();
    const verifyAuthMutation = useVerifyDeviceAuthorization();
    const { data: authStatus, refetch: refetchAuthStatus } =
        useVerifyUserAuthorization();
    const { leaveStar } = useLeaveGithubStar();
    const unauthorizeMutation = useUnauthorize();

    const { verifySlowDownTimer, setVerifySlowDownTimer } =
        useVerifySlowdownTimer({ initialValue: null });

    const authorized = authStatus?.authorized ?? false;

    const startAuthorization = async () => {
        try {
            const data = await startAuthMutation.mutateAsync({});
            setCode(data.user_code);
            setCodeTimeLeft(data.expires_in);
            setAuthLink(data.verification_uri);
            setDeviceCode(data.device_code);
        } catch (error) {
            console.error(error);
            toast.error("Failed to start authorization");
        }
    };

    useDeviceAuthorizationTimer({
        codeTimeLeft,
        onTimeExpired: startAuthorization,
    });

    const verifyAuthorization = async () => {
        if (verifySlowDownTimer !== null) {
            toast.info(
                `${verifySlowDownTimer} seconds left before you can verify again`,
            );
            return;
        }
        if (!deviceCode) {
            toast.error("No device code available");
            return;
        }

        try {
            const data = await verifyAuthMutation.mutateAsync({
                device_code: deviceCode,
            });

            const result = handleAuthorizationResult(data);
            showAuthorizationToast(result);

            if (result.type === AuthorizationResultType.SUCCESS) {
                if (starAllowed) {
                    await leaveStar();
                }
                setCode(null);
                setAuthLink(null);
                setDeviceCode(null);
                setVerifySlowDownTimer(null);
                await refetchAuthStatus();
            }

            switch (result.errorType) {
                case AuthorizationErrorType.SLOW_DOWN:
                    setVerifySlowDownTimer(data.interval || 15);
                    break;
                case AuthorizationErrorType.EXPIRED_TOKEN:
                    startAuthorization();
                    break;
            }
        } catch (error) {
            console.error(error);
            toast.error("Failed to verify authorization");
        }
    };

    const unauthorizeGithub = async () => {
        try {
            await unauthorizeMutation.mutateAsync({});
            await refetchAuthStatus();
            toast.success("Successfully unauthorized from GitHub");
        } catch (error) {
            console.error(error);
            toast.error("Failed to unauthorize");
        }
    };

    return (
        <AuthorizeGithubContext.Provider
            value={{
                authorized,
                code,
                codeTimeLeft,
                authLink,
                startAuthorization,
                verifyAuthorization,
                unauthorizeGithub,
                starAllowed,
                setStarAllowed,
            }}>
            {children}
        </AuthorizeGithubContext.Provider>
    );
};
