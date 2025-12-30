import { toast } from "sonner";

export enum AuthorizationResultType {
    INFO = "info",
    WARNING = "warning",
    ERROR = "error",
    SUCCESS = "success",
}

export enum AuthorizationErrorType {
    SLOW_DOWN = "slow_down",
    AUTHORIZATION_PENDING = "authorization_pending",
    EXPIRED_TOKEN = "expired_token",
    UNKNOWN = "unknown",
}

export interface AuthorizationResult {
    message: string;
    type: AuthorizationResultType;
    errorType?: AuthorizationErrorType;
}

/**
 * Handles authorization verification responses and returns appropriate
 * error/info messages for user feedback.
 *
 * @param response - The verification authorization response from the API
 * @returns AuthorizationResult with message and type
 */
export const handleAuthorizationResult = ({
    success,
    error,
    interval,
}: {
    success: boolean;
    error?: string;
    interval?: number;
}): AuthorizationResult => {
    if (success) {
        return {
            message: "Authorization successful!",
            type: AuthorizationResultType.SUCCESS,
        };
    }

    switch (error) {
        case AuthorizationErrorType.SLOW_DOWN:
            return {
                message: `${interval || 15} seconds left before you can verify again`,
                type: AuthorizationResultType.INFO,
                errorType: AuthorizationErrorType.SLOW_DOWN,
            };
        case AuthorizationErrorType.AUTHORIZATION_PENDING:
            return {
                message: "You need to authorize in GitHub",
                type: AuthorizationResultType.INFO,
                errorType: AuthorizationErrorType.AUTHORIZATION_PENDING,
            };
        case AuthorizationErrorType.EXPIRED_TOKEN:
            return {
                message: "Token expired. Refreshing code.",
                type: AuthorizationResultType.WARNING,
                errorType: AuthorizationErrorType.EXPIRED_TOKEN,
            };
        default:
            return {
                message: "Unknown authorization error",
                type: AuthorizationResultType.ERROR,
                errorType: AuthorizationErrorType.UNKNOWN,
            };
    }
};

/**
 * Displays appropriate toast notification based on the authorization error result
 *
 * @param result - The authorization error result
 */
export const showAuthorizationToast = (result: AuthorizationResult) => {
    const toastMap = {
        info: toast.info,
        warning: toast.info,
        error: toast.error,
        success: toast.success,
    };

    toastMap[result.type](result.message);
};
