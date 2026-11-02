import { ORPCError } from "@orpc/server";
import {
    CreateIssueInput,
    CreateIssueOutput,
    StartDeviceAuthorizationOutput,
    VerifyDeviceAuthorizationInput,
    VerifyDeviceAuthorizationOutput,
    VerifyUserAuthorizationOutput,
} from "@serene-ui/shared-core";
import { GithubRepository } from "../../repositories";

export const GithubService = {
    startDeviceAuthorization:
        async (): Promise<StartDeviceAuthorizationOutput> => {
            const clientId = "Ov23li92Lcju9wglpRz4";
            const scope = ["public_repo"];

            const response = await fetch(
                `https://github.com/login/device/code`,
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        Accept: "application/json",
                    },
                    body: JSON.stringify({
                        client_id: clientId,
                        scope: scope.join(","),
                    }),
                },
            );

            const data = await response.json();

            if (!response.ok) {
                throw new ORPCError(
                    "INTERNAL_SERVER_ERROR",
                    data.error_description || "Failed to start authorization",
                );
            }

            return {
                user_code: data.user_code,
                device_code: data.device_code,
                verification_uri: data.verification_uri,
                expires_in: data.expires_in,
                interval: data.interval,
            };
        },

    verifyDeviceAuthorization: async (
        input: VerifyDeviceAuthorizationInput,
    ): Promise<VerifyDeviceAuthorizationOutput> => {
        const clientId = "Ov23li92Lcju9wglpRz4";

        const response = await fetch(
            `https://github.com/login/oauth/access_token`,
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Accept: "application/json",
                },
                body: JSON.stringify({
                    client_id: clientId,
                    device_code: input.device_code,
                    grant_type: "urn:ietf:params:oauth:grant-type:device_code",
                }),
            },
        );

        const responseData = await response.json();

        if (!response.ok || responseData.error || !responseData.access_token) {
            return {
                success: false,
                error: responseData.error || "unknown",
                interval: responseData.interval,
            };
        }

        GithubRepository.setAccessToken(responseData.access_token);

        return {
            success: true,
        };
    },

    verifyUserAuthorized: async () => {
        const token = GithubRepository.getAccessToken();

        return {
            authorized: !!token,
        };
    },

    getAccessToken: async () => {
        const token = GithubRepository.getAccessToken();
        return token;
    },

    deleteAccessToken: async () => {
        const deleted = GithubRepository.deleteAccessToken();
        return deleted;
    },

    leaveStar: async () => {
        const token = GithubRepository.getAccessToken();

        if (!token) {
            throw new ORPCError("BAD_REQUEST", {
                message: "No access token found",
            });
        }

        const response = await fetch(
            `https://api.github.com/user/starred/iljaname/iljaname.github.io`,
            {
                method: "PUT",
                headers: {
                    Authorization: `Bearer ${token}`,
                    "Content-Length": "0",
                    Accept: "application/vnd.github.v3+json",
                },
            },
        );

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new ORPCError(
                "INTERNAL_SERVER_ERROR",
                errorData.message || "Failed to star repository",
            );
        }
    },

    unauthorize: async () => {
        GithubRepository.deleteAccessToken();
    },

    createIssue: async (
        input: CreateIssueInput,
    ): Promise<CreateIssueOutput> => {
        const token = GithubRepository.getAccessToken();

        if (!token) {
            throw new ORPCError("BAD_REQUEST", {
                message: "No access token found",
            });
        }

        const response = await fetch(
            `https://api.github.com/repos/iljaname/iljaname.github.io/issues`,
            {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${token}`,
                    Accept: "application/vnd.github+json",
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    title: input.title,
                    body: input.body,
                }),
            },
        );

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new ORPCError("BAD_REQUEST", {
                message: errorData.message || "Failed to create issue",
            });
        }

        const responseData = await response.json();

        return {
            issue_url: responseData.html_url,
            issue_number: responseData.number,
        };
    },
};
