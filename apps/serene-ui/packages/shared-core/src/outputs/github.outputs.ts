import z from "zod";

export const StartDeviceAuthorizationOutput = z.object({
    user_code: z.string(),
    device_code: z.string(),
    verification_uri: z.string(),
    expires_in: z.number(),
    interval: z.number(),
});
export type StartDeviceAuthorizationOutput = z.infer<
    typeof StartDeviceAuthorizationOutput
>;
export const VerifyDeviceAuthorizationOutput = z.object({
    success: z.boolean(),
    error: z.string().optional(),
    interval: z.number().optional(),
});
export type VerifyDeviceAuthorizationOutput = z.infer<
    typeof VerifyDeviceAuthorizationOutput
>;

export const VerifyUserAuthorizationOutput = z.object({
    authorized: z.boolean(),
});
export type VerifyUserAuthorizationOutput = z.infer<
    typeof VerifyUserAuthorizationOutput
>;

export const CreateIssueOutput = z.object({
    issue_number: z.number(),
    issue_url: z.string(),
});
export type CreateIssueOutput = z.infer<typeof CreateIssueOutput>;
