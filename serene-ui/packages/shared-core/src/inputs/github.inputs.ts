import z from "zod";

export const VerifyDeviceAuthorizationInput = z.object({
    device_code: z.string().max(256),
});
export type VerifyDeviceAuthorizationInput = z.infer<
    typeof VerifyDeviceAuthorizationInput
>;

export const CreateIssueInput = z.object({
    title: z.string().min(3).max(255),
    body: z.string().min(3).max(10000),
});
export type CreateIssueInput = z.infer<typeof CreateIssueInput>;
