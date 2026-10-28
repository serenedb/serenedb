import { call, implement } from "@orpc/server";
import assert from "node:assert";
import { describe, it } from "node:test";
import { apiContracts } from "@serene-ui/shared-core";

const os = implement(apiContracts.github);

describe("GithubRouter", () => {
    describe("authorization", () => {
        describe("verify", () => {
            it("should verify authorized user", async () => {
                const mockAuthStatus = {
                    authorized: true,
                };

                const fakeVerify = os.authorization.verify.handler(
                    async () => mockAuthStatus,
                );

                const result = await call(fakeVerify, undefined);

                assert.deepStrictEqual(result, mockAuthStatus);
            });

            it("should return unauthorized status", async () => {
                const mockAuthStatus = {
                    authorized: false,
                };

                const fakeVerify = os.authorization.verify.handler(
                    async () => mockAuthStatus,
                );

                const result = await call(fakeVerify, undefined);

                assert.deepStrictEqual(result, mockAuthStatus);
            });
        });

        describe("unauthorize", () => {
            it("should unauthorize user successfully", async () => {
                const fakeUnauthorize = os.authorization.unauthorize.handler(
                    async () => undefined,
                );

                const result = await call(fakeUnauthorize, undefined);

                assert.strictEqual(result, undefined);
            });
        });
    });

    describe("verification (device flow)", () => {
        describe("start", () => {
            it("should start device authorization flow", async () => {
                const mockDeviceAuth = {
                    device_code: "abc123def456",
                    user_code: "ABCD-1234",
                    verification_uri: "https://github.com/login/device",
                    expires_in: 900,
                    interval: 5,
                };

                const fakeStart = os.vetification.start.handler(
                    async () => mockDeviceAuth,
                );

                const result = await call(fakeStart, undefined);

                assert.deepStrictEqual(result, mockDeviceAuth);
            });

            it("should start device authorization with custom expiry", async () => {
                const mockDeviceAuth = {
                    device_code: "xyz789",
                    user_code: "WXYZ-9876",
                    verification_uri: "https://github.com/login/device",
                    expires_in: 600,
                    interval: 10,
                };

                const fakeStart = os.vetification.start.handler(
                    async () => mockDeviceAuth,
                );

                const result = await call(fakeStart, undefined);

                assert.deepStrictEqual(result, mockDeviceAuth);
            });
        });

        describe("verifyDevice", () => {
            it("should verify device successfully", async () => {
                const mockVerification = {
                    success: true,
                };

                const fakeVerifyDevice = os.vetification.verifyDevice.handler(
                    async () => mockVerification,
                );

                const result = await call(fakeVerifyDevice, {
                    device_code: "abc123def456",
                });

                assert.deepStrictEqual(result, mockVerification);
            });

            it("should return pending status", async () => {
                const mockVerification = {
                    success: false,
                    error: "authorization_pending",
                    interval: 5,
                };

                const fakeVerifyDevice = os.vetification.verifyDevice.handler(
                    async () => mockVerification,
                );

                const result = await call(fakeVerifyDevice, {
                    device_code: "abc123def456",
                });

                assert.deepStrictEqual(result, mockVerification);
            });

            it("should return slow down status", async () => {
                const mockVerification = {
                    success: false,
                    error: "slow_down",
                    interval: 10,
                };

                const fakeVerifyDevice = os.vetification.verifyDevice.handler(
                    async () => mockVerification,
                );

                const result = await call(fakeVerifyDevice, {
                    device_code: "abc123def456",
                });

                assert.deepStrictEqual(result, mockVerification);
            });

            it("should return expired status", async () => {
                const mockVerification = {
                    success: false,
                    error: "expired_token",
                };

                const fakeVerifyDevice = os.vetification.verifyDevice.handler(
                    async () => mockVerification,
                );

                const result = await call(fakeVerifyDevice, {
                    device_code: "expired_code",
                });

                assert.deepStrictEqual(result, mockVerification);
            });

            it("should return access denied status", async () => {
                const mockVerification = {
                    success: false,
                    error: "access_denied",
                };

                const fakeVerifyDevice = os.vetification.verifyDevice.handler(
                    async () => mockVerification,
                );

                const result = await call(fakeVerifyDevice, {
                    device_code: "denied_code",
                });

                assert.deepStrictEqual(result, mockVerification);
            });
        });
    });

    describe("actions", () => {
        describe("leaveStar", () => {
            it("should star the repository successfully", async () => {
                const fakeLeaveStar = os.actions.leaveStar.handler(
                    async () => undefined,
                );

                const result = await call(fakeLeaveStar, undefined);

                assert.strictEqual(result, undefined);
            });
        });

        describe("createIssue", () => {
            it("should create an issue successfully", async () => {
                const mockIssue = {
                    issue_number: 123,
                    issue_url: "https://github.com/owner/repo/issues/123",
                };

                const fakeCreateIssue = os.actions.createIssue.handler(
                    async () => mockIssue,
                );

                const result = await call(fakeCreateIssue, {
                    title: "Bug Report: Application crashes on startup",
                    body: "When I start the application, it immediately crashes with the following error...",
                });

                assert.deepStrictEqual(result, mockIssue);
            });

            it("should create a feature request issue", async () => {
                const mockIssue = {
                    issue_number: 124,
                    issue_url: "https://github.com/owner/repo/issues/124",
                };

                const fakeCreateIssue = os.actions.createIssue.handler(
                    async () => mockIssue,
                );

                const result = await call(fakeCreateIssue, {
                    title: "Feature Request: Add dark mode support",
                    body: "It would be great if the application supported dark mode for better user experience.",
                });

                assert.deepStrictEqual(result, mockIssue);
            });

            it("should create a documentation issue", async () => {
                const mockIssue = {
                    issue_number: 125,
                    issue_url: "https://github.com/owner/repo/issues/125",
                };

                const fakeCreateIssue = os.actions.createIssue.handler(
                    async () => mockIssue,
                );

                const result = await call(fakeCreateIssue, {
                    title: "Documentation: Update installation guide",
                    body: "The installation guide needs to be updated with the latest version requirements and steps.",
                });

                assert.deepStrictEqual(result, mockIssue);
            });

            it("should create an issue with long body content", async () => {
                const mockIssue = {
                    issue_number: 126,
                    issue_url: "https://github.com/owner/repo/issues/126",
                };

                const fakeCreateIssue = os.actions.createIssue.handler(
                    async () => mockIssue,
                );

                const longBody = `
## Description
This is a detailed bug report with multiple sections.

## Steps to Reproduce
1. Open the application
2. Navigate to settings
3. Click on the advanced tab
4. Try to change configuration

## Expected Behavior
The configuration should update successfully.

## Actual Behavior
The application crashes with an error.

## Environment
- OS: macOS 14.0
- Node Version: 20.10.0
- App Version: 1.2.3
                `.trim();

                const result = await call(fakeCreateIssue, {
                    title: "Bug: Configuration update causes crash",
                    body: longBody,
                });

                assert.deepStrictEqual(result, mockIssue);
            });
        });
    });
});
