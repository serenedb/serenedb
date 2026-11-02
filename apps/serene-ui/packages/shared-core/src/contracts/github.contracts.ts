import { oc } from "@orpc/contract";
import {
    CreateIssueInput,
    VerifyDeviceAuthorizationInput,
} from "../inputs/github.inputs";
import {
    CreateIssueOutput,
    StartDeviceAuthorizationOutput,
    VerifyDeviceAuthorizationOutput,
    VerifyUserAuthorizationOutput,
} from "../outputs/github.outputs";

export const startDeviceAuthorizationContract = oc.output(
    StartDeviceAuthorizationOutput,
);

export const verifyDeviceAuthorizationContract = oc
    .input(VerifyDeviceAuthorizationInput)
    .output(VerifyDeviceAuthorizationOutput);

export const verifyUserAuthorizationContract = oc.output(
    VerifyUserAuthorizationOutput,
);

export const leaveStarContract = oc;

export const unauthorizeContract = oc;
export const createIssue = oc.input(CreateIssueInput).output(CreateIssueOutput);

export const githubContracts = {
    vetification: {
        start: startDeviceAuthorizationContract,
        verifyDevice: verifyDeviceAuthorizationContract,
    },
    authorization: {
        verify: verifyUserAuthorizationContract,
        unauthorize: unauthorizeContract,
    },
    actions: {
        leaveStar: leaveStarContract,
        createIssue: createIssue,
    },
};
