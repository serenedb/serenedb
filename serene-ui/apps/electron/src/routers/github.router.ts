import { implement } from "@orpc/server";
import { apiContracts } from "@serene-ui/shared-core";
import { GithubService } from "@serene-ui/shared-backend";

const os = implement(apiContracts.github);

export const verifyUserAuthorized = os.authorization.verify.handler(
    async () => {
        return await GithubService.verifyUserAuthorized();
    },
);

export const unauthorize = os.authorization.unauthorize.handler(async () => {
    return await GithubService.unauthorize();
});

export const startDeviceAuthorization = os.vetification.start.handler(
    async () => {
        return await GithubService.startDeviceAuthorization();
    },
);

export const verifyDeviceAuthorization = os.vetification.verifyDevice.handler(
    async ({ input }) => {
        return await GithubService.verifyDeviceAuthorization(input);
    },
);

export const leaveStar = os.actions.leaveStar.handler(async () => {
    return await GithubService.leaveStar();
});

export const createIssue = os.actions.createIssue.handler(async ({ input }) => {
    return await GithubService.createIssue(input);
});

export const GithubRouter = os.router({
    authorization: {
        verify: verifyUserAuthorized,
        unauthorize: unauthorize,
    },
    vetification: {
        start: startDeviceAuthorization,
        verifyDevice: verifyDeviceAuthorization,
    },
    actions: {
        leaveStar: leaveStar,
        createIssue: createIssue,
    },
});
