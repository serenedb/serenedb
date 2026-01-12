import { VerifyUserAuthorizationOutput } from "@serene-ui/shared-core";
import { useQuery, useMutation, UseQueryOptions } from "@tanstack/react-query";
import { orpc } from "../../../shared/api/orpc";

export const useStartDeviceAuthorization = () => {
    return useMutation(orpc.github.vetification.start.mutationOptions());
};

export const useVerifyDeviceAuthorization = () => {
    return useMutation(orpc.github.vetification.verifyDevice.mutationOptions());
};

export const useVerifyUserAuthorization = (
    props?: Partial<UseQueryOptions<VerifyUserAuthorizationOutput, Error>>,
) => {
    return useQuery(
        orpc.github.authorization.verify.queryOptions({
            queryKey: ["github-authorized"],
            ...props,
        }),
    );
};

export const useLeaveStar = () => {
    return useMutation(orpc.github.actions.leaveStar.mutationOptions());
};

export const useUnauthorize = () => {
    return useMutation(orpc.github.authorization.unauthorize.mutationOptions());
};

export const useCreateIssue = () => {
    return useMutation(orpc.github.actions.createIssue.mutationOptions());
};
