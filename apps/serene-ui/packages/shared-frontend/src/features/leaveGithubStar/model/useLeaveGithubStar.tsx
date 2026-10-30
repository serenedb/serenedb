import { useLeaveStar } from "@serene-ui/shared-frontend/entities";

export const useLeaveGithubStar = () => {
    const leaveStarMutation = useLeaveStar();

    const leaveStar = async () => {
        try {
            await leaveStarMutation.mutateAsync({});
        } catch (error) {
            console.error("Failed to star repository:", error);
        }
    };

    return {
        leaveStar,
    };
};
