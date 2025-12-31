import { LogoLoader } from "@serene-ui/shared-frontend/widgets";

export const LoadingScreen = () => {
    return (
        <div className="flex items-center justify-center h-dvh w-full">
            <LogoLoader />
        </div>
    );
};
