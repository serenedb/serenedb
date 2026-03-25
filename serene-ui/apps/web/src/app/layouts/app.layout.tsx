import { useLocation } from "react-router-dom";
import { AppSidebar, Toaster } from "@serene-ui/shared-frontend";
import { WithEntities, WithFeatures, WithPages } from "../providers";

export const AppLayout = ({ children }: { children: React.ReactNode }) => {
    const location = useLocation();

    return (
        <WithEntities>
            <WithFeatures>
                <WithPages>
                    <Toaster richColors />
                    <div className="flex w-dvw">
                        <AppSidebar />
                        <div
                            key={location.pathname}
                            className="h-dvh page-fade flex flex-1">
                            {children}
                        </div>
                    </div>
                </WithPages>
            </WithFeatures>
        </WithEntities>
    );
};
