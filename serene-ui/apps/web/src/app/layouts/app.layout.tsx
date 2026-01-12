import { useLocation } from "react-router-dom";
import {
    SidebarInset,
    SidebarProvider,
    Toaster,
} from "@serene-ui/shared-frontend";
import { AppSidebar } from "@serene-ui/shared-frontend";
import { WithEntities, WithFeatures, WithPages } from "../providers";

export const AppLayout = ({ children }: { children: React.ReactNode }) => {
    const location = useLocation();

    return (
        <WithEntities>
            <WithFeatures>
                <WithPages>
                    <SidebarProvider defaultOpen={false}>
                        <Toaster richColors />
                        <AppSidebar />
                        <SidebarInset className="overflow-hidden">
                            <div
                                key={location.pathname}
                                className="h-dvh page-fade flex">
                                {children}
                            </div>
                        </SidebarInset>
                    </SidebarProvider>
                </WithPages>
            </WithFeatures>
        </WithEntities>
    );
};
