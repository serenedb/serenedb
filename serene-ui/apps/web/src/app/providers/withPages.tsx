import { ConsoleProvider } from "@/pages/console/model";
import { DashboardPageProvider } from "@/pages/dashboards/model";

export const WithPages = ({ children }: { children: React.ReactNode }) => {
    return (
        <ConsoleProvider>
            <DashboardPageProvider>{children}</DashboardPageProvider>
        </ConsoleProvider>
    );
};
