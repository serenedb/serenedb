import { DashboardPageProvider } from "@/pages/dashboards/model";

export const WithPages = ({ children }: { children: React.ReactNode }) => {
    return <DashboardPageProvider>{children}</DashboardPageProvider>;
};
