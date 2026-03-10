import { type RouteObject } from "react-router-dom";
import { AppLayout } from "@/app/layouts";
import { ConsolePage } from "@/pages/console";
import { navigationMap } from "@serene-ui/shared-frontend/shared";
import { DashboardsPage } from "@/pages/dashboards/ui/DashboardsPage";
import { ReplicationPage } from "@/pages/replication/ui/ReplicationPage";

const appRoutes: RouteObject[] = [
    {
        path: navigationMap.console,
        element: <ConsolePage />,
    },
    {
        path: navigationMap.frames,
        element: <DashboardsPage />,
    },
    {
        path: navigationMap.replication,
        element: <ReplicationPage />,
    },
];

for (let i = 0; i < appRoutes.length; i++) {
    appRoutes[i].element = <AppLayout>{appRoutes[i].element}</AppLayout>;
}

export default appRoutes;
