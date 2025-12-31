import { type RouteObject } from "react-router-dom";
import { AppLayout } from "@/app/layouts";
import { ConsolePage } from "@/pages/console";
import { navigationMap } from "@serene-ui/shared-frontend/shared";
import { FramesPage } from "@/pages/frames/ui/FramesPage";
import { ReplicationPage } from "@/pages/replication/ui/ReplicationPage";

const appRoutes: RouteObject[] = [
    {
        path: navigationMap.console,
        element: <ConsolePage />,
    },
    {
        path: navigationMap.frames,
        element: <FramesPage />,
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
