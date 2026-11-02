import { type RouteObject } from "react-router-dom";
import { LoaderLayout } from "@/app/layouts";
import { LoadingScreen } from "@/pages/loading/ui/LoadingPage";

const loaderRoutes: RouteObject[] = [
    {
        path: "/",
        element: <LoadingScreen />,
    },
];

for (let i = 0; i < loaderRoutes.length; i++) {
    loaderRoutes[i].element = (
        <LoaderLayout>{loaderRoutes[i].element}</LoaderLayout>
    );
}

export default loaderRoutes;
