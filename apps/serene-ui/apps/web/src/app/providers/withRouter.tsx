import React from "react";
import {
    createBrowserRouter,
    createHashRouter,
    RouterProvider,
} from "react-router-dom";
import { buildRoutes } from "@/app/routes";

const mode = import.meta.env.MODE;

export const WithRouter: React.FC = () => {
    const routes = buildRoutes();
    const router = mode.includes("electron")
        ? createHashRouter(routes)
        : createBrowserRouter(routes);

    return <RouterProvider router={router} />;
};
