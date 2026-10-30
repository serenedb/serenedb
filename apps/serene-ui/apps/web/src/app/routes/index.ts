import appRoutes from "./app.routes";
import loaderRoutes from "./loader.routes";

export const buildRoutes = () => {
    return [...appRoutes, ...loaderRoutes];
};
