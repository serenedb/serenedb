import { implement } from "@orpc/server";
import { apiContracts } from "@serene-ui/shared-core";
import { DashboardService } from "@serene-ui/shared-backend";

const os = implement(apiContracts.dashboard);

export const addDashboard = os.add.handler(async ({ input }) => {
    return await DashboardService.addDashboard(input);
});

export const listMyDashboards = os.listMy.handler(async () => {
    return await DashboardService.listMyDashboards();
});

export const getDashboard = os.get.handler(async ({ input }) => {
    return await DashboardService.getDashboard(input);
});

export const updateDashboard = os.update.handler(async ({ input }) => {
    return await DashboardService.updateDashboard(input);
});

export const deleteDashboard = os.delete.handler(async ({ input }) => {
    return await DashboardService.deleteDashboard(input);
});

export const DashboardRouter = os.router({
    add: addDashboard,
    listMy: listMyDashboards,
    get: getDashboard,
    update: updateDashboard,
    delete: deleteDashboard,
});
