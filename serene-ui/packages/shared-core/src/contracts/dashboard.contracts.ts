import { oc } from "@orpc/contract";
import {
    AddDashboardInput,
    DeleteDashboardInput,
    GetDashboardInput,
    UpdateDashboardInput,
} from "../inputs";
import {
    AddDashboardOutput,
    DeleteDashboardOutput,
    GetDashboardOutput,
    ListMyDashboardsOutput,
    UpdateDashboardOutput,
} from "../outputs";

export const listMyDashboards = oc.output(ListMyDashboardsOutput);
export const getDashboard = oc
    .input(GetDashboardInput)
    .output(GetDashboardOutput);
export const addDashboard = oc
    .input(AddDashboardInput)
    .output(AddDashboardOutput);
export const updateDashboard = oc
    .input(UpdateDashboardInput)
    .output(UpdateDashboardOutput);
export const deleteDashboard = oc
    .input(DeleteDashboardInput)
    .output(DeleteDashboardOutput);

export const dashboardContracts = {
    listMy: listMyDashboards,
    get: getDashboard,
    add: addDashboard,
    update: updateDashboard,
    delete: deleteDashboard,
};
