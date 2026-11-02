import { call, implement } from "@orpc/server";
import assert from "node:assert";
import { describe, it } from "node:test";
import { apiContracts } from "@serene-ui/shared-core";

const os = implement(apiContracts.dashboard);

describe("DashboardRouter", () => {
    describe("add dashboard", () => {
        it("should add a dashboard successfully", async () => {
            const mockDashboard = {
                id: 1,
                name: "Sales Dashboard",
                favorite: false,
                auto_refresh: true,
                refresh_interval: 30,
                row_limit: 500,
                blocks: [],
            };

            const fakeAddDashboard = os.add.handler(async () => mockDashboard);

            const result = await call(fakeAddDashboard, {
                name: "Sales Dashboard",
                favorite: false,
                auto_refresh: true,
                refresh_interval: 30,
                row_limit: 500,
                blocks: [],
            });

            assert.deepStrictEqual(result, mockDashboard);
        });
    });

    describe("list dashboards", () => {
        it("should list all dashboards", async () => {
            const mockDashboards = [
                {
                    id: 1,
                    name: "Sales Dashboard",
                    favorite: false,
                    auto_refresh: true,
                    refresh_interval: 30,
                    row_limit: 500,
                    blocks: [],
                },
                {
                    id: 2,
                    name: "Operations Dashboard",
                    favorite: true,
                    auto_refresh: false,
                    refresh_interval: 60,
                    row_limit: 1000,
                    blocks: [],
                },
            ];

            const fakeListDashboards = os.listMy.handler(
                async () => mockDashboards,
            );

            const result = await call(fakeListDashboards, undefined);

            assert.deepStrictEqual(result, mockDashboards);
        });

        it("should list favorite dashboards", async () => {
            const favoriteDashboards = [
                {
                    id: 2,
                    name: "Operations Dashboard",
                    favorite: true,
                    auto_refresh: false,
                    refresh_interval: 60,
                    row_limit: 1000,
                    blocks: [],
                },
            ];

            const fakeListFavorites = os.favorites.handler(
                async () => favoriteDashboards,
            );

            const result = await call(fakeListFavorites, undefined);

            assert.deepStrictEqual(result, favoriteDashboards);
        });
    });

    describe("get dashboard", () => {
        it("should get dashboard by id", async () => {
            const mockDashboard = {
                id: 1,
                name: "Sales Dashboard",
                favorite: false,
                auto_refresh: true,
                refresh_interval: 30,
                row_limit: 500,
                blocks: [],
            };

            const fakeGetDashboard = os.get.handler(async () => mockDashboard);

            const result = await call(fakeGetDashboard, {
                id: 1,
            });

            assert.deepStrictEqual(result, mockDashboard);
        });
    });

    describe("update dashboard", () => {
        it("should update dashboard settings", async () => {
            const mockDashboard = {
                id: 1,
                name: "Updated Dashboard",
                favorite: true,
                auto_refresh: false,
                refresh_interval: 120,
                row_limit: 2000,
                blocks: [],
            };

            const fakeUpdateDashboard = os.update.handler(
                async () => mockDashboard,
            );

            const result = await call(fakeUpdateDashboard, {
                id: 1,
                name: "Updated Dashboard",
                favorite: true,
                auto_refresh: false,
                refresh_interval: 120,
                row_limit: 2000,
                blocks: [],
            });

            assert.deepStrictEqual(result, mockDashboard);
        });
    });

    describe("delete dashboard", () => {
        it("should delete dashboard by id", async () => {
            const deletedDashboard = {
                id: 1,
                name: "Sales Dashboard",
                favorite: false,
                auto_refresh: true,
                refresh_interval: 30,
                row_limit: 500,
                blocks: [],
            };

            const fakeDeleteDashboard = os.delete.handler(
                async () => deletedDashboard,
            );

            const result = await call(fakeDeleteDashboard, {
                id: 1,
            });

            assert.deepStrictEqual(result, deletedDashboard);
        });
    });
});
