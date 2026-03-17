import type {
    AddDashboardInput,
    DashboardBaseSchema,
    DashboardSchema,
    UpdateDashboardInput,
} from "@serene-ui/shared-core";
import {
    buildDelete,
    buildInsert,
    buildSelect,
    buildUpdate,
} from "../../utils/request-builder.js";
import { DBClient } from "../../database/db-init.js";
import { dashboardToRow, rowToDashboard } from "./dashboard.mappers.js";
import { DashboardBlockRepository } from "../dashboard-block/dashboard-block.repository.js";
import type { DashboardRow } from "./dashboard.types.js";

type DashboardCreatePayload = AddDashboardInput;
type DashboardUpdatePayload = Omit<UpdateDashboardInput, "id">;

export const DashboardRepository = {
    findById: (id: number): DashboardSchema | null => {
        const { sql, values } = buildSelect("dashboards", {
            where: { id },
        });
        const row = DBClient.prepare(sql).get(...values);

        if (!row) {
            return null;
        }

        const blocks = DashboardBlockRepository.findManyByDashboardId(id);

        return rowToDashboard(row as DashboardRow, blocks);
    },

    findOne: (query?: Partial<DashboardBaseSchema>): DashboardSchema | null => {
        const { sql, values } = buildSelect("dashboards", {
            where: query,
            orderBy: "updated_at DESC, id DESC",
            limit: 1,
        });
        const row = DBClient.prepare(sql).get(...values);

        if (!row) {
            return null;
        }

        const dashboardRow = row as DashboardRow;
        const blocks = DashboardBlockRepository.findManyByDashboardId(
            dashboardRow.id ?? 0,
        );

        return rowToDashboard(dashboardRow, blocks);
    },

    findMany: (query?: Partial<DashboardBaseSchema>): DashboardSchema[] => {
        const { sql, values } = buildSelect("dashboards", {
            where: query,
            orderBy: "updated_at DESC, id DESC",
        });
        const rows = DBClient.prepare(sql).all(...values);
        const dashboardRows = rows as DashboardRow[];
        const dashboardIds = dashboardRows
            .map((row) => row.id)
            .filter((id): id is number => typeof id === "number");
        const blocksByDashboardId =
            DashboardBlockRepository.findManyGroupedByDashboardIds(
                dashboardIds,
            );

        return dashboardRows.map((dashboardRow) => {
            const dashboardId = dashboardRow.id ?? 0;
            const blocks = blocksByDashboardId.get(dashboardId) ?? [];

            return rowToDashboard(dashboardRow, blocks);
        });
    },

    create: (dashboard: DashboardCreatePayload): DashboardSchema | null => {
        const createTx = DBClient.transaction(
            (payload: DashboardCreatePayload) => {
                const { blocks, ...dashboardBase } = payload;
                const dashboardRow = dashboardToRow(dashboardBase);
                const { sql, values } = buildInsert("dashboards", dashboardRow);
                const result = DBClient.prepare(sql).run(...values);
                const dashboardId = result.lastInsertRowid as number;

                DashboardBlockRepository.createMany(
                    blocks.map((block) => ({
                        ...block,
                        dashboard_id: dashboardId,
                    })),
                );

                return dashboardId;
            },
        );

        const dashboardId = createTx(dashboard);

        return DashboardRepository.findById(dashboardId);
    },

    update: (
        id: number,
        updates: DashboardUpdatePayload,
    ): DashboardSchema | null => {
        const currentDashboard = DashboardRepository.findById(id);

        if (!currentDashboard) {
            return null;
        }

        const updateTx = DBClient.transaction(
            (dashboardId: number, payload: DashboardUpdatePayload) => {
                const { blocks, ...dashboardBase } = payload;
                const entries = Object.entries(dashboardBase).filter(
                    ([key, value]) => key !== "blocks" && value !== undefined,
                );

                if (entries.length || blocks !== undefined) {
                    const processedUpdates = Object.fromEntries(
                        entries.map(([key, value]) => [
                            key,
                            typeof value === "boolean"
                                ? value
                                    ? 1
                                    : 0
                                : value,
                        ]),
                    );
                    const { sql, values } = buildUpdate(
                        "dashboards",
                        {
                            ...processedUpdates,
                            updated_at: new Date().toISOString(),
                        },
                        {
                            where: { id: dashboardId },
                        },
                    );

                    DBClient.prepare(sql).run(...values);
                }

                if (blocks !== undefined) {
                    DashboardBlockRepository.replaceForDashboard(
                        dashboardId,
                        blocks.map((block) => ({
                            ...block,
                            dashboard_id: dashboardId,
                        })),
                    );
                }
            },
        );

        updateTx(id, updates);

        return DashboardRepository.findById(id);
    },

    delete: (id: number): DashboardSchema | null => {
        const dashboard = DashboardRepository.findById(id);

        if (!dashboard) {
            return null;
        }

        const deleteTx = DBClient.transaction((dashboardId: number) => {
            DashboardBlockRepository.deleteByDashboardId(dashboardId);

            const { sql, values } = buildDelete("dashboards", {
                where: { id: dashboardId },
            });
            DBClient.prepare(sql).run(...values);
        });

        deleteTx(id);

        return dashboard;
    },
};
