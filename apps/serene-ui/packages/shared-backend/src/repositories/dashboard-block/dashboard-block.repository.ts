import { buildDelete, buildInsert, buildSelect } from "../../utils/request-builder.js";
import { DBClient } from "../../database/db-init.js";
import {
    dashboardBlockToRow,
    rowToDashboardBlock,
    type DashboardBlockWrite,
} from "./dashboard-block.mappers.js";
import type { DashboardBlockSchema } from "@serene-ui/shared-core";
import type { DashboardBlockRow } from "./dashboard-block.types.js";

const insertBlocks = (blocks: DashboardBlockWrite[]) => {
    for (const [index, block] of blocks.entries()) {
        const blockRow = dashboardBlockToRow(block, index);
        const { sql, values } = buildInsert("dashboard_blocks", blockRow, {
            ignoreId: blockRow.id == null,
        });

        DBClient.prepare(sql).run(...values);
    }
};

export const DashboardBlockRepository = {
    findManyByDashboardId: (dashboardId: number): DashboardBlockSchema[] => {
        const { sql, values } = buildSelect("dashboard_blocks", {
            where: { dashboard_id: dashboardId },
            orderBy: "position ASC, id ASC",
        });
        const rows = DBClient.prepare(sql).all(...values);

        return rows.map((row) => rowToDashboardBlock(row as DashboardBlockRow));
    },

    findManyGroupedByDashboardIds: (
        dashboardIds: number[],
    ): Map<number, DashboardBlockSchema[]> => {
        const groupedBlocks = new Map<number, DashboardBlockSchema[]>();

        if (!dashboardIds.length) {
            return groupedBlocks;
        }

        const placeholders = dashboardIds.map(() => "?").join(", ");
        const sql = `
            SELECT *
            FROM dashboard_blocks
            WHERE dashboard_id IN (${placeholders})
            ORDER BY dashboard_id ASC, position ASC, id ASC
        `;
        const rows = DBClient.prepare(sql).all(...dashboardIds);

        for (const row of rows) {
            const block = rowToDashboardBlock(row as DashboardBlockRow);
            const blocks = groupedBlocks.get(block.dashboard_id) ?? [];

            blocks.push(block);
            groupedBlocks.set(block.dashboard_id, blocks);
        }

        return groupedBlocks;
    },

    createMany: (blocks: DashboardBlockWrite[]): DashboardBlockSchema[] => {
        if (!blocks.length) {
            return [];
        }

        insertBlocks(blocks);

        return DashboardBlockRepository.findManyByDashboardId(
            blocks[0]!.dashboard_id,
        );
    },

    replaceForDashboard: (
        dashboardId: number,
        blocks: DashboardBlockWrite[],
    ): DashboardBlockSchema[] => {
        const { sql, values } = buildDelete("dashboard_blocks", {
            where: { dashboard_id: dashboardId },
        });

        DBClient.prepare(sql).run(...values);

        if (blocks.length) {
            insertBlocks(blocks);
        }

        return DashboardBlockRepository.findManyByDashboardId(dashboardId);
    },

    deleteByDashboardId: (dashboardId: number): void => {
        const { sql, values } = buildDelete("dashboard_blocks", {
            where: { dashboard_id: dashboardId },
        });

        DBClient.prepare(sql).run(...values);
    },
};
