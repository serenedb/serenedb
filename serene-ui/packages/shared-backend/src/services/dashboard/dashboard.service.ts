import { ORPCError } from "@orpc/server";
import type {
    AddDashboardInput,
    AddDashboardOutput,
    DeleteDashboardInput,
    DeleteDashboardOutput,
    GetDashboardInput,
    GetDashboardOutput,
    ListMyDashboardsOutput,
    UpdateDashboardInput,
    UpdateDashboardOutput,
} from "@serene-ui/shared-core";
import { DashboardRepository } from "../../repositories";

export const DashboardService = {
    listMyDashboards: async (): Promise<ListMyDashboardsOutput> => {
        try {
            return DashboardRepository.findMany();
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }

            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to list dashboards: " + (message || ""),
            });
        }
    },

    getDashboard: async (
        input: GetDashboardInput,
    ): Promise<GetDashboardOutput> => {
        try {
            const dashboard = DashboardRepository.findById(input.id);

            if (!dashboard) {
                throw new ORPCError("BAD_REQUEST", {
                    message: "Dashboard not found",
                });
            }

            return dashboard;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }

            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to fetch dashboard: " + (message || ""),
            });
        }
    },

    addDashboard: async (
        input: AddDashboardInput,
    ): Promise<AddDashboardOutput> => {
        try {
            const newDashboard = DashboardRepository.create({
                ...input,
                blocks: input.blocks ?? [],
            });

            if (!newDashboard) {
                throw new ORPCError("INTERNAL_SERVER_ERROR", {
                    message: "Dashboard was not created",
                });
            }

            return newDashboard;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }

            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Dashboard was not created: " + (message || ""),
            });
        }
    },

    updateDashboard: async (
        input: UpdateDashboardInput,
    ): Promise<UpdateDashboardOutput> => {
        try {
            const { id, ...updates } = input;
            const updatedDashboard = DashboardRepository.update(id, updates);

            if (!updatedDashboard) {
                throw new ORPCError("BAD_REQUEST", {
                    message: "Dashboard not found",
                });
            }

            return updatedDashboard;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }

            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Dashboard was not updated: " + (message || ""),
            });
        }
    },

    deleteDashboard: async (
        input: DeleteDashboardInput,
    ): Promise<DeleteDashboardOutput> => {
        try {
            const deletedDashboard = DashboardRepository.delete(input.id);

            if (!deletedDashboard) {
                throw new ORPCError("BAD_REQUEST", {
                    message: "Dashboard not found",
                });
            }

            return deletedDashboard;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }

            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Dashboard was not deleted: " + (message || ""),
            });
        }
    },
};
