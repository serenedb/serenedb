import { ORPCError } from "@orpc/server";
import { ConnectionRepository } from "../../repositories";
import type {
    AddConnectionInput,
    AddConnectionOutput,
    DeleteConnectionInput,
    DeleteConnectionOutput,
    ListMyConnectionOutput,
    UpdateConnectionInput,
    UpdateConnectionOutput,
} from "@serene-ui/shared-core";

export const ConnectionService = {
    listMyConnections: async (): Promise<ListMyConnectionOutput> => {
        try {
            const connections = ConnectionRepository.findMany();

            return connections;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }
            const message =
                error instanceof Error ? error.message : String(error);
            console.error("Error listing connections:", message);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to list connections:" + (message || ""),
            });
        }
    },
    addConnection: async (
        input: AddConnectionInput,
    ): Promise<AddConnectionOutput> => {
        try {
            if (input.mode === "host") {
                if (!input.host) {
                    throw new ORPCError("BAD_REQUEST", {
                        message: "Host in host mode is required",
                    });
                }
            }

            if (input.mode === "socket") {
                if (!input.socket) {
                    throw new ORPCError("BAD_REQUEST", {
                        message: "Socket in socket mode is required",
                    });
                }
            }
            const newConnection = ConnectionRepository.create({
                ...input,
                port: input.port ?? 5432,
            });

            if (!newConnection) {
                throw new ORPCError("INTERNAL_SERVER_ERROR", {
                    message: "Failed to create new connection",
                });
            }

            return newConnection;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }
            const message =
                error instanceof Error ? error.message : String(error);
            console.error("Error adding connection:", message);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to add connection: " + (message || ""),
            });
        }
    },
    updateConnection: async (
        input: UpdateConnectionInput,
    ): Promise<UpdateConnectionOutput> => {
        try {
            const updatedConnection = ConnectionRepository.update(
                input.id,
                input,
            );

            if (!updatedConnection) {
                throw new ORPCError("INTERNAL_SERVER_ERROR", {
                    message: `Failed to update connection`,
                });
            }
            return updatedConnection;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }
            const message =
                error instanceof Error ? error.message : String(error);
            console.error("Error updating connection:", message);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to update connection: " + (message || ""),
            });
        }
    },
    deleteConnection: async (
        input: DeleteConnectionInput,
    ): Promise<DeleteConnectionOutput> => {
        try {
            const deletedConnection = ConnectionRepository.delete(input.id);

            if (!deletedConnection) {
                throw new ORPCError("INTERNAL_SERVER_ERROR", {
                    message: `Failed to delete connection`,
                });
            }
            return deletedConnection;
        } catch (error) {
            if (error instanceof ORPCError) {
                throw error;
            }
            const message =
                error instanceof Error ? error.message : String(error);
            console.error("Error deleting connection:", message);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to delete connection: " + (message || ""),
            });
        }
    },
};
