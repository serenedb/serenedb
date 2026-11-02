import { ORPCError } from "@orpc/server";
import {
    AddSavedQueryInput,
    AddSavedQueryOutput,
    DeleteSavedQueryInput,
    DeleteSavedQueryOutput,
    ListMySavedQueriesOutput,
    UpdateSavedQueryInput,
    UpdateSavedQueryOutput,
} from "@serene-ui/shared-core";
import { SavedQueryRepository } from "../../repositories";

export const SavedQueryService = {
    listMySavedQueries: async (): Promise<ListMySavedQueriesOutput> => {
        try {
            const savedQueries = SavedQueryRepository.findMany();

            return savedQueries;
        } catch (error) {
            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Failed to fetch saved queries:" + message,
            });
        }
    },
    addSavedQuery: async (
        input: AddSavedQueryInput,
    ): Promise<AddSavedQueryOutput> => {
        try {
            const newSavedQuery = SavedQueryRepository.create({
                ...input,
                bind_vars: input.bind_vars || [],
            });

            return newSavedQuery;
        } catch (error) {
            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Saved query was not created:" + message,
            });
        }
    },
    updateSavedQuery: async (
        input: UpdateSavedQueryInput,
    ): Promise<UpdateSavedQueryOutput> => {
        try {
            const updatedSavedQuery = SavedQueryRepository.update(
                input.id,
                input,
            );

            if (!updatedSavedQuery) {
                throw new ORPCError("INTERNAL_SERVER_ERROR", {
                    message: "Saved query was not updated",
                });
            }
            return updatedSavedQuery;
        } catch (error) {
            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Saved query was not updated:" + message,
            });
        }
    },
    deleteSavedQuery: async (
        input: DeleteSavedQueryInput,
    ): Promise<DeleteSavedQueryOutput> => {
        try {
            const deletedSavedQuery = SavedQueryRepository.delete(input.id);

            if (!deletedSavedQuery) {
                throw new ORPCError("INTERNAL_SERVER_ERROR", {
                    message: "Saved query was not deleted",
                });
            }
            return deletedSavedQuery;
        } catch (error) {
            const message =
                error instanceof Error ? error.message : String(error);
            throw new ORPCError("INTERNAL_SERVER_ERROR", {
                message: "Saved query was not deleted:" + message,
            });
        }
    },
};
