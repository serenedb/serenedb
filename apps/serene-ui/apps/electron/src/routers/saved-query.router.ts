import { implement } from "@orpc/server";
import { apiContracts } from "@serene-ui/shared-core";
import { SavedQueryService } from "@serene-ui/shared-backend";

const os = implement(apiContracts.savedQuery);

export const addSavedQuery = os.add.handler(async ({ input }) => {
    return await SavedQueryService.addSavedQuery(input);
});

export const listMySavedQuerys = os.listMy.handler(async () => {
    return await SavedQueryService.listMySavedQueries();
});

export const updateSavedQuery = os.update.handler(async ({ input }) => {
    return await SavedQueryService.updateSavedQuery(input);
});

export const deleteSavedQuery = os.delete.handler(async ({ input }) => {
    return await SavedQueryService.deleteSavedQuery(input);
});

export const SavedQueryRouter = os.router({
    add: addSavedQuery,
    listMy: listMySavedQuerys,
    update: updateSavedQuery,
    delete: deleteSavedQuery,
});
