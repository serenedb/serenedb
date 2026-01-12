import { implement } from "@orpc/server";
import { apiContracts } from "@serene-ui/shared-core";
import { ConnectionService } from "@serene-ui/shared-backend";

const os = implement(apiContracts.connection);

export const addConnection = os.add.handler(async ({ input }) => {
    return await ConnectionService.addConnection(input);
});

export const listMyConnections = os.listMy.handler(async () => {
    return await ConnectionService.listMyConnections();
});

export const updateConnection = os.update.handler(async ({ input }) => {
    return await ConnectionService.updateConnection(input);
});

export const deleteConnection = os.delete.handler(async ({ input }) => {
    return await ConnectionService.deleteConnection(input);
});

export const ConnectionRouter = os.router({
    add: addConnection,
    listMy: listMyConnections,
    update: updateConnection,
    delete: deleteConnection,
});
