import { createORPCClient } from "@orpc/client";
import { RPCLink } from "@orpc/client/fetch";
import { RPCLink as MessagePortLink } from "@orpc/client/message-port";
import { ContractRouterClient } from "@orpc/contract";
import { createTanstackQueryUtils } from "@orpc/tanstack-query";
import { apiContracts } from "@serene-ui/shared-core";

type Mode = "dev-docker" | "prod-docker" | "dev-electron" | "prod-electron";

function createLink() {
    const mode = import.meta.env.MODE as Mode;

    if (mode && mode.includes("electron")) {
        const { port1, port2 } = new MessageChannel();

        window.postMessage("start-orpc-client", "*", [port2]);

        port1.start();

        return new MessagePortLink({
            port: port1,
        });
    }

    return new RPCLink({
        url: "http://0.0.0.0:3000",
    });
}

const link = createLink();

export const apiClient: ContractRouterClient<typeof apiContracts> =
    createORPCClient(link);

export const orpc = createTanstackQueryUtils(apiClient);
