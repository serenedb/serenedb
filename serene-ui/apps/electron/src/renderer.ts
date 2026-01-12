import { createORPCClient } from "@orpc/client";

const { port1: clientPort, port2: serverPort } = new MessageChannel();

window.postMessage("start-orpc-client", "*", [serverPort]);

const messageChannelLink = async (context: {
    path: string[];
    input: unknown;
}) => {
    return new Promise((resolve, reject) => {
        const requestId = Math.random().toString(36).substring(7);

        const messageHandler = (event: MessageEvent) => {
            const { id, result, error } = event.data;

            if (id === requestId) {
                clientPort.removeEventListener("message", messageHandler);

                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            }
        };

        clientPort.addEventListener("message", messageHandler);

        clientPort.postMessage({
            id: requestId,
            path: context.path,
            input: context.input,
        });
    });
};

clientPort.start();

export const apiClient = createORPCClient(messageChannelLink);
