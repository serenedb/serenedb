import { useEffect, useState } from "react";
import { ConnectionContext, CurrentConnection } from "./ConnectionContext";
import { useGetConnections } from "../api";

export const ConnectionProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const { data: connections, isFetched } = useGetConnections();
    const [currentConnection, setCurrentConnectionInternal] =
        useState<CurrentConnection>(() => {
            try {
                const stored = localStorage.getItem("system:currentConnection");
                return stored
                    ? JSON.parse(stored)
                    : { connectionId: -1, database: "" };
            } catch {
                return { connectionId: -1, database: "" };
            }
        });

    const setCurrentConnection = setCurrentConnectionInternal;

    useEffect(() => {
        if (
            currentConnection.connectionId === -1 &&
            currentConnection.database !== ""
        ) {
            setCurrentConnection({
                connectionId: -1,
                database: "",
            });
        }
        try {
            localStorage.setItem(
                "system:currentConnection",
                JSON.stringify(currentConnection),
            );
        } catch {}
    }, [currentConnection]);

    useEffect(() => {
        if (isFetched && connections && currentConnection.connectionId !== -1) {
            const connectionExists = connections.some(
                (conn) => conn.id === currentConnection.connectionId,
            );
            if (!connectionExists) {
                setCurrentConnection({
                    connectionId: -1,
                    database: "",
                });
            }
        }
    }, [connections, isFetched, currentConnection.connectionId]);

    return (
        <ConnectionContext.Provider
            value={{
                currentConnection,
                setCurrentConnection,
            }}>
            {children}
        </ConnectionContext.Provider>
    );
};
