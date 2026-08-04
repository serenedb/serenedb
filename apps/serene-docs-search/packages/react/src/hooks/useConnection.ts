import { useCallback, useEffect, useMemo, useState } from "react";
import { SereneSearchClient, type HealthResponse } from "@serenedb/docs-search-core";
import type { SearchStorage } from "../lib/storage";

export type ConnectionStatus = "unconfigured" | "connecting" | "online" | "offline";

export interface ConnectionOptions {
    backendUrl?: string;
    token?: string;
}

/** Backend connection: prop-pinned or restored from storage, switchable at runtime. */
export function useConnection(options: ConnectionOptions, storage: SearchStorage) {
    const [conn, setConn] = useState<{ backendUrl: string; token?: string } | null>(() => {
        if (options.backendUrl) return { backendUrl: options.backendUrl, token: options.token };
        return typeof window !== "undefined" ? storage.getConnection() : null;
    });
    useEffect(() => {
        if (options.backendUrl) {
            setConn({ backendUrl: options.backendUrl, token: options.token });
        }
    }, [options.backendUrl, options.token]);

    const client = useMemo(
        () => (conn ? new SereneSearchClient(conn) : null),
        [conn?.backendUrl, conn?.token],
    );

    const [status, setStatus] = useState<ConnectionStatus>(conn ? "connecting" : "unconfigured");
    const [health, setHealth] = useState<HealthResponse | null>(null);

    const refreshHealth = useCallback(async () => {
        if (!client) {
            setStatus("unconfigured");
            setHealth(null);
            return;
        }
        try {
            const h = await client.health();
            setHealth(h);
            setStatus(h.serenedb.connected ? "online" : "offline");
        } catch {
            setHealth(null);
            setStatus("offline");
        }
    }, [client]);

    const connect = useCallback(
        (backendUrl: string, token?: string) => {
            storage.saveConnection({ backendUrl, token });
            setConn({ backendUrl, token });
            setStatus("connecting");
        },
        [storage],
    );

    const disconnect = useCallback(() => {
        storage.clearConnection();
        if (!options.backendUrl) setConn(null);
    }, [storage, options.backendUrl]);

    return {
        backendUrl: conn?.backendUrl ?? null,
        client,
        status,
        setStatus,
        health,
        refreshHealth,
        connect,
        disconnect,
    };
}
