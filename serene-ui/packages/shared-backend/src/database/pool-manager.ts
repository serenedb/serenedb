import { Pool } from "pg";
import { ConnectionSchema } from "@serene-ui/shared-core";

class PoolManager {
    private pools = new Map<
        string,
        {
            pool: Pool;
            lastUsed: number;
        }
    >();
    private cleanupInterval: NodeJS.Timeout;

    constructor(
        private cleanupIntervalMs = 60000,
        private connectionLifetimeMs = 5 * 60000,
    ) {
        this.cleanupInterval = setInterval(() => {
            this.cleanUp();
        }, this.cleanupIntervalMs);
    }

    generateKey(connection: Partial<ConnectionSchema>) {
        return JSON.stringify(connection);
    }

    public getPool(connection: Partial<ConnectionSchema>) {
        const key = this.generateKey(connection);
        const poolObj = this.pools.get(key);
        if (poolObj) {
            poolObj.lastUsed = Date.now();
            return poolObj.pool;
        }
        const pool = new Pool({
            host:
                connection.mode === "host"
                    ? connection.host
                    : connection.mode === "socket"
                      ? connection.socket
                      : undefined,
            port: connection.port,
            user: connection.user || "",
            password: connection.password || "",
            database: connection.database,
            ssl: connection.ssl,
            connectionTimeoutMillis: 5000,
        });

        pool.on("error", (err) => {
            console.error("Unexpected error on idle database client", err);
            this.pools.delete(key);
        });

        this.pools.set(key, {
            pool,
            lastUsed: Date.now(),
        });
        return pool;
    }

    public async cleanUp() {
        const now = Date.now();
        for (const [key, entry] of this.pools.entries()) {
            if (now - entry.lastUsed > this.connectionLifetimeMs) {
                await entry.pool.end();
                this.pools.delete(key);
            }
        }
    }

    public destroy() {
        clearInterval(this.cleanupInterval);
        for (const entry of this.pools.values()) {
            entry.pool.end();
        }
    }
}

export const PoolManagerInstance = new PoolManager();
