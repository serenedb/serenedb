import { ConnectionSchema } from "@serene-ui/shared-core";
import { ConnectionRepository } from "../../repositories/index.js";
import { logger } from "../../utils/logger.js";

const DEFAULT_CONNECTION_STRING_ENV = "DEFAULT_CONNECTION_STRING";
const DEFAULT_CONNECTION_NAME_ENV = "DEFAULT_CONNECTION_NAME";

const TRUE_VALUES = new Set(["1", "true", "yes", "on"]);
const FALSE_VALUES = new Set(["0", "false", "no", "off"]);
const SSL_ENABLED_MODES = new Set(["require", "verify-ca", "verify-full"]);
const SSL_DISABLED_MODES = new Set(["disable", "allow", "prefer"]);
type HostConnection = Omit<Extract<ConnectionSchema, { mode: "host" }>, "id">;
type SocketConnection = Omit<
    Extract<ConnectionSchema, { mode: "socket" }>,
    "id"
>;
type DefaultConnection = HostConnection | SocketConnection;

export const getDefaultConnectionName = (): string | undefined => {
    const connectionName = process.env[DEFAULT_CONNECTION_NAME_ENV]?.trim();

    return connectionName || undefined;
};

const decodeUriComponentSafe = (value: string): string => {
    try {
        return decodeURIComponent(value);
    } catch {
        return value;
    }
};

const parseBoolean = (value: string | null): boolean | undefined => {
    if (!value) {
        return undefined;
    }

    const normalizedValue = value.trim().toLowerCase();

    if (!normalizedValue) {
        return undefined;
    }

    if (TRUE_VALUES.has(normalizedValue)) {
        return true;
    }

    if (FALSE_VALUES.has(normalizedValue)) {
        return false;
    }

    return undefined;
};

const parseSslFlag = (url: URL): boolean => {
    const sslRaw = url.searchParams.get("ssl");
    const explicitSsl = parseBoolean(sslRaw);

    if (typeof explicitSsl === "boolean") {
        return explicitSsl;
    }

    const sslMode = url.searchParams.get("sslmode")?.trim().toLowerCase();

    if (!sslMode) {
        return false;
    }

    if (SSL_DISABLED_MODES.has(sslMode)) {
        return false;
    }

    if (SSL_ENABLED_MODES.has(sslMode)) {
        return true;
    }

    return false;
};

const parsePort = (url: URL): number | null => {
    const rawPort = url.port || url.searchParams.get("port")?.trim() || "";

    if (!rawPort) {
        return 5432;
    }

    const port = Number(rawPort);
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
        return null;
    }

    return port;
};

const parseDefaultConnection = (
    name: string,
    connectionString: string,
): DefaultConnection | null => {
    let url: URL;
    try {
        url = new URL(connectionString);
    } catch {
        logger.warn(
            "Skipped default connection preload: DEFAULT_CONNECTION_STRING is not a valid URL",
        );
        return null;
    }

    if (!["postgres:", "postgresql:"].includes(url.protocol)) {
        logger.warn(
            `Skipped default connection preload: unsupported protocol "${url.protocol}"`,
        );
        return null;
    }

    const queryHostRaw = url.searchParams.get("host");
    const queryHost = queryHostRaw
        ? decodeUriComponentSafe(queryHostRaw).trim()
        : "";
    const hostName = decodeUriComponentSafe(url.hostname).trim();
    const socket = queryHost.startsWith("/")
        ? queryHost
        : hostName.startsWith("/")
          ? hostName
          : undefined;
    const host =
        queryHost && !queryHost.startsWith("/")
            ? queryHost
            : hostName || "localhost";

    const port = parsePort(url);
    if (!port) {
        logger.warn(
            "Skipped default connection preload: invalid port in DEFAULT_CONNECTION_STRING",
        );
        return null;
    }

    const databasePath = url.pathname.replace(/^\/+/, "").trim();
    const database = databasePath
        ? decodeUriComponentSafe(databasePath)
        : undefined;
    const user = url.username
        ? decodeUriComponentSafe(url.username).trim()
        : undefined;
    const password = url.password
        ? decodeUriComponentSafe(url.password)
        : undefined;
    const ssl = parseSslFlag(url);

    const baseConnection = {
        name,
        type: "postgres" as const,
        ssl,
        authMethod: "password" as const,
        user,
        password,
        port,
        database,
    };

    if (socket) {
        return {
            ...baseConnection,
            mode: "socket",
            socket,
        } as SocketConnection;
    }

    return {
        ...baseConnection,
        mode: "host",
        host,
    } as HostConnection;
};

export const loadDefaultConnection = (): void => {
    const connectionName = getDefaultConnectionName();
    const connectionString = process.env[DEFAULT_CONNECTION_STRING_ENV]?.trim();

    if (!connectionName && !connectionString) {
        return;
    }

    if (!connectionName || !connectionString) {
        logger.warn(
            `Skipped default connection preload: both ${DEFAULT_CONNECTION_NAME_ENV} and ${DEFAULT_CONNECTION_STRING_ENV} are required`,
        );
        return;
    }

    if (connectionName.length > 255) {
        logger.warn(
            "Skipped default connection preload: DEFAULT_CONNECTION_NAME is too long (max 255 chars)",
        );
        return;
    }

    try {
        const existingConnection = ConnectionRepository.findOne({
            name: connectionName,
        });

        if (existingConnection) {
            logger.info(
                `Skipped default connection preload: connection "${connectionName}" already exists`,
            );
            return;
        }

        const parsedConnection = parseDefaultConnection(
            connectionName,
            connectionString,
        );
        if (!parsedConnection) {
            return;
        }

        const createdConnection = ConnectionRepository.create(parsedConnection);
        if (!createdConnection) {
            logger.warn(
                `Default connection preload failed: could not create connection "${connectionName}"`,
            );
            return;
        }

        logger.info(
            `Default connection preload complete: connection "${connectionName}" created`,
        );
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logger.warn(
            `Default connection preload failed: unable to create "${connectionName}" (${message})`,
        );
    }
};
