import { useSyncExternalStore } from "react";

export type SchemaMetadataInvalidationScope =
    | {
          level: "connection";
          connectionId?: number | null;
      }
    | {
          level: "database";
          connectionId?: number | null;
          database?: string | null;
      };

const listeners = new Set<() => void>();
const versions = new Map<string, number>();

const normalizeConnectionId = (connectionId?: number | null) =>
    typeof connectionId === "number" && connectionId > 0
        ? connectionId
        : undefined;

const normalizeDatabase = (database?: string | null) => {
    const normalized = database?.trim();
    return normalized ? normalized : undefined;
};

const getScopeKey = (scope: SchemaMetadataInvalidationScope) => {
    const connectionId = normalizeConnectionId(scope.connectionId);
    if (!connectionId) {
        return undefined;
    }

    if (scope.level === "connection") {
        return `connection:${connectionId}`;
    }

    const database = normalizeDatabase(scope.database);
    if (!database) {
        return undefined;
    }

    return `database:${connectionId}:${database}`;
};

const subscribe = (listener: () => void) => {
    listeners.add(listener);

    return () => {
        listeners.delete(listener);
    };
};

const getVersion = (scope: SchemaMetadataInvalidationScope) => {
    const key = getScopeKey(scope);
    if (!key) {
        return 0;
    }

    return versions.get(key) ?? 0;
};

export const invalidateSchemaMetadata = (
    scope: SchemaMetadataInvalidationScope,
) => {
    const key = getScopeKey(scope);
    if (!key) {
        return;
    }

    versions.set(key, (versions.get(key) ?? 0) + 1);
    listeners.forEach((listener) => listener());
};

export const useSchemaMetadataVersion = (
    scope: SchemaMetadataInvalidationScope,
) =>
    useSyncExternalStore(
        subscribe,
        () => getVersion(scope),
        () => 0,
    );
