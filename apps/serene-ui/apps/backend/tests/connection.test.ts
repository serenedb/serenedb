import { call, implement } from "@orpc/server";
import assert from "node:assert";
import { describe, it } from "node:test";
import { apiContracts } from "@serene-ui/shared-core";

const os = implement(apiContracts.connection);

describe("ConnectionRouter", () => {
    describe("add connection", () => {
        it("should add a PostgreSQL connection with host mode", async () => {
            const mockConnection = {
                id: 1,
                name: "Test PostgreSQL DB",
                type: "postgres" as const,
                ssl: false,
                database: "testdb",
                mode: "host" as const,
                host: "localhost",
                port: 5432,
                authMethod: "password" as const,
                user: "testuser",
                password: "testpass",
            };

            const fakeAddConnection = os.add.handler(
                async () => mockConnection,
            );

            const result = await call(fakeAddConnection, {
                name: "Test PostgreSQL DB",
                type: "postgres",
                ssl: false,
                database: "testdb",
                mode: "host",
                host: "localhost",
                port: 5432,
                authMethod: "password",
                user: "testuser",
                password: "testpass",
            });

            assert.deepStrictEqual(result, mockConnection);
        });

        it("should add a PostgreSQL connection with socket mode", async () => {
            const mockConnection = {
                id: 2,
                name: "Socket DB",
                type: "postgres" as const,
                ssl: false,
                database: "socketdb",
                mode: "socket" as const,
                socket: "/var/run/postgresql",
                port: 5432,
                authMethod: "password" as const,
                user: "socketuser",
                password: "socketpass",
            };

            const fakeAddConnection = os.add.handler(
                async () => mockConnection,
            );

            const result = await call(fakeAddConnection, {
                name: "Socket DB",
                type: "postgres",
                ssl: false,
                database: "socketdb",
                mode: "socket",
                socket: "/var/run/postgresql",
                port: 5432,
                authMethod: "password",
                user: "socketuser",
                password: "socketpass",
            });

            assert.deepStrictEqual(result, mockConnection);
        });

        it("should add a connection with SSL enabled", async () => {
            const mockConnection = {
                id: 3,
                name: "Secure DB",
                type: "postgres" as const,
                ssl: true,
                database: "securedb",
                mode: "host" as const,
                host: "secure.example.com",
                port: 5432,
                authMethod: "password" as const,
                user: "secureuser",
                password: "securepass",
            };

            const fakeAddConnection = os.add.handler(
                async () => mockConnection,
            );

            const result = await call(fakeAddConnection, {
                name: "Secure DB",
                type: "postgres",
                ssl: true,
                database: "securedb",
                mode: "host",
                host: "secure.example.com",
                port: 5432,
                authMethod: "password",
                user: "secureuser",
                password: "securepass",
            });

            assert.deepStrictEqual(result, mockConnection);
        });
    });

    describe("list connections", () => {
        it("should list all connections", async () => {
            const mockConnections = [
                {
                    id: 1,
                    name: "PostgreSQL DB",
                    type: "postgres" as const,
                    ssl: false,
                    database: "testdb",
                    mode: "host" as const,
                    host: "localhost",
                    port: 5432,
                    authMethod: "password" as const,
                    user: "testuser",
                    password: null,
                },
                {
                    id: 2,
                    name: "Socket DB",
                    type: "postgres" as const,
                    ssl: false,
                    database: "socketdb",
                    mode: "socket" as const,
                    socket: "/var/run/postgresql",
                    port: 5432,
                    authMethod: "password" as const,
                    user: "socketuser",
                    password: null,
                },
            ];

            const fakeListConnections = os.listMy.handler(
                async () => mockConnections,
            );

            const result = await call(fakeListConnections, undefined);

            assert.deepStrictEqual(result, mockConnections);
        });

        it("should return empty array when no connections exist", async () => {
            const fakeListConnections = os.listMy.handler(async () => []);

            const result = await call(fakeListConnections, undefined);

            assert.deepStrictEqual(result, []);
        });
    });

    describe("update connection", () => {
        it("should update connection name", async () => {
            const mockUpdatedConnection = {
                id: 1,
                name: "Updated DB Name",
                type: "postgres" as const,
                ssl: false,
                database: "testdb",
                mode: "host" as const,
                host: "localhost",
                port: 5432,
                authMethod: "password" as const,
                user: "testuser",
                password: null,
            };

            const fakeUpdateConnection = os.update.handler(
                async () => mockUpdatedConnection,
            );

            const result = await call(fakeUpdateConnection, {
                id: 1,
                name: "Updated DB Name",
            });

            assert.deepStrictEqual(result, mockUpdatedConnection);
        });

        it("should update connection host and port", async () => {
            const mockUpdatedConnection = {
                id: 1,
                name: "Test DB",
                type: "postgres" as const,
                ssl: false,
                database: "testdb",
                mode: "host" as const,
                host: "db.example.com",
                port: 5433,
                authMethod: "password" as const,
                user: "testuser",
                password: null,
            };

            const fakeUpdateConnection = os.update.handler(
                async () => mockUpdatedConnection,
            );

            const result = await call(fakeUpdateConnection, {
                id: 1,
                mode: "host",
                host: "db.example.com",
                port: 5433,
            });

            assert.deepStrictEqual(result, mockUpdatedConnection);
        });

        it("should update connection to use socket mode", async () => {
            const mockUpdatedConnection = {
                id: 1,
                name: "Test DB",
                type: "postgres" as const,
                ssl: false,
                database: "testdb",
                mode: "socket" as const,
                socket: "/tmp",
                port: 5432,
                authMethod: "password" as const,
                user: "testuser",
                password: null,
            };

            const fakeUpdateConnection = os.update.handler(
                async () => mockUpdatedConnection,
            );

            const result = await call(fakeUpdateConnection, {
                id: 1,
                mode: "socket",
                socket: "/tmp",
            });

            assert.deepStrictEqual(result, mockUpdatedConnection);
        });
    });

    describe("delete connection", () => {
        it("should delete a connection successfully", async () => {
            const mockDeletedConnection = {
                id: 1,
                name: "Deleted DB",
                type: "postgres" as const,
                ssl: false,
                mode: "host" as const,
                host: "localhost",
                port: 5432,
                authMethod: "password" as const,
                user: "testuser",
                password: null,
            };

            const fakeDeleteConnection = os.delete.handler(
                async () => mockDeletedConnection,
            );

            const result = await call(fakeDeleteConnection, { id: 1 });

            assert.deepStrictEqual(result, mockDeletedConnection);
        });
    });
});
