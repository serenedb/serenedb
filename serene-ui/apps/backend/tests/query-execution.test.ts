import { call, implement } from "@orpc/server";
import assert from "node:assert";
import { describe, it } from "node:test";
import { apiContracts } from "@serene-ui/shared-core";

const os = implement(apiContracts.queryExecution);

describe("QueryExecutionRouter", () => {
    describe("execute", () => {
        it("should execute a query with connection ID", async () => {
            const mockResult = {
                jobId: 1,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: 1,
                query: "SELECT * FROM users",
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should execute a query with bind variables", async () => {
            const mockResult = {
                jobId: 2,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: 1,
                query: "SELECT * FROM users WHERE id = :user_id",
                bind_vars: ["123"],
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should execute an async query", async () => {
            const mockResult = {
                jobId: 3,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: 1,
                query: "SELECT * FROM large_table",
                async: true,
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should execute a query with limit", async () => {
            const mockResult = {
                jobId: 4,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: 1,
                query: "SELECT * FROM users",
                limit: 100,
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should execute query with connection details (host mode)", async () => {
            const mockResult = {
                jobId: 5,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: undefined,
                query: "SELECT * FROM users",
                type: "postgres",
                ssl: false,
                mode: "host",
                host: "localhost",
                port: 5432,
                database: "testdb",
                authMethod: "password",
                user: "testuser",
                password: "testpass",
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should execute query with connection details (socket mode)", async () => {
            const mockResult = {
                jobId: 6,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: undefined,
                query: "SELECT * FROM users",
                type: "postgres",
                ssl: false,
                mode: "socket",
                socket: "/var/run/postgresql",
                port: 5432,
                database: "testdb",
                authMethod: "password",
                user: "testuser",
                password: "testpass",
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should return immediate result for fast queries", async () => {
            const mockResult = {
                jobId: 7,
                result: [
                    { id: 1, name: "Alice" },
                    { id: 2, name: "Bob" },
                ],
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: 1,
                query: "SELECT id, name FROM users LIMIT 2",
            });

            assert.deepStrictEqual(result, mockResult);
        });

        it("should execute query with custom database override", async () => {
            const mockResult = {
                jobId: 8,
            };

            const fakeExecute = os.execute.handler(async () => mockResult);

            const result = await call(fakeExecute, {
                connectionId: 1,
                query: "SELECT * FROM users",
                database: "custom_db",
            });

            assert.deepStrictEqual(result, mockResult);
        });
    });

    describe("subscribe", () => {
        it("should subscribe to query execution with pending status", async () => {
            const mockInitialResult = {
                id: 1,
                query: "SELECT * FROM large_table",
                status: "pending" as const,
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockInitialResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.deepStrictEqual(results[0], mockInitialResult);
        });

        it("should receive status updates from pending to success", async () => {
            const mockResults = [
                {
                    id: 1,
                    query: "SELECT * FROM users",
                    status: "pending" as const,
                },
                {
                    id: 1,
                    query: "SELECT * FROM users",
                    status: "running" as const,
                },
                {
                    id: 1,
                    query: "SELECT * FROM users",
                    status: "success" as const,
                    action_type: "SELECT" as const,
                    result: [
                        { id: 1, name: "Alice" },
                        { id: 2, name: "Bob" },
                    ],
                },
            ];

            const fakeSubscribe = os.subscribe.handler(async function* () {
                for (const result of mockResults) {
                    yield result;
                }
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 3);
            assert.strictEqual(results[0].status, "pending");
            assert.strictEqual(results[1].status, "running");
            assert.strictEqual(results[2].status, "success");
            assert.ok("result" in results[2]);
        });

        it("should handle immediate success", async () => {
            const mockResult = {
                id: 1,
                query: "SELECT COUNT(*) FROM users",
                status: "success" as const,
                action_type: "SELECT" as const,
                result: [{ count: 42 }],
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].status, "success");
            assert.deepStrictEqual(results[0].result, [{ count: 42 }]);
        });

        it("should handle query failure", async () => {
            const mockResults = [
                {
                    id: 1,
                    query: "SELCT * FROM users",
                    status: "pending" as const,
                },
                {
                    id: 1,
                    query: "SELCT * FROM users",
                    status: "running" as const,
                },
                {
                    id: 1,
                    query: "SELCT * FROM users",
                    status: "failed" as const,
                    error: 'Syntax error: unexpected token "SELCT"',
                },
            ];

            const fakeSubscribe = os.subscribe.handler(async function* () {
                for (const result of mockResults) {
                    yield result;
                }
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 3);
            assert.strictEqual(results[2].status, "failed");
            assert.ok("error" in results[2]);
        });

        it("should handle INSERT action type", async () => {
            const mockResult = {
                id: 1,
                query: "INSERT INTO users (name) VALUES ('Charlie')",
                status: "success" as const,
                action_type: "INSERT" as const,
                result: [],
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].status, "success");
            if (results[0].status === "success") {
                assert.strictEqual(results[0].action_type, "INSERT");
            }
        });

        it("should handle UPDATE action type", async () => {
            const mockResult = {
                id: 1,
                query: "UPDATE users SET active = true WHERE id = 1",
                status: "success" as const,
                action_type: "UPDATE" as const,
                result: [],
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].status, "success");
            if (results[0].status === "success") {
                assert.strictEqual(results[0].action_type, "UPDATE");
            }
        });

        it("should handle DELETE action type", async () => {
            const mockResult = {
                id: 1,
                query: "DELETE FROM users WHERE id = 99",
                status: "success" as const,
                action_type: "DELETE" as const,
                result: [],
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.strictEqual(results[0].status, "success");
            if (results[0].status === "success") {
                assert.strictEqual(results[0].action_type, "DELETE");
            }
        });

        it("should include timestamps in results", async () => {
            const now = new Date().toISOString();
            const mockResult = {
                id: 1,
                query: "SELECT * FROM users",
                status: "success" as const,
                action_type: "SELECT" as const,
                result: [],
                created_at: now,
                execution_started_at: now,
                execution_finished_at: now,
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.ok("created_at" in results[0]);
            assert.ok("execution_started_at" in results[0]);
            assert.ok("execution_finished_at" in results[0]);
        });

        it("should handle queries with bind_vars", async () => {
            const mockResult = {
                id: 1,
                query: "SELECT * FROM users WHERE email = :email",
                bind_vars: ["user@example.com"],
                status: "success" as const,
                action_type: "SELECT" as const,
                result: [{ id: 1, email: "user@example.com" }],
            };

            const fakeSubscribe = os.subscribe.handler(async function* () {
                yield mockResult;
            });

            const results = [];
            for await (const result of await call(fakeSubscribe, {
                jobId: 1,
            })) {
                results.push(result);
            }

            assert.strictEqual(results.length, 1);
            assert.ok("bind_vars" in results[0]);
            assert.deepStrictEqual(results[0].bind_vars, ["user@example.com"]);
        });
    });
});
