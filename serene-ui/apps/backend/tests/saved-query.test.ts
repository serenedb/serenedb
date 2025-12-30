import { call, implement } from "@orpc/server";
import assert from "node:assert";
import { describe, it } from "node:test";
import { apiContracts } from "@serene-ui/shared-core";

const os = implement(apiContracts.savedQuery);

describe("SavedQueryRouter", () => {
    describe("add saved query", () => {
        it("should add a saved query successfully", async () => {
            const mockSavedQuery = {
                id: 1,
                name: "Get All Users",
                query: "SELECT * FROM users",
                usage_count: 0,
            };

            const fakeAddSavedQuery = os.add.handler(
                async () => mockSavedQuery,
            );

            const result = await call(fakeAddSavedQuery, {
                name: "Get All Users",
                query: "SELECT * FROM users",
            });

            assert.deepStrictEqual(result, mockSavedQuery);
        });

        it("should add a saved query with bind variables", async () => {
            const mockSavedQuery = {
                id: 2,
                name: "Find User By Email",
                query: "SELECT * FROM users WHERE email = :email",
                bind_vars: [
                    {
                        name: "email",
                        default_value: "user@example.com",
                        description: "User email address",
                    },
                ],
                usage_count: 0,
            };

            const fakeAddSavedQuery = os.add.handler(
                async () => mockSavedQuery,
            );

            const result = await call(fakeAddSavedQuery, {
                name: "Find User By Email",
                query: "SELECT * FROM users WHERE email = :email",
                bind_vars: [
                    {
                        name: "email",
                        default_value: "user@example.com",
                        description: "User email address",
                    },
                ],
            });

            assert.deepStrictEqual(result, mockSavedQuery);
        });

        it("should add a complex query with joins", async () => {
            const mockSavedQuery = {
                id: 3,
                name: "User Orders Summary",
                query: `SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
                    FROM users u
                    LEFT JOIN orders o ON u.id = o.user_id
                    GROUP BY u.id, u.name
                    ORDER BY total_spent DESC`,
                usage_count: 0,
            };

            const fakeAddSavedQuery = os.add.handler(
                async () => mockSavedQuery,
            );

            const result = await call(fakeAddSavedQuery, {
                name: "User Orders Summary",
                query: mockSavedQuery.query,
            });

            assert.deepStrictEqual(result, mockSavedQuery);
        });

        it("should add a saved query with multiple bind variables", async () => {
            const mockSavedQuery = {
                id: 4,
                name: "Date Range Report",
                query: "SELECT * FROM orders WHERE created_at BETWEEN :start_date AND :end_date",
                bind_vars: [
                    {
                        name: "start_date",
                        default_value: "2024-01-01",
                        description: "Start date for the report",
                    },
                    {
                        name: "end_date",
                        default_value: "2024-12-31",
                        description: "End date for the report",
                    },
                ],
                usage_count: 0,
            };

            const fakeAddSavedQuery = os.add.handler(
                async () => mockSavedQuery,
            );

            const result = await call(fakeAddSavedQuery, {
                name: "Date Range Report",
                query: "SELECT * FROM orders WHERE created_at BETWEEN :start_date AND :end_date",
                bind_vars: [
                    {
                        name: "start_date",
                        default_value: "2024-01-01",
                        description: "Start date for the report",
                    },
                    {
                        name: "end_date",
                        default_value: "2024-12-31",
                        description: "End date for the report",
                    },
                ],
            });

            assert.deepStrictEqual(result, mockSavedQuery);
        });
    });

    describe("list saved queries", () => {
        it("should list all saved queries", async () => {
            const mockQueries = [
                {
                    id: 1,
                    name: "Get All Users",
                    query: "SELECT * FROM users",
                    usage_count: 5,
                },
                {
                    id: 2,
                    name: "Get All Products",
                    query: "SELECT * FROM products",
                    usage_count: 3,
                },
            ];

            const fakeListSavedQueries = os.listMy.handler(
                async () => mockQueries,
            );

            const result = await call(fakeListSavedQueries, undefined);

            assert.deepStrictEqual(result, mockQueries);
        });

        it("should return empty array when no saved queries exist", async () => {
            const fakeListSavedQueries = os.listMy.handler(async () => []);

            const result = await call(fakeListSavedQueries, undefined);

            assert.deepStrictEqual(result, []);
        });

        it("should list queries with bind variables", async () => {
            const mockQueries = [
                {
                    id: 1,
                    name: "Find User",
                    query: "SELECT * FROM users WHERE id = :user_id",
                    bind_vars: [
                        {
                            name: "user_id",
                            default_value: "1",
                            description: "User ID",
                        },
                    ],
                    usage_count: 10,
                },
                {
                    id: 2,
                    name: "Date Range Query",
                    query: "SELECT * FROM orders WHERE date BETWEEN :start AND :end",
                    bind_vars: [
                        {
                            name: "start",
                            default_value: "2024-01-01",
                        },
                        {
                            name: "end",
                            default_value: "2024-12-31",
                        },
                    ],
                    usage_count: 7,
                },
            ];

            const fakeListSavedQueries = os.listMy.handler(
                async () => mockQueries,
            );

            const result = await call(fakeListSavedQueries, undefined);

            assert.deepStrictEqual(result, mockQueries);
        });

        it("should list queries with high usage count", async () => {
            const mockQueries = [
                {
                    id: 1,
                    name: "Popular Query",
                    query: "SELECT COUNT(*) FROM users",
                    usage_count: 150,
                },
            ];

            const fakeListSavedQueries = os.listMy.handler(
                async () => mockQueries,
            );

            const result = await call(fakeListSavedQueries, undefined);

            assert.deepStrictEqual(result, mockQueries);
        });
    });

    describe("update saved query", () => {
        it("should update query name", async () => {
            const mockUpdatedQuery = {
                id: 1,
                name: "Updated Query Name",
                query: "SELECT * FROM users",
                usage_count: 5,
            };

            const fakeUpdateSavedQuery = os.update.handler(
                async () => mockUpdatedQuery,
            );

            const result = await call(fakeUpdateSavedQuery, {
                id: 1,
                name: "Updated Query Name",
            });

            assert.deepStrictEqual(result, mockUpdatedQuery);
        });

        it("should update query content", async () => {
            const mockUpdatedQuery = {
                id: 1,
                name: "Get Active Users",
                query: "SELECT * FROM users WHERE active = true",
                usage_count: 5,
            };

            const fakeUpdateSavedQuery = os.update.handler(
                async () => mockUpdatedQuery,
            );

            const result = await call(fakeUpdateSavedQuery, {
                id: 1,
                query: "SELECT * FROM users WHERE active = true",
            });

            assert.deepStrictEqual(result, mockUpdatedQuery);
        });

        it("should update bind variables", async () => {
            const mockUpdatedQuery = {
                id: 1,
                name: "User Query",
                query: "SELECT * FROM users WHERE email = :email",
                bind_vars: [
                    {
                        name: "email",
                        default_value: "updated@example.com",
                        description: "Updated email description",
                    },
                ],
                usage_count: 5,
            };

            const fakeUpdateSavedQuery = os.update.handler(
                async () => mockUpdatedQuery,
            );

            const result = await call(fakeUpdateSavedQuery, {
                id: 1,
                bind_vars: [
                    {
                        name: "email",
                        default_value: "updated@example.com",
                        description: "Updated email description",
                    },
                ],
            });

            assert.deepStrictEqual(result, mockUpdatedQuery);
        });

        it("should update multiple fields at once", async () => {
            const mockUpdatedQuery = {
                id: 1,
                name: "Comprehensive User Report",
                query: `SELECT u.*, COUNT(o.id) as order_count
                    FROM users u
                    LEFT JOIN orders o ON u.id = o.user_id
                    GROUP BY u.id`,
                bind_vars: [
                    {
                        name: "min_orders",
                        default_value: "1",
                        description: "Minimum number of orders",
                    },
                ],
                usage_count: 12,
            };

            const fakeUpdateSavedQuery = os.update.handler(
                async () => mockUpdatedQuery,
            );

            const result = await call(fakeUpdateSavedQuery, {
                id: 1,
                name: "Comprehensive User Report",
                query: mockUpdatedQuery.query,
                bind_vars: [
                    {
                        name: "min_orders",
                        default_value: "1",
                        description: "Minimum number of orders",
                    },
                ],
            });

            assert.deepStrictEqual(result, mockUpdatedQuery);
        });

        it("should update usage count", async () => {
            const mockUpdatedQuery = {
                id: 1,
                name: "User Query",
                query: "SELECT * FROM users",
                usage_count: 25,
            };

            const fakeUpdateSavedQuery = os.update.handler(
                async () => mockUpdatedQuery,
            );

            const result = await call(fakeUpdateSavedQuery, {
                id: 1,
                usage_count: 25,
            });

            assert.deepStrictEqual(result, mockUpdatedQuery);
        });
    });

    describe("delete saved query", () => {
        it("should delete a saved query successfully", async () => {
            const mockDeletedQuery = {
                id: 1,
                name: "Deleted Query",
                query: "SELECT * FROM users",
                usage_count: 0,
            };

            const fakeDeleteSavedQuery = os.delete.handler(
                async () => mockDeletedQuery,
            );

            const result = await call(fakeDeleteSavedQuery, { id: 1 });

            assert.deepStrictEqual(result, mockDeletedQuery);
        });

        it("should delete a query with bind variables", async () => {
            const mockDeletedQuery = {
                id: 2,
                name: "Query with Binds",
                query: "SELECT * FROM users WHERE id = :user_id",
                bind_vars: [
                    {
                        name: "user_id",
                        default_value: "1",
                    },
                ],
                usage_count: 5,
            };

            const fakeDeleteSavedQuery = os.delete.handler(
                async () => mockDeletedQuery,
            );

            const result = await call(fakeDeleteSavedQuery, { id: 2 });

            assert.deepStrictEqual(result, mockDeletedQuery);
        });
    });
});
