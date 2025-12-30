import { call, implement } from "@orpc/server";
import assert from "node:assert";
import { describe, it } from "node:test";
import { apiRouter } from "../src/routers/index";

describe("apiRouter", () => {
    describe("router structure", () => {
        it("should have all expected routers", () => {
            assert.ok("connection" in apiRouter);
            assert.ok("github" in apiRouter);
            assert.ok("queryExecution" in apiRouter);
            assert.ok("savedQuery" in apiRouter);
        });

        it("should have connection router with all endpoints", () => {
            assert.ok("add" in apiRouter.connection);
            assert.ok("listMy" in apiRouter.connection);
            assert.ok("update" in apiRouter.connection);
            assert.ok("delete" in apiRouter.connection);
        });

        it("should have github router with nested structure", () => {
            assert.ok("authorization" in apiRouter.github);
            assert.ok("vetification" in apiRouter.github);
            assert.ok("actions" in apiRouter.github);
        });

        it("should have github authorization endpoints", () => {
            assert.ok("verify" in apiRouter.github.authorization);
            assert.ok("unauthorize" in apiRouter.github.authorization);
        });

        it("should have github verification endpoints", () => {
            assert.ok("start" in apiRouter.github.vetification);
            assert.ok("verifyDevice" in apiRouter.github.vetification);
        });

        it("should have github action endpoints", () => {
            assert.ok("leaveStar" in apiRouter.github.actions);
            assert.ok("createIssue" in apiRouter.github.actions);
        });

        it("should have query execution router endpoints", () => {
            assert.ok("execute" in apiRouter.queryExecution);
            assert.ok("subscribe" in apiRouter.queryExecution);
        });

        it("should have saved query router endpoints", () => {
            assert.ok("add" in apiRouter.savedQuery);
            assert.ok("listMy" in apiRouter.savedQuery);
            assert.ok("update" in apiRouter.savedQuery);
            assert.ok("delete" in apiRouter.savedQuery);
        });
    });

    describe("integration tests", () => {
        it("should be able to call connection endpoints through main router", () => {
            assert.strictEqual(typeof apiRouter.connection.add, "object");
            assert.strictEqual(typeof apiRouter.connection.listMy, "object");
            assert.strictEqual(typeof apiRouter.connection.update, "object");
            assert.strictEqual(typeof apiRouter.connection.delete, "object");
        });

        it("should be able to call github endpoints through main router", () => {
            assert.strictEqual(
                typeof apiRouter.github.authorization.verify,
                "object",
            );
            assert.strictEqual(
                typeof apiRouter.github.authorization.unauthorize,
                "object",
            );
            assert.strictEqual(
                typeof apiRouter.github.vetification.start,
                "object",
            );
            assert.strictEqual(
                typeof apiRouter.github.vetification.verifyDevice,
                "object",
            );
            assert.strictEqual(
                typeof apiRouter.github.actions.leaveStar,
                "object",
            );
            assert.strictEqual(
                typeof apiRouter.github.actions.createIssue,
                "object",
            );
        });

        it("should be able to call query execution endpoints through main router", () => {
            assert.strictEqual(
                typeof apiRouter.queryExecution.execute,
                "object",
            );
            assert.strictEqual(
                typeof apiRouter.queryExecution.subscribe,
                "object",
            );
        });

        it("should be able to call saved query endpoints through main router", () => {
            assert.strictEqual(typeof apiRouter.savedQuery.add, "object");
            assert.strictEqual(typeof apiRouter.savedQuery.listMy, "object");
            assert.strictEqual(typeof apiRouter.savedQuery.update, "object");
            assert.strictEqual(typeof apiRouter.savedQuery.delete, "object");
        });
    });

    describe("router type safety", () => {
        it("should not have unexpected properties", () => {
            const expectedRouters = [
                "connection",
                "github",
                "queryExecution",
                "savedQuery",
            ];
            const actualRouters = Object.keys(apiRouter);

            assert.deepStrictEqual(
                actualRouters.sort(),
                expectedRouters.sort(),
            );
        });

        it("should have exactly 4 routers", () => {
            assert.strictEqual(Object.keys(apiRouter).length, 4);
        });
    });
});
