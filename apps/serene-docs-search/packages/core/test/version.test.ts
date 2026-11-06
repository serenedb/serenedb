import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { VERSION } from "../src/defaults";

interface Manifest {
    name: string;
    version: string;
    dependencies?: Record<string, string>;
    devDependencies?: Record<string, string>;
}

const manifest = (path: string): Manifest =>
    JSON.parse(readFileSync(new URL(path, import.meta.url), "utf8")) as Manifest;

const MANIFESTS = [
    "../../../package.json",
    "../package.json",
    "../../backend/package.json",
    "../../mcp/package.json",
    "../../react/package.json",
    "../../embed/package.json",
];

describe("VERSION", () => {
    it("matches every package of the app and their dependencies on each other", () => {
        for (const path of MANIFESTS) {
            const pkg = manifest(path);
            expect(pkg.version, pkg.name).toBe(VERSION);
            const deps = { ...pkg.dependencies, ...pkg.devDependencies };
            for (const [name, range] of Object.entries(deps)) {
                if (name.startsWith("@serenedb/docs-search-")) expect(range, `${pkg.name} -> ${name}`).toBe(VERSION);
            }
        }
    });
});
