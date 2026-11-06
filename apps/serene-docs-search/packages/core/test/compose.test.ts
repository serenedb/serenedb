import { describe, expect, it } from "vitest";
import { generateCompose } from "../src/compose";
import { VERSION, defaultConfig, generatePassword } from "../src/defaults";
import type { SereneSearchConfig } from "../src/types";

const service = (compose: string, name: string): string => {
    const start = compose.indexOf(`\n  ${name}:\n`);
    expect(start).toBeGreaterThanOrEqual(0);
    const rest = compose.slice(start + 1);
    const end = rest.search(/\n\n/);
    return end < 0 ? rest : rest.slice(0, end);
};

const folderConfig = (): SereneSearchConfig => defaultConfig({ type: "folder", path: "./docs" });

describe("generateCompose", () => {
    it("gives SereneDB and the backend the same postgres password", () => {
        const dbPassword = generatePassword();
        const compose = generateCompose({ config: folderConfig(), token: "sk-local-t", dbPassword });

        expect(service(compose, "serenedb")).toContain(`      POSTGRES_PASSWORD: "${dbPassword}"`);
        expect(service(compose, "search-backend")).toContain(`      SERENEDB_PASSWORD: "${dbPassword}"`);
        expect(service(compose, "search-backend")).toContain("      SERENE_SEARCH_TOKEN: sk-local-t");
    });

    it("quotes the password so YAML keeps a digit-only one a string", () => {
        const compose = generateCompose({ config: folderConfig(), token: "t", dbPassword: "1234e5" });

        expect(compose).toContain('POSTGRES_PASSWORD: "1234e5"');
        expect(compose).toContain('SERENEDB_PASSWORD: "1234e5"');
    });

    it("pins the docs-search images to the package version", () => {
        const config = { ...folderConfig(), mcp: { enabled: true } };
        const compose = generateCompose({ config, token: "t", dbPassword: "p" });

        expect(service(compose, "search-backend")).toContain(`image: serenedb/docs-search-backend:${VERSION}`);
        expect(service(compose, "docs-search-mcp")).toContain(`image: serenedb/docs-search-mcp:${VERSION}`);
        expect(service(compose, "serenedb")).toContain("image: serenedb/serenedb:latest");
    });

    it("takes image overrides", () => {
        const config = { ...folderConfig(), mcp: { enabled: true } };
        const compose = generateCompose({
            config,
            token: "t",
            dbPassword: "p",
            serenedbImage: "serenedb/serenedb:26.11.1",
            backendImage: "local/backend:dev",
            mcpImage: "local/mcp:dev",
        });

        expect(compose).toContain("image: serenedb/serenedb:26.11.1");
        expect(compose).toContain("image: local/backend:dev");
        expect(compose).toContain("image: local/mcp:dev");
    });
});
