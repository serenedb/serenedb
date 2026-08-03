import { afterEach, describe, expect, it, vi } from "vitest";
import { ModelsService } from "../src/services/models";

const provider = (baseUrl: string) =>
    ({ kind: "ollama", baseUrl, model: "nomic-embed-text" }) as const;

afterEach(() => vi.unstubAllGlobals());

describe("ensureOllamaModel", () => {
    it("does nothing when the model is already present", async () => {
        const fetchMock = vi.fn(async () =>
            new Response(JSON.stringify({ models: [{ name: "nomic-embed-text:latest" }] })),
        );
        vi.stubGlobal("fetch", fetchMock);
        await ModelsService.ensureOllamaModel(provider("http://ollama:11434"));
        expect(fetchMock).toHaveBeenCalledTimes(1); // tags only, no pull
    });

    it("pulls the model when it is missing", async () => {
        const calls: string[] = [];
        vi.stubGlobal(
            "fetch",
            vi.fn(async (url: string | URL) => {
                calls.push(String(url));
                if (String(url).endsWith("/api/tags"))
                    return new Response(JSON.stringify({ models: [] }));
                return new Response(JSON.stringify({ status: "success" }));
            }),
        );
        await ModelsService.ensureOllamaModel(provider("http://ollama:11434"));
        expect(calls[1]).toContain("/api/pull");
    });

    it("skips the check instead of failing when ollama is unreachable from the backend (split topology)", async () => {
        vi.stubGlobal("fetch", vi.fn(async () => {
            throw new TypeError("fetch failed");
        }));
        await expect(
            ModelsService.ensureOllamaModel(provider("http://host.docker.internal:11434")),
        ).resolves.toBeUndefined();
    });
});
