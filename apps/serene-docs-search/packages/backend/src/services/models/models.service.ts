import type { AiProvider } from "@serenedb/docs-search-core";
import { BUNDLED_OLLAMA_URL } from "@serenedb/docs-search-core";

interface OllamaTag {
    name: string;
}

export const ModelsService = {
    /**
     * Makes sure an Ollama server has the model locally, pulling it when
     * missing. `/api/pull` with stream:false blocks until the download
     * finishes — large models take a while, so callers run this before the
     * stage that needs it.
     */
    ensureOllamaModel: async (
        provider: AiProvider,
        onProgress?: (msg: string) => void,
    ): Promise<void> => {
        if (provider.kind !== "ollama" || !provider.model) return;
        const base = (provider.baseUrl || BUNDLED_OLLAMA_URL).replace(/\/+$/, "");
        const model = provider.model;

        try {
            const res = await fetch(`${base}/api/tags`, { signal: AbortSignal.timeout(10_000) });
            if (res.ok) {
                const tags = (await res.json()) as { models?: OllamaTag[] };
                const have = (tags.models ?? []).some(
                    (m) => m.name === model || m.name === `${model}:latest`,
                );
                if (have) return;
            }
        } catch (err) {
            // The backend not seeing the ollama server is normal in split
            // topologies (engine in docker, ollama on the host): the URL is
            // meant for the engine, which may reach it fine. Skip the pull
            // check — if the server truly is down, the embed/answer stage
            // fails with its own actionable error.
            console.warn(
                `ollama not reachable from the backend at ${base} (${(err as Error).message}); skipping the model pull check`,
            );
            return;
        }

        onProgress?.(`pulling ${model} from the ollama registry`);
        console.log(`ollama: pulling ${model} on ${base}`);
        const res = await fetch(`${base}/api/pull`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ model, stream: false }),
        });
        if (!res.ok) {
            throw new Error(`ollama pull ${model} failed: ${res.status} ${await res.text().catch(() => "")}`);
        }
        const status = (await res.json().catch(() => null)) as { status?: string } | null;
        if (status?.status !== "success") {
            throw new Error(`ollama pull ${model}: unexpected status ${JSON.stringify(status)}`);
        }
        console.log(`ollama: ${model} ready`);
    },

    /**
     * Kicks off model preparation for one provider. Never rejects — resolves
     * to the Error instead, so each role (answers / embeddings) can fail
     * independently without an unhandled rejection or blocking the other.
     */
    prepareProvider: (
        provider: AiProvider | undefined,
        onProgress?: (msg: string) => void,
    ): Promise<Error | null> => {
        if (!provider || provider.kind !== "ollama" || !provider.model) {
            return Promise.resolve(null);
        }
        return ModelsService.ensureOllamaModel(provider, onProgress).then(
            () => null,
            (err: Error) => {
                console.error(`model preparation failed (${provider.model}):`, err.message);
                return err;
            },
        );
    },
};
