/**
 * Splits a configured OpenAI-compatible base URL into what CREATE SECRET
 * wants. Two shapes exist in the wild:
 *   - bare host (Ollama, vLLM: "http://localhost:11434")   -> path "/v1/embeddings"
 *   - root includes a path ("https://api.openai.com/v1",
 *     ".../v1beta/openai" for Gemini, ".../api/v1" for OpenRouter)
 *                                                          -> path "/embeddings"
 * Returns null for the OpenAI default (no base_url needed on the secret).
 */
export function resolveProvider(
    rawBaseUrl: string,
): { baseUrl: string; embeddingsPath: string } | null {
    const base = rawBaseUrl.replace(/\/+$/, "");
    let url: URL;
    try {
        url = new URL(base);
    } catch {
        return null;
    }
    if (/(^|\.)api\.openai\.com$/.test(url.hostname)) return null;
    const hasPath = url.pathname !== "" && url.pathname !== "/";
    return { baseUrl: base, embeddingsPath: hasPath ? "/embeddings" : "/v1/embeddings" };
}

/**
 * Chat-completions endpoint for a configured base URL. A base with a path
 * ("https://api.openai.com/v1", Gemini's ".../v1beta/openai") already IS the
 * OpenAI-compatible root; a bare host (Ollama) needs the "/v1" segment.
 */
export function chatCompletionsUrl(rawBaseUrl: string | undefined): string {
    const base = (rawBaseUrl || "https://api.openai.com/v1").replace(/\/+$/, "");
    try {
        const url = new URL(base);
        const hasPath = url.pathname !== "" && url.pathname !== "/";
        return hasPath ? `${base}/chat/completions` : `${base}/v1/chat/completions`;
    } catch {
        return `${base}/v1/chat/completions`;
    }
}
