import React from "react";
import { BUNDLED_OLLAMA_URL, defaultProvider, type AiProvider } from "@serenedb/docs-search-core";
import { Field } from "../../components/primitives";
import type { StepProps } from "../types";

/** Provider editor: OpenAI-compatible API or an Ollama server (auto-pulled model). */
export function ProviderFields({
    role,
    provider,
    update,
}: {
    role: "answers" | "embeddings";
    provider: AiProvider | undefined;
    update: StepProps["update"];
}): React.ReactElement {
    const p = provider ?? defaultProvider("openai", role);
    const mutate = (fn: (target: AiProvider) => void) =>
        update((c) => {
            const cur = ((c.ai ??= { enabled: false })[role] ??= defaultProvider(p.kind, role));
            fn(cur);
        });
    const setKind = (kind: AiProvider["kind"]) => {
        if (kind === p.kind) return;
        update((c) => {
            (c.ai ??= { enabled: false })[role] = defaultProvider(kind, role);
        });
    };

    return (
        <>
            <div className="sds-chips">
                <button
                    type="button"
                    className={`sds-chip${p.kind === "openai" ? " on" : ""}`}
                    onClick={() => setKind("openai")}
                >
                    OpenAI-compatible API
                </button>
                <button
                    type="button"
                    className={`sds-chip${p.kind === "ollama" ? " on" : ""}`}
                    onClick={() => setKind("ollama")}
                >
                    Ollama
                </button>
            </div>

            {p.kind === "openai" ? (
                <>
                    <div className="sds-row">
                        <Field label="Base URL" className="sds-grow-3">
                            <input
                                className="sds-input"
                                value={p.baseUrl ?? ""}
                                placeholder="https://api.openai.com/v1"
                                onChange={(e) => mutate((t) => (t.baseUrl = e.target.value))}
                            />
                        </Field>
                        <Field label="API key" className="sds-grow-2">
                            <input
                                className="sds-input"
                                type="password"
                                value={p.apiKey ?? ""}
                                placeholder="sk-… or ${OPENAI_API_KEY}"
                                onChange={(e) => mutate((t) => (t.apiKey = e.target.value || undefined))}
                            />
                        </Field>
                    </div>
                    <Field label={role === "answers" ? "Answer model" : "Embeddings model"}>
                        <input
                            className="sds-input"
                            value={p.model ?? ""}
                            placeholder={role === "answers" ? "gpt-4o-mini" : "text-embedding-3-small"}
                            onChange={(e) => mutate((t) => (t.model = e.target.value))}
                        />
                    </Field>
                    <div className="sds-hint">
                        &gt; vLLM, Gemini, OpenRouter… anything OpenAI-compatible — the key stays in
                        the backend container
                    </div>
                </>
            ) : (
                <>
                    <div className="sds-row">
                        <Field label="Ollama URL" className="sds-grow-2">
                            <input
                                className="sds-input"
                                value={p.baseUrl ?? ""}
                                placeholder={BUNDLED_OLLAMA_URL}
                                onChange={(e) => mutate((t) => (t.baseUrl = e.target.value))}
                            />
                        </Field>
                        <Field
                            label={role === "answers" ? "Answer model — pulled automatically" : "Embeddings model — pulled automatically"}
                            className="sds-grow-2"
                        >
                            <input
                                className="sds-input"
                                value={p.model ?? ""}
                                placeholder={role === "answers" ? "llama3.2" : "nomic-embed-text"}
                                onChange={(e) => mutate((t) => (t.model = e.target.value))}
                            />
                        </Field>
                    </div>
                    <div className="sds-hint">
                        &gt; keep {BUNDLED_OLLAMA_URL} to run Ollama inside the generated stack — the
                        backend pulls the model on first sync
                    </div>
                </>
            )}
        </>
    );
}
