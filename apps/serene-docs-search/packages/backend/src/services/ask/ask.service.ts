import {
    DEFAULT_SYSTEM_PROMPT,
    DOCS_AGENT_TOOLS,
    formatDocsHit,
    formatDocsSection,
    type AiConfig,
    type AskEvent,
    type AskMessage,
    type AskSource,
    type SearchResultItem,
} from "@serenedb/docs-search-core";
import { SearchRepository } from "@repositories/search";
import { SectionsRepository } from "@repositories/sections";
import { DocsTools } from "@services/tools";
import { chatCompletionsUrl } from "@utils/providers";

const MAX_CONTEXT_SECTIONS = 6;
const MAX_SECTION_CHARS = 3000;
const MAX_HISTORY_MESSAGES = 8;
const MAX_HISTORY_CHARS = 4000;
const MAX_ROUNDS = 5;
const MAX_THOUGHT_CHARS = 220;

/** Clamp client-supplied history: valid roles only, last N, capped length. */
export function sanitizeHistory(history: unknown): AskMessage[] {
    if (!Array.isArray(history)) return [];
    return history
        .filter(
            (m): m is AskMessage =>
                !!m &&
                typeof m === "object" &&
                ((m as AskMessage).role === "user" || (m as AskMessage).role === "assistant") &&
                typeof (m as AskMessage).content === "string" &&
                (m as AskMessage).content.trim() !== "",
        )
        .slice(-MAX_HISTORY_MESSAGES)
        .map((m) => ({ role: m.role, content: m.content.slice(0, MAX_HISTORY_CHARS) }));
}

/**
 * The first sentence or two of the model's thought, as one trail-friendly
 * line. Thinking models produce long reasoning; the trail wants a glimpse.
 */
export function excerptThought(text: string): string {
    const clean = text.replace(/\s+/g, " ").trim();
    if (!clean) return "";
    const sentences = clean.match(/[^.!?]+[.!?]+/g);
    let excerpt = sentences
        ? sentences
              .slice(0, 2)
              .map((s) => s.trim())
              .join(" ")
        : clean;
    if (excerpt.length > MAX_THOUGHT_CHARS) {
        excerpt = excerpt.slice(0, MAX_THOUGHT_CHARS).replace(/\s+\S*$/, "") + "…";
    }
    return excerpt;
}

// tool definitions live in core so the MCP server and this agent stay in sync
const TOOLS = DOCS_AGENT_TOOLS;

interface ChatMessage {
    role: string;
    content: string | null;
    tool_calls?: { id: string; type: "function"; function: { name: string; arguments: string } }[];
    tool_call_id?: string;
}

/**
 * "Ask AI" answering — a ReAct-style research loop: the model thinks (the
 * thought lands in the trail), calls search_docs / read_section, studies the
 * results and repeats until it can answer. Tool rounds run non-streaming
 * (reliable tool calls everywhere, incl. ollama); only a forced final round
 * streams. Providers without tool support fall back to single-shot RAG.
 */
export const AskService = {
    stream: async (
        ai: AiConfig,
        hybrid: boolean,
        question: string,
        emit: (ev: AskEvent) => void,
        signal?: AbortSignal,
        history?: AskMessage[],
    ): Promise<void> => {
        const answers = ai.answers;
        if (!answers?.model) {
            emit({ type: "error", message: "AI answers provider is not configured" });
            return;
        }
        const url = chatCompletionsUrl(answers.baseUrl);
        const headers = {
            "Content-Type": "application/json",
            ...(answers.apiKey ? { Authorization: `Bearer ${answers.apiKey}` } : {}),
        };

        const system =
            (ai.systemPrompt || DEFAULT_SYSTEM_PROMPT) +
            "\nYou are the documentation assistant for this product: every question is about " +
            "this product, and you answer from its documentation, which you research first — " +
            "step by step:\n" +
            "1. A search for the user's literal question has already run; its results follow. " +
            "Say in a short sentence what the user needs, then add 1-2 search_docs calls of " +
            "your own when other angles would help (focused keyword queries, 2-6 words, " +
            "concrete feature names — never pronouns like it/this).\n" +
            "2. Snippets are previews, not answers. Before answering, read_section the most " +
            "promising hits (at least one). Think out loud in a sentence between steps; " +
            "search again from another angle when results look off-topic.\n" +
            "3. Write the final answer from what you read:\n" +
            '- "how do I / how to" questions want a practical guide: concrete steps with the ' +
            "exact commands, code or config from the docs, in fenced code blocks.\n" +
            "- Cite sources as [n] where relevant; use markdown.\n" +
            "- Claim the docs don't cover something ONLY after reading the closest matching " +
            "sections — a section whose title matches the question almost always contains " +
            "the answer; read it instead of guessing from its snippet.";

        const messages: ChatMessage[] = [
            { role: "system", content: system },
            ...sanitizeHistory(history).map((m) => ({ role: m.role, content: m.content })),
            { role: "user", content: `Question: ${question}` },
        ];

        // source numbering is stable across the whole conversation turn: the
        // same section keeps its [n] no matter how many searches surface it
        const sources: AskSource[] = [];
        const numberById = new Map<string, number>();
        let sectionsRead = 0;
        let readNudgeUsed = false;
        let answerNudgeUsed = false;
        let seeded = false;

        const runTool = async (name: string, argsJson: string): Promise<string> => {
            let args: Record<string, unknown> = {};
            try {
                args = JSON.parse(argsJson || "{}") as Record<string, unknown>;
            } catch {
                return "error: tool arguments were not valid JSON";
            }
            if (name === "search_docs") {
                const q = String(args.query ?? "").trim();
                if (!q) return "error: missing query";
                const hits = await DocsTools.search(q, hybrid);
                emit({
                    type: "tool",
                    name: "search_docs",
                    detail: `${q} — ${hits.length} ${hits.length === 1 ? "section" : "sections"}`,
                });
                if (hits.length === 0) return "no results";
                const lines = hits.map((h) => {
                    let n = numberById.get(h.id);
                    if (n == null) {
                        n = sources.length + 1;
                        numberById.set(h.id, n);
                        sources.push({ n, id: h.id, path: h.crumb || h.url, url: h.url, title: h.title });
                    }
                    return formatDocsHit(n, h);
                });
                emit({ type: "sources", sources: [...sources] });
                return lines.join("\n\n");
            }
            if (name === "read_section") {
                const n = Number(args.n);
                const src = sources.find((s) => s.n === n);
                if (!src) return `error: unknown source number ${n} — call search_docs first`;
                emit({ type: "tool", name: "read_section", detail: `[${n}] ${src.title}` });
                const text = await DocsTools.read({ id: src.id });
                if (!text) return "error: section content unavailable";
                sectionsRead++;
                return formatDocsSection(`[${n}] ${src.title}`, text);
            }
            return `error: unknown tool "${name}"`;
        };

        // baseline grounding: search the raw question before the model takes
        // over — its own queries add angles, but one typo'd query must never
        // leave the context without the on-topic pages (they also become the
        // sources the forced read falls back to)
        const seedSearch = async () => {
            if (seeded) return;
            seeded = true;
            const seedArgs = JSON.stringify({ query: question });
            const seedResults = await runTool("search_docs", seedArgs);
            // Deliver the baseline search as plain context, NOT a synthetic
            // assistant tool_call: Gemini 3.x rejects a functionCall the model
            // didn't itself produce ("Function call is missing a
            // thought_signature"). runTool still emits the search_docs event so
            // the seed shows in the trail.
            messages.push({
                role: "user",
                content:
                    `A search for my question has already run. Results:\n\n${seedResults}\n\n` +
                    "Research further with search_docs / read_section as needed, then answer.",
            });
        };
        await seedSearch();

        for (let round = 0; round < MAX_ROUNDS; round++) {
            const allowTools = round < MAX_ROUNDS - 1;

            if (!allowTools) {
                // out of research budget — flip the model into answer mode
                // explicitly, or it keeps narrating its research plans
                messages.push({
                    role: "user",
                    content:
                        "Stop researching. Using the sources gathered above, write the final " +
                        "answer to my original question now. Cite sources as [n] where relevant.",
                });
                const res = await fetch(url, {
                    method: "POST",
                    headers,
                    body: JSON.stringify({ model: answers.model, stream: true, messages }),
                    signal,
                });
                if (!res.ok || !res.body) {
                    const detail = await res.text().catch(() => "");
                    emit({
                        type: "error",
                        message: `AI provider error ${res.status}: ${detail.slice(0, 300)}`,
                    });
                    return;
                }
                await consumeCompletionStream(res.body, (delta) => {
                    if (delta.content) emit({ type: "delta", text: delta.content });
                });
                break;
            }

            const res = await fetch(url, {
                method: "POST",
                headers,
                body: JSON.stringify({
                    model: answers.model,
                    stream: false,
                    messages,
                    tools: TOOLS,
                }),
                signal,
            });
            if (!res.ok) {
                const detail = await res.text().catch(() => "");
                // provider without tool support — single-shot RAG still works
                if (round === 0 && /tool/i.test(detail)) {
                    return streamLegacy(ai, hybrid, question, emit, signal, history);
                }
                emit({
                    type: "error",
                    message: `AI provider error ${res.status}: ${detail.slice(0, 300)}`,
                });
                return;
            }
            const json = (await res.json()) as {
                choices?: {
                    message?: {
                        content?: string | null;
                        reasoning?: string | null;
                        tool_calls?: { id?: string; function?: { name?: string; arguments?: string } }[];
                    };
                }[];
            };
            const msg = json.choices?.[0]?.message ?? {};
            const content = msg.content ?? "";
            const calls = (msg.tool_calls ?? [])
                .map((tc, i) => ({
                    id: tc.id || `call_${round}_${i}`,
                    name: tc.function?.name ?? "",
                    args: tc.function?.arguments ?? "",
                }))
                .filter((t) => t.name);

            if (calls.length === 0) {
                if (sectionsRead === 0 && sources.length > 0 && !readNudgeUsed) {
                    // it is about to answer from snippets alone (the classic
                    // "the docs don't cover this" miss) — read the top hits
                    // for it and go around again with the full texts in hand
                    readNudgeUsed = true;
                    const toRead = sources.slice(0, 2);
                    // deliver the forced reads as plain context, not a synthetic
                    // tool_call (Gemini 3.x rejects it — missing thought_signature)
                    const reads: string[] = [];
                    for (const s of toRead) {
                        reads.push(await runTool("read_section", JSON.stringify({ n: s.n })));
                    }
                    messages.push({
                        role: "user",
                        content:
                            `Full text of the most relevant sources:\n\n${reads.join("\n\n")}\n\n` +
                            "Answer my original question from these now, citing sources as [n].",
                    });
                    continue;
                }
                if (!content.trim()) {
                    // thinking models occasionally close a round with pure
                    // reasoning and empty content — demand the answer once
                    if (!answerNudgeUsed) {
                        answerNudgeUsed = true;
                        messages.push({
                            role: "user",
                            content:
                                "Write the final answer now, based on the sources gathered " +
                                "above. Cite sources as [n] where relevant.",
                        });
                        continue;
                    }
                    break;
                }
                // the whole non-streamed message is the final answer
                emit({ type: "delta", text: content });
                break;
            }

            // surface the model's own reasoning about this step in the trail
            const thought = excerptThought(content || msg.reasoning || "");
            if (thought) emit({ type: "tool", name: "think", detail: thought });

            // echo the model's tool_calls back VERBATIM — reconstructing them
            // drops provider fields Gemini 3.x requires round-tripped on every
            // functionCall (extra_content.google.thought_signature); without it
            // the next round 400s and the whole loop falls back to single-shot.
            messages.push({
                role: "assistant",
                content: content || null,
                tool_calls: msg.tool_calls as ChatMessage["tool_calls"],
            });
            for (const t of calls) {
                messages.push({
                    role: "tool",
                    tool_call_id: t.id,
                    content: await runTool(t.name, t.args),
                });
            }
        }
        emit({ type: "done", model: answers.model });
    },
};

interface CompletionDelta {
    content?: string;
    tool_calls?: { index?: number; id?: string; function?: { name?: string; arguments?: string } }[];
}

/** Reads an OpenAI-compatible SSE completion stream, invoking onDelta per chunk. */
async function consumeCompletionStream(
    body: ReadableStream<Uint8Array>,
    onDelta: (delta: CompletionDelta) => void,
): Promise<void> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let nl: number;
        while ((nl = buf.indexOf("\n")) >= 0) {
            const line = buf.slice(0, nl).trim();
            buf = buf.slice(nl + 1);
            if (!line.startsWith("data:")) continue;
            const payload = line.slice(5).trim();
            if (payload === "[DONE]") continue;
            try {
                const json = JSON.parse(payload) as { choices?: { delta?: CompletionDelta }[] };
                const delta = json.choices?.[0]?.delta;
                if (delta) onDelta(delta);
            } catch {
                /* keep-alives / malformed chunks */
            }
        }
    }
}

/** Single-shot RAG: retrieve once, answer once — for providers without tools. */
async function streamLegacy(
    ai: AiConfig,
    hybrid: boolean,
    question: string,
    emit: (ev: AskEvent) => void,
    signal?: AbortSignal,
    history?: AskMessage[],
): Promise<void> {
    let hits: SearchResultItem[] = [];
    if (hybrid) {
        try {
            hits = await SearchRepository.searchSemantic(question, MAX_CONTEXT_SECTIONS);
        } catch {
            /* fall back to fulltext */
        }
    }
    if (hits.length === 0) {
        hits = (await SearchRepository.searchFulltext(question, MAX_CONTEXT_SECTIONS)).items;
    }
    const withContent = await SectionsRepository.contentsFor(hits.map((h) => h.id));

    const sources: AskSource[] = hits.map((h, i) => ({
        n: i + 1,
        id: h.id,
        path: h.path,
        url: h.url,
        title: h.title,
    }));
    emit({ type: "sources", sources });

    const context = hits
        .map((h, i) => {
            const body = (withContent.get(h.id) ?? "").slice(0, MAX_SECTION_CHARS);
            return `[${i + 1}] ${h.crumb}\nURL: ${h.url}\n${body}`;
        })
        .join("\n\n---\n\n");

    const system = ai.systemPrompt || DEFAULT_SYSTEM_PROMPT;
    const user =
        `Documentation sections:\n\n${context}\n\n` +
        `Question: ${question}\n\n` +
        `Answer using only the sections above. Reference them as [1], [2]… where relevant. ` +
        `Use markdown; keep code in fenced blocks. If the sections don't answer it, say so.`;

    const answers = ai.answers!;
    const res = await fetch(chatCompletionsUrl(answers.baseUrl), {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...(answers.apiKey ? { Authorization: `Bearer ${answers.apiKey}` } : {}),
        },
        body: JSON.stringify({
            model: answers.model,
            stream: true,
            messages: [
                { role: "system", content: system },
                ...sanitizeHistory(history),
                { role: "user", content: user },
            ],
        }),
        signal,
    });
    if (!res.ok || !res.body) {
        const detail = await res.text().catch(() => "");
        emit({ type: "error", message: `AI provider error ${res.status}: ${detail.slice(0, 300)}` });
        return;
    }
    await consumeCompletionStream(res.body, (delta) => {
        if (delta.content) emit({ type: "delta", text: delta.content });
    });
    emit({ type: "done", model: answers.model });
}
