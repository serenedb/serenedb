import { readFileSync } from "node:fs";
import { beforeAll, describe, expect, it } from "vitest";

/**
 * Relevance eval against a RUNNING backend — opt-in, skipped by `npm test`:
 *
 *   npm run eval                                # hybrid vs http://localhost:7700
 *   EVAL_MODE=fulltext npm run eval             # skips the semantic category
 *   EVAL_BACKEND=http://host:7700 npm run eval
 *   EVAL_CAT=typos npm run eval                 # one category, no aggregate gate
 *   EVAL_SETS=cases,questions,names,summaries,sentences npm run eval
 *
 * Sets (relevance.<set>.json):
 *   cases      — graded cases per category; each must land its page in the
 *                top 3, plus an aggregate hit@1 / MRR@10 bar
 *   questions  — natural-language questions, symptoms and task phrases
 *   names      — object names (functions, settings, types, statements, dot
 *                commands) sampled from serened's embedded docs catalog
 *   summaries  — those objects' one-line descriptions
 *   sentences  — a sentence pasted from every page
 * The last four are page-level batteries in the style of serenedb#1216:
 * they report r@1 / r@5 / MRR and hold a floor per set, not per query.
 */

const enabled = Boolean(process.env.EVAL);
const BACKEND = process.env.EVAL_BACKEND ?? "http://localhost:7700";
const MODE = process.env.EVAL_MODE === "fulltext" ? "fulltext" : "hybrid";
const ONLY_CAT = process.env.EVAL_CAT;
const SETS = (process.env.EVAL_SETS ?? "cases").split(",").map((s) => s.trim());
const K = 10;

/**
 * r@1 / r@5 floors per battery and mode: the reference run minus a margin.
 * Reference stack: the published serenedb.com/docs HTML (docs only, no
 * blog) indexed with the production config, SereneDB 26.09.2, ollama
 * nomic-embed-text. Measured (r@1 / r@5):
 *   fulltext — questions .52/.75, names .99/1.0, summaries .98/1.0, sentences .98/.99
 *   hybrid   — questions .59/.82, names .99/1.0, summaries .98/.99, sentences .98/.99
 * Indexing the blog alongside costs the questions about .05 (posts that
 * mention everything); the other batteries don't move.
 */
const FLOORS: Record<string, Record<"fulltext" | "hybrid", { r1: number; r5: number }>> = {
    questions: { fulltext: { r1: 0.48, r5: 0.71 }, hybrid: { r1: 0.55, r5: 0.78 } },
    names: { fulltext: { r1: 0.96, r5: 0.99 }, hybrid: { r1: 0.96, r5: 0.99 } },
    summaries: { fulltext: { r1: 0.95, r5: 0.98 }, hybrid: { r1: 0.95, r5: 0.97 } },
    sentences: { fulltext: { r1: 0.95, r5: 0.98 }, hybrid: { r1: 0.95, r5: 0.97 } },
};

interface RelevanceCase {
    cat: string;
    q: string;
    /** Page URL path (a prefix for `cases`, exact for batteries), or a case-insensitive title regex. */
    expect: Array<string | { title: string }>;
    /** Pages that must NOT appear in the top 3 (negation cases). */
    exclude?: string[];
    /** A documented miss (why): expected to stay out of the top 3. */
    known?: string;
}

const load = (set: string): RelevanceCase[] =>
    (
        JSON.parse(readFileSync(new URL(`./relevance.${set}.json`, import.meta.url), "utf8")) as {
            cases: RelevanceCase[];
        }
    ).cases;

/** Path of the page a result URL points to — absolute site URLs included. */
const page = (url: string): string => {
    const path = /^https?:\/\//.test(url) ? new URL(url).pathname : url;
    return path.split("#")[0].replace(/\/+$/, "");
};
const matches = (e: string | { title: string }, item: { url: string; title: string }, exact: boolean) =>
    typeof e === "string"
        ? exact
            ? page(item.url) === e
            : page(item.url).startsWith(e)
        : new RegExp(e.title, "i").test(item.title);

async function search(q: string): Promise<Array<{ url: string; title: string }>> {
    const res = await fetch(`${BACKEND}/v1/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ q, mode: MODE, limit: K }),
    });
    if (!res.ok) throw new Error(`POST /v1/search → ${res.status} ${res.statusText}`);
    const data = (await res.json()) as {
        mode?: string;
        results?: Array<{ url: string; title: string }>;
    };
    // a hybrid eval silently served by the fulltext fallback (fulltext-only
    // install, embeddings down) would report fulltext numbers as hybrid
    if (data.mode && data.mode !== MODE) {
        throw new Error(`asked for ${MODE}, the backend served ${data.mode}`);
    }
    return data.results ?? [];
}

const rankOf = (c: RelevanceCase, results: Array<{ url: string; title: string }>, exact: boolean) => {
    const i = results.findIndex((r) => c.expect.some((e) => matches(e, r, exact)));
    return i < 0 ? null : i + 1;
};

const stats = (rs: Array<number | null>) => ({
    n: rs.length,
    h1: rs.filter((r) => r === 1).length / rs.length,
    h3: rs.filter((r) => r !== null && r <= 3).length / rs.length,
    h5: rs.filter((r) => r !== null && r <= 5).length / rs.length,
    mrr: rs.reduce((s: number, r) => s + (r ? 1 / r : 0), 0) / rs.length,
});
const pct = (x: number) => `${Math.round(100 * x)}%`.padStart(4);

describe.runIf(enabled)(`search relevance — ${MODE} @ ${BACKEND}`, () => {
    beforeAll(async () => {
        const res = await fetch(`${BACKEND}/v1/health`).catch(() => null);
        if (!res?.ok) {
            throw new Error(
                `backend not reachable at ${BACKEND} — start the stack first, or point EVAL_BACKEND elsewhere`,
            );
        }
    });

    describe.runIf(SETS.includes("cases"))("cases", () => {
        // the semantic-paraphrase category needs embeddings by construction
        const selected = load("cases")
            .filter((c) => (ONLY_CAT ? c.cat === ONLY_CAT : true))
            .filter((c) => MODE === "hybrid" || c.cat !== "semantic");
        const ranks = new Map<string, number | null>();

        it.each(selected.map((c) => [c.cat, c.q, c] as const))(
            "[%s] “%s” lands in the top 3",
            async (_cat, _q, c) => {
                const results = await search(c.q);
                const rank = rankOf(c, results, false);
                ranks.set(`${c.cat}|${c.q}`, rank);

                const top3 = results.slice(0, 3);
                const got = top3.map((r, i) => `  ${i + 1}. ${r.title}  <${page(r.url)}>`).join("\n");
                if (c.exclude) {
                    const bad = top3.find((r) => c.exclude!.some((e) => page(r.url).startsWith(e)));
                    expect(bad, `excluded page ranked in the top 3:\n${got}`).toBeUndefined();
                }
                if (c.known) {
                    // a known miss that starts passing should lose its marker
                    expect(rank !== null && rank <= 3, `known miss now passes (${c.known}) — drop "known"`).toBe(false);
                    return;
                }
                expect(rank !== null && rank <= 3, `rank=${rank ?? "-"}, top 3 was:\n${got}`).toBe(true);
            },
        );

        // aggregate gate — only meaningful on a full hybrid run
        it.runIf(!ONLY_CAT && MODE === "hybrid")("aggregate: hit@1 ≥ 75%, MRR@10 ≥ 0.85", () => {
            const byCat = new Map<string, Array<number | null>>();
            for (const [key, rank] of ranks) {
                const cat = key.split("|")[0];
                (byCat.get(cat) ?? byCat.set(cat, []).get(cat)!).push(rank);
            }
            console.log(`\ncategory          n   hit@1  hit@3  MRR@10`);
            for (const [cat, rs] of [...byCat.entries()].sort()) {
                const s = stats(rs);
                console.log(
                    `${cat.padEnd(16)}${String(s.n).padStart(3)}   ${pct(s.h1)}  ${pct(s.h3)}   ${s.mrr.toFixed(3)}`,
                );
            }

            const all = stats([...ranks.values()]);
            console.log(
                `${"TOTAL".padEnd(16)}${String(all.n).padStart(3)}   ${pct(all.h1)}  ${pct(all.h3)}   ${all.mrr.toFixed(3)}\n`,
            );
            expect(all.h1, "hit@1 regressed below 75%").toBeGreaterThanOrEqual(0.75);
            expect(all.mrr, "MRR@10 regressed below 0.85").toBeGreaterThanOrEqual(0.85);
        });
    });

    for (const set of SETS.filter((s) => s !== "cases")) {
        it(`battery: ${set}`, { timeout: 600_000 }, async () => {
            const cases = load(set);
            const ranks: Array<number | null> = [];
            const misses: string[] = [];
            // a few in flight at a time, like a handful of concurrent users
            for (let i = 0; i < cases.length; i += 4) {
                const batch = cases.slice(i, i + 4);
                const got = await Promise.all(batch.map((c) => search(c.q)));
                batch.forEach((c, j) => {
                    const rank = rankOf(c, got[j], true);
                    ranks.push(rank);
                    if (rank !== 1 && misses.length < 15) {
                        misses.push(`  ${rank ?? "-"}  ${c.q.slice(0, 90)}  → ${page(got[j][0]?.url ?? "")}`);
                    }
                });
            }
            const s = stats(ranks);
            console.log(
                `\n${set} (${MODE}) n=${s.n}  r@1 ${s.h1.toFixed(3)}  r@3 ${s.h3.toFixed(3)}  r@5 ${s.h5.toFixed(3)}  MRR ${s.mrr.toFixed(3)}\n` +
                    `first misses:\n${misses.join("\n")}\n`,
            );
            const floor = FLOORS[set]?.[MODE];
            if (floor) {
                expect(s.h1, `${set} r@1 regressed`).toBeGreaterThanOrEqual(floor.r1);
                expect(s.h5, `${set} r@5 regressed`).toBeGreaterThanOrEqual(floor.r5);
            }
        });
    }
});
