import { readFileSync } from "node:fs";
import { beforeAll, describe, expect, it } from "vitest";

/**
 * Relevance eval against a RUNNING backend — opt-in, skipped by `npm test`:
 *
 *   npm run eval                                # hybrid vs http://localhost:7700
 *   EVAL_MODE=fulltext npm run eval             # skips the semantic category
 *   EVAL_BACKEND=http://host:7700 npm run eval
 *   EVAL_CAT=typos npm run eval                 # one category, no aggregate gate
 *
 * Every case from relevance.cases.json is its own test asserting the expected
 * page lands in the top 3 (the reference run scores hit@3 100%); a final
 * aggregate test holds the hit@1 / MRR@10 bar (reference: hybrid + nomic —
 * hit@1 85%, MRR 0.922).
 */

const enabled = Boolean(process.env.EVAL);
const BACKEND = process.env.EVAL_BACKEND ?? "http://localhost:7700";
const MODE = process.env.EVAL_MODE === "fulltext" ? "fulltext" : "hybrid";
const ONLY_CAT = process.env.EVAL_CAT;
const K = 10;

interface RelevanceCase {
    cat: string;
    q: string;
    /** URL-prefix of the page, or a case-insensitive title regex. */
    expect: Array<string | { title: string }>;
    /** Pages that must NOT appear in the top 3 (negation cases). */
    exclude?: string[];
}

const { cases } = JSON.parse(
    readFileSync(new URL("./relevance.cases.json", import.meta.url), "utf8"),
) as { cases: RelevanceCase[] };

// the semantic-paraphrase category needs embeddings by construction
const selected = cases
    .filter((c) => (ONLY_CAT ? c.cat === ONLY_CAT : true))
    .filter((c) => MODE === "hybrid" || c.cat !== "semantic");

const page = (url: string) => url.split("#")[0];
const matches = (e: string | { title: string }, item: { url: string; title: string }) =>
    typeof e === "string"
        ? page(item.url).startsWith(e)
        : new RegExp(e.title, "i").test(item.title);

async function search(q: string): Promise<Array<{ url: string; title: string }>> {
    const res = await fetch(`${BACKEND}/v1/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ q, mode: MODE, limit: K }),
    });
    if (!res.ok) throw new Error(`POST /v1/search → ${res.status} ${res.statusText}`);
    const data = (await res.json()) as { results?: Array<{ url: string; title: string }> };
    return data.results ?? [];
}

const ranks = new Map<string, number | null>();

describe.runIf(enabled)(`search relevance — ${MODE} @ ${BACKEND}`, () => {
    beforeAll(async () => {
        const res = await fetch(`${BACKEND}/v1/health`).catch(() => null);
        if (!res?.ok) {
            throw new Error(
                `backend not reachable at ${BACKEND} — start the stack first, or point EVAL_BACKEND elsewhere`,
            );
        }
    });

    it.each(selected.map((c) => [c.cat, c.q, c] as const))(
        "[%s] “%s” lands in the top 3",
        async (_cat, _q, c) => {
            const results = await search(c.q);
            let rank: number | null = null;
            for (let i = 0; i < results.length; i++) {
                if (c.expect.some((e) => matches(e, results[i]))) {
                    rank = i + 1;
                    break;
                }
            }
            ranks.set(`${c.cat}|${c.q}`, rank);

            const top3 = results.slice(0, 3);
            const got = top3.map((r, i) => `  ${i + 1}. ${r.title}  <${page(r.url)}>`).join("\n");
            if (c.exclude) {
                const bad = top3.find((r) => c.exclude!.some((e) => page(r.url).startsWith(e)));
                expect(bad, `excluded page ranked in the top 3:\n${got}`).toBeUndefined();
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
        const stats = (rs: Array<number | null>) => ({
            n: rs.length,
            h1: rs.filter((r) => r === 1).length / rs.length,
            h3: rs.filter((r) => r !== null && r <= 3).length / rs.length,
            mrr: rs.reduce((s: number, r) => s + (r ? 1 / r : 0), 0) / rs.length,
        });
        const pct = (x: number) => `${Math.round(100 * x)}%`.padStart(4);
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
