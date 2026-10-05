import { getDbContext } from "@database";
import type { Section } from "@repositories/sections";
import type { CatalogEntry } from "./objects.types";

const INSERT_BATCH = 300;

/**
 * Version marker for the catalog build (meta table, cleared before a
 * rebuild and set after it). Bump when the extraction rules change so a
 * quiet corpus still gets its catalog rebuilt.
 */
export const OBJECTS_SIGNATURE = "v4";

/** Ties between objects of one name: definitions before mentions in tables. */
const KIND_ORDER = ["function", "statement", "command", "type", "setting", "name"];

const COLUMNS = "name, section_id, kind, signature, source, level, ord";

/**
 * The object catalog: one row per (lookup key, documenting section). Plain
 * equality lookups on a small table — no text analysis involved, a name is
 * matched exactly as the docs spell it (case-insensitive).
 */
export const ObjectsRepository = {
    /** Cheap idempotent DDL, safe to run on every config apply. */
    ensureSchema: async (): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(
            `CREATE TABLE IF NOT EXISTS ${ctx.objectsTable} (
                name VARCHAR, section_id VARCHAR, kind VARCHAR, signature VARCHAR,
                source VARCHAR, level INTEGER, ord INTEGER)`,
        );
    },

    /**
     * Replaces the catalog whole (dropped and recreated, so column changes
     * need no migration) whenever the corpus changed or OBJECTS_SIGNATURE
     * moved — bump it on any column or extraction change.
     */
    replaceAll: async (entries: CatalogEntry[]): Promise<void> => {
        const ctx = getDbContext();
        await ctx.pool.query(`DROP TABLE IF EXISTS ${ctx.objectsTable}`);
        await ObjectsRepository.ensureSchema();
        for (let i = 0; i < entries.length; i += INSERT_BATCH) {
            const batch = entries.slice(i, i + INSERT_BATCH);
            const values = batch
                .map((_, j) => `(${[1, 2, 3, 4, 5, 6, 7].map((k) => `$${j * 7 + k}`).join(", ")})`)
                .join(", ");
            await ctx.pool.query(
                `INSERT INTO ${ctx.objectsTable} (${COLUMNS}) VALUES ${values}`,
                batch.flatMap((e, j) => [
                    e.name,
                    e.sectionId,
                    e.kind,
                    e.signature,
                    e.source,
                    e.level,
                    i + j,
                ]),
            );
        }
    },

    /**
     * Sections documenting an object of exactly this name, best first: a
     * heading that IS the name before a table row listing it, the page
     * about it (CREATE INDEX's own page) before a subsection elsewhere.
     */
    lookup: async (name: string): Promise<CatalogEntry[]> => {
        const ctx = getDbContext();
        const r = await ctx.pool.query(
            `SELECT ${COLUMNS} FROM ${ctx.objectsTable} WHERE name = $1`,
            [name],
        );
        return r.rows
            .map((row) => ({
                name: String(row.name),
                sectionId: String(row.section_id),
                kind: String(row.kind) as CatalogEntry["kind"],
                signature: String(row.signature),
                source: String(row.source) as CatalogEntry["source"],
                level: Number(row.level),
                ord: Number(row.ord),
            }))
            .sort(
                (a, b) =>
                    (a.source === "heading" ? 0 : 1) - (b.source === "heading" ? 0 : 1) ||
                    a.level - b.level ||
                    KIND_ORDER.indexOf(a.kind) - KIND_ORDER.indexOf(b.kind) ||
                    a.ord - b.ord,
            )
            .map(({ ord: _ord, ...e }) => e);
    },
};

/** The name as a whole token of a heading ("The read_json Function"). */
const namedIn = (name: string, title: string): boolean => {
    const at = title.toLowerCase().indexOf(name);
    if (at < 0) return false;
    const before = at === 0 ? " " : title[at - 1];
    const after = title[at + name.length] ?? " ";
    return /[\s(]/.test(before) && /[\s(,:]/.test(after);
};

/**
 * Catalog rows for a parsed corpus: every lookup key of every object.
 *
 * A table row is the weaker witness. When a heading on the same page IS
 * the object (a function's own "date_trunc(part, date)" section next to
 * the summary table listing it), the row adds nothing and is dropped; when
 * a heading on that page merely names it ("The read_json Function"), the
 * row points there instead of at the table's section. And a page titled
 * with a known name becomes that name's own entry.
 */
export function catalogEntries(sections: Section[]): CatalogEntry[] {
    const byPage = new Map<string, Section[]>();
    for (const s of sections) (byPage.get(s.path) ?? byPage.set(s.path, []).get(s.path)!).push(s);

    const out: CatalogEntry[] = [];
    const seen = new Set<string>();
    const add = (e: CatalogEntry) => {
        const key = `${e.name}\0${e.sectionId}`;
        if (seen.has(key)) return;
        seen.add(key);
        out.push(e);
    };
    for (const [, page] of byPage) {
        const headed = new Set<string>();
        for (const s of page) {
            for (const o of s.objects ?? []) {
                if (o.source === "heading") o.names.forEach((n) => headed.add(n));
            }
        }
        for (const s of page) {
            for (const o of s.objects ?? []) {
                for (const name of o.names) {
                    if (o.source === "table" && headed.has(name)) continue;
                    let at = s;
                    // only code-like names: "point" must not jump to "Floating-Point …"
                    if (o.source === "table" && /[_.]/.test(name)) {
                        at = page.find((x) => x !== s && x.level > 1 && namedIn(name, x.title)) ?? s;
                    }
                    add({
                        name,
                        sectionId: at.id,
                        kind: o.kind,
                        signature: o.signature,
                        source: o.source,
                        level: at.level,
                    });
                }
            }
        }
    }
    // a page whose own title is an object's name is the page about it
    // ("Union" for the UNION type) — tables elsewhere only list it
    const byName = new Map<string, CatalogEntry>();
    for (const e of out) if (!byName.has(e.name)) byName.set(e.name, e);
    for (const s of sections) {
        if (s.level > 1) continue;
        const e = byName.get(s.title.trim().toLowerCase());
        if (e) add({ ...e, sectionId: s.id, signature: s.title, source: "heading", level: s.level });
    }
    return out;
}
