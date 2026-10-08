/**
 * The object catalog: things a reference documents by name — functions,
 * statements, dot commands, settings, types. A query that IS one of those
 * names ("date_trunc", "BIGINT", ".timer", "memory_limit", "CREATE INDEX")
 * goes to the section that documents it first, the way serened's embedded
 * docs search puts a known object first (server/docs/docs_search.cpp,
 * KnownFirst). Lexical ranking alone can't: a name is one rare term, and
 * every page that merely uses the function competes on it.
 *
 * Two sources, both structural rather than site-specific:
 *   - headings that are a signature, a statement or an identifier
 *     ("ts_starts_with(prefix)", "VACUUM", "split_text", ".mode");
 *   - reference tables whose first column is the name (Function / Name /
 *     Command / Setting / Type …), with an Alias(es) column feeding aliases.
 */

import type { DocObject, DocObjectKind } from "@repositories/objects";

export type { DocObject };

/** "date_trunc(", "sdb_docs.search(", "BM25(" — a name glued to its "(". */
const CALL_RE = /^((?:[A-Za-z_][\w$]*\.)*([A-Za-z_][\w$]*))\(/;
/** "CREATE INDEX", "VACUUM", "ALTER TABLE" — upper-case keywords only. */
const KEYWORDS_RE = /^[A-Z][A-Z0-9_]*(?: [A-Z][A-Z0-9_]*){0,4}$/;
/** "split_text", "sdb_docs.search", ".timer" */
const IDENT_RE = /^\.?[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)*$/;
/** Text that reads as code: snake_case, dotted, or one case throughout ("abs", "BM25"). */
const CODE_NAME_RE = /[_.$]|^[a-z][a-z0-9]*$|^[A-Z][A-Z0-9]*$/;

const uniq = (xs: string[]): string[] => [...new Set(xs.filter(Boolean))];

/** Backticks, markdown links and emphasis off a heading or table cell. */
export function plainCell(text: string): string {
    return text
        .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
        .replace(/`([^`]*)`/g, "$1")
        .replace(/\*\*([^*]*)\*\*/g, "$1")
        .replace(/\s+/g, " ")
        .trim();
}

/**
 * Lookup keys of a (possibly qualified) name: the full name, plus the bare
 * last part when that is itself identifier-like — "pg_catalog.pg_class"
 * also answers "pg_class", but "sdb_docs.search" must not claim "search".
 */
function nameKeys(qualified: string, bare: string): string[] {
    const keys = [qualified.toLowerCase()];
    if (bare !== qualified && /[_$]/.test(bare)) keys.push(bare.toLowerCase());
    return keys;
}

/**
 * A call signature and nothing else: "date_trunc(part, date)",
 * "BM25(tableoid[, k1, b])", "f() / af()", "lower(x) -> VARCHAR". Prose
 * that happens to have a parenthesis — "Hybrid (reciprocal rank fusion)",
 * "Range (radius) search" — is not: the "(" must touch a code-like name
 * and the text must end at the matching ")".
 */
function callNames(text: string): string[] | null {
    const names: string[] = [];
    for (const part of text.split(/\s+\/\s+/)) {
        const m = CALL_RE.exec(part);
        if (!m || !CODE_NAME_RE.test(m[1])) return null;
        let depth = 0;
        let close = -1;
        for (let i = m[0].length - 1; i < part.length && close < 0; i++) {
            if (part[i] === "(" || part[i] === "[") depth++;
            else if ((part[i] === ")" || part[i] === "]") && --depth === 0) close = i;
        }
        const rest = close < 0 ? null : part.slice(close + 1).trim();
        if (rest == null || (rest && !/^(->|→|:)/.test(rest))) return null;
        names.push(...nameKeys(m[1], m[2]));
    }
    return names.length ? uniq(names) : null;
}

/**
 * A dot command answers to its dotted name; the bare name only when it
 * can't be an everyday word — ".auto_format" also answers "auto_format",
 * but ".indexes" / ".help" / ".import" must not claim "indexes", "help",
 * "import".
 */
function commandKeys(command: string): string[] {
    const bare = command.slice(1);
    return uniq([command.toLowerCase(), /[_$]/.test(bare) ? bare.toLowerCase() : ""]);
}

function keywordNames(text: string): string[] | null {
    // "SET / RESET" documents both statements
    const parts = text.split(/\s*\/\s*/);
    if (!parts.every((p) => KEYWORDS_RE.test(p))) return null;
    return uniq([text.toLowerCase(), ...parts.map((p) => p.toLowerCase())]);
}

/**
 * A heading that names an object. Plain prose headings ("Examples",
 * "Vector search", "Hybrid (reciprocal rank fusion)") never qualify: an
 * identifier must carry "_" or ".", a call must be code, and an upper-case
 * keyword stands alone only as a page title ("VACUUM" is a statement's
 * page; a "JSON" subsection of a compatibility matrix is not).
 */
export function objectFromTitle(title: string, level = 1): DocObject | null {
    const t = plainCell(title);
    if (t.length < 2 || t.length > 160) return null;
    const call = callNames(t);
    if (call) return { kind: "function", source: "heading", signature: t, names: call };
    const keywords = t.length >= 3 ? keywordNames(t) : null;
    if (keywords && (/[ /]/.test(t) || level <= 1)) {
        return { kind: "statement", source: "heading", signature: t, names: keywords };
    }
    if (IDENT_RE.test(t) && /[_.]/.test(t)) {
        if (t.startsWith(".")) {
            return { kind: "command", source: "heading", signature: t, names: commandKeys(t) };
        }
        const dot = t.lastIndexOf(".");
        return { kind: "function", source: "heading", signature: t, names: nameKeys(t, t.slice(dot + 1)) };
    }
    return null;
}

const NAME_HEADERS: Record<string, DocObjectKind> = {
    function: "function",
    functions: "function",
    aggregate: "function",
    command: "command",
    setting: "setting",
    option: "setting",
    parameter: "setting",
    type: "type",
    name: "name",
    index: "name",
};

/**
 * Objects listed by a reference table. Only tables whose FIRST header
 * names the thing (Function, Name, Command, Setting, Type…) qualify, and
 * only cells shaped like a name: a call, an identifier, a dot command or
 * an upper-case type name. Every table may list code-shaped names
 * (snake_case, dotted, calls); a bare word counts only where the table
 * itself vouches for it:
 *   - function, command and setting tables list code, so a bare word in
 *     one case is a name ("abs", "threads") — a label like "Host" is not;
 *   - an upper-case word is a type only in a type table (first header
 *     Type, or Name with an Alias column — "BIGINT · INT8, LONG");
 *     option and parameter tables list arguments ("CASE", "FORMAT").
 */
export function objectsFromTable(headers: string[], rows: string[][]): DocObject[] {
    const norm = headers.map((h) => plainCell(h).toLowerCase());
    const first = NAME_HEADERS[norm[0] ?? ""];
    if (!first) return [];
    const header = norm[0];
    const aliasCol = norm.findIndex((h) => h === "alias" || h === "aliases");
    const codeTable = first === "function" || first === "command" || header === "setting";
    const typeTable = header === "type" || (header === "name" && aliasCol > 0);
    const out: DocObject[] = [];
    for (const row of rows) {
        const cell = plainCell(row[0] ?? "");
        if (cell.length < 2 || cell.length > 120) continue;
        const aliases =
            aliasCol > 0
                ? plainCell(row[aliasCol] ?? "")
                      .split(",")
                      .map((a) => a.trim().toLowerCase())
                      .filter((a) => a.length >= 2 && IDENT_RE.test(a.replace(/ /g, "_")))
                : [];
        const call = callNames(cell);
        if (call) {
            out.push({
                kind: "function",
                source: "table",
                signature: cell,
                names: uniq([...call, ...aliases]),
            });
            continue;
        }
        if (IDENT_RE.test(cell)) {
            const command = cell.startsWith(".");
            const upper = /^[A-Z][A-Z0-9_]*$/.test(cell);
            const codeShaped = /[_.$]/.test(cell);
            const bareName = codeTable && CODE_NAME_RE.test(cell);
            if (!command && !codeShaped && !bareName && !(upper && typeTable)) continue;
            const dot = cell.lastIndexOf(".");
            const names = command ? commandKeys(cell) : nameKeys(cell, cell.slice(dot + 1));
            out.push({
                kind: command ? "command" : typeTable && upper ? "type" : first,
                source: "table",
                signature: cell,
                names: uniq([...names, ...aliases]),
            });
            continue;
        }
        const keywords = keywordNames(cell);
        if (keywords && typeTable) {
            out.push({
                kind: "type",
                source: "table",
                signature: cell,
                names: uniq([...keywords, ...aliases]),
            });
        }
    }
    return out;
}

/**
 * The name a query refers to: "date_trunc('day', ts)" → "date_trunc",
 * "BIGINT" → "bigint", "  .timer " → ".timer". Same reduction as serened's
 * CallName — cut at the first "(" when everything before it is a name —
 * but only for a call alone (or still being typed): "LIST(INTEGER): live
 * documents per term" is a sentence that starts with a type, not a lookup.
 */
export function lookupKey(q: string): string {
    const t = q.trim();
    const paren = t.indexOf("(");
    if (paren > 0) {
        const head = t.slice(0, paren).trimEnd();
        if (/^[A-Za-z0-9_.$]+$/.test(head)) {
            let depth = 0;
            let close = -1;
            for (let i = paren; i < t.length && close < 0; i++) {
                if (t[i] === "(") depth++;
                else if (t[i] === ")" && --depth === 0) close = i;
            }
            if (close < 0 || t.slice(close + 1).trim().length <= 1) return head.toLowerCase();
        }
    }
    return t.replace(/\s+/g, " ").toLowerCase();
}
