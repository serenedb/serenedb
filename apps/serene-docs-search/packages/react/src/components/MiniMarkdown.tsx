import React, { type ReactNode } from "react";

/**
 * Tiny markdown renderer for streamed AI answers: headings, paragraphs,
 * fenced code (with lightweight syntax highlighting), inline code, bold,
 * links, lists and [n] citation chips. Deliberately not a full parser —
 * answers are short and the format is prompt-controlled.
 */
export function MiniMarkdown({
    text,
    onCitation,
}: {
    text: string;
    onCitation?: (n: number) => void;
}): React.ReactElement {
    const blocks = splitBlocks(text);
    return (
        <>
            {blocks.map((b, i) =>
                b.type === "code" ? (
                    <pre key={i}>
                        <code>{highlightCode(b.text, b.lang)}</code>
                    </pre>
                ) : b.type === "list" ? (
                    <ul key={i}>
                        {b.items!.map((item, j) => (
                            <li key={j}>{renderInline(item, onCitation)}</li>
                        ))}
                    </ul>
                ) : b.type === "h" ? (
                    <div key={i} className="sds-md-h" data-level={b.level}>
                        {renderInline(b.text, onCitation)}
                    </div>
                ) : (
                    <p key={i}>{renderInline(b.text, onCitation)}</p>
                ),
            )}
        </>
    );
}

interface Block {
    type: "p" | "code" | "list" | "h";
    text: string;
    items?: string[];
    lang?: string;
    level?: number;
}

function splitBlocks(text: string): Block[] {
    const blocks: Block[] = [];
    // split on fence markers, capturing the info string ("```sql" -> "sql")
    const parts = text.split(/```([^\n]*)\n?/);
    for (let i = 0; i < parts.length; i += 2) {
        const seg = parts[i];
        if ((i / 2) % 2 === 1) {
            // between an opening and a closing fence
            const code = seg.replace(/\n$/, "");
            if (code.trim())
                blocks.push({ type: "code", text: code, lang: (parts[i - 1] ?? "").trim() });
            continue;
        }
        for (const para of seg.split(/\n{2,}/)) {
            let lines = para.split("\n").filter((l) => l.trim());
            // leading heading lines become their own blocks
            while (lines.length > 0) {
                const h = /^\s*(#{1,6})\s+(.*)$/.exec(lines[0]);
                if (!h) break;
                blocks.push({ type: "h", text: h[2].replace(/\s*#+\s*$/, ""), level: h[1].length });
                lines = lines.slice(1);
            }
            if (!lines.length) continue;
            const listItems = lines
                .filter((l) => /^\s*([-*+]|\d+\.)\s+/.test(l))
                .map((l) => l.replace(/^\s*([-*+]|\d+\.)\s+/, ""));
            if (listItems.length === lines.length && listItems.length > 0) {
                blocks.push({ type: "list", text: "", items: listItems });
            } else {
                blocks.push({ type: "p", text: lines.join(" ") });
            }
        }
    }
    return blocks;
}

function renderInline(text: string, onCitation?: (n: number) => void): ReactNode[] {
    // order matters: code spans first so other syntax inside them survives
    const out: ReactNode[] = [];
    const re = /(`[^`]+`)|(\*\*[^*]+\*\*)|(\[(\d+)\])|(\[([^\]]+)\]\(([^)]+)\))/g;
    let last = 0;
    let m: RegExpExecArray | null;
    let key = 0;
    while ((m = re.exec(text))) {
        if (m.index > last) out.push(text.slice(last, m.index));
        if (m[1]) {
            out.push(<code key={key++}>{m[1].slice(1, -1)}</code>);
        } else if (m[2]) {
            out.push(<strong key={key++}>{m[2].slice(2, -2)}</strong>);
        } else if (m[3]) {
            const n = Number(m[4]);
            out.push(
                <button
                    key={key++}
                    type="button"
                    className="sds-cite"
                    onClick={() => onCitation?.(n)}
                >
                    [{n}]
                </button>,
            );
        } else if (m[5]) {
            out.push(
                <a key={key++} href={m[7]} target="_blank" rel="noreferrer">
                    {m[6]}
                </a>,
            );
        }
        last = m.index + m[0].length;
    }
    if (last < text.length) out.push(text.slice(last));
    return out;
}

/* ---------------- fenced-code highlighting ---------------- */

const KEYWORDS = new Set(
    (
        "select from where create table index insert into values update set delete join left right inner outer on group by order limit offset and or not null as with using primary key drop alter add column if exists distinct union all having in is like ilike between case when then else end true false copy to format returning explain analyze vacuum refresh begin commit rollback grant revoke function returns language cast desc asc " +
        "varchar integer int float double boolean date timestamp text serial bigint smallint real numeric decimal array json variant blob interval " +
        "const let var return import export default new class extends async await for while do break continue typeof instanceof try catch throw"
    ).split(" "),
);

const TOKEN_RE =
    /(--[^\n]*|\/\/[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*")|(\b\d+(?:\.\d+)?\b)|(\b[A-Za-z_][A-Za-z0-9_]*\b)/g;

/**
 * Dependency-free token pass good enough for docs answers (SQL and friends):
 * comments, strings, numbers and a keyword set; everything else stays plain.
 */
export function highlightCode(code: string, _lang?: string): ReactNode[] {
    const out: ReactNode[] = [];
    let last = 0;
    let m: RegExpExecArray | null;
    let key = 0;
    TOKEN_RE.lastIndex = 0;
    while ((m = TOKEN_RE.exec(code))) {
        if (m.index > last) out.push(code.slice(last, m.index));
        if (m[1]) {
            out.push(
                <span key={key++} className="sds-tok-c">
                    {m[1]}
                </span>,
            );
        } else if (m[2]) {
            out.push(
                <span key={key++} className="sds-tok-s">
                    {m[2]}
                </span>,
            );
        } else if (m[3]) {
            out.push(
                <span key={key++} className="sds-tok-n">
                    {m[3]}
                </span>,
            );
        } else if (m[4]) {
            if (KEYWORDS.has(m[4].toLowerCase())) {
                out.push(
                    <span key={key++} className="sds-tok-k">
                        {m[4]}
                    </span>,
                );
            } else {
                out.push(m[4]);
            }
        }
        last = m.index + m[0].length;
    }
    if (last < code.length) out.push(code.slice(last));
    return out;
}
