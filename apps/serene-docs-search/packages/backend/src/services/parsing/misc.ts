import { classifyKind, slugify, type RawSection } from "./section";

/** .txt — one section per file. */
export function parseText(raw: string): RawSection[] {
    const content = raw.replace(/\r\n/g, "\n").trim();
    if (!content) return [];
    return [{ title: "", kind: "text", level: 0, content }];
}

/**
 * .rst — split on reStructuredText section headers (a line underlined with
 * = - ~ ^ " etc.). Anchors follow the docutils convention (lowercase, dashes).
 */
export function parseRst(raw: string): RawSection[] {
    const lines = raw.replace(/\r\n/g, "\n").split("\n");
    const sections: RawSection[] = [];
    let title = "";
    let anchor: string | undefined;
    let level = 0;
    let buf: string[] = [];
    const seenAdornments: string[] = [];

    const flush = () => {
        const content = buf.join("\n").replace(/\n{3,}/g, "\n\n").trim();
        buf = [];
        if (!title && !content) return;
        sections.push({ anchor, title, kind: classifyKind(title, content, level), level, content });
    };

    for (let i = 0; i < lines.length; i++) {
        const text = lines[i];
        const under = lines[i + 1];
        const isHeader =
            text.trim().length > 0 &&
            under != null &&
            /^([=\-~^"'`#*+.:_])\1{2,}\s*$/.test(under) &&
            under.trim().length >= Math.min(text.trim().length, 4);
        if (isHeader) {
            flush();
            title = text.trim();
            anchor = slugify(title);
            const ch = under.trim()[0];
            let idx = seenAdornments.indexOf(ch);
            if (idx < 0) {
                seenAdornments.push(ch);
                idx = seenAdornments.length - 1;
            }
            level = Math.min(idx + 1, 3);
            i++; // skip the underline
        } else {
            buf.push(text);
        }
    }
    flush();
    return sections;
}

interface NotebookCell {
    cell_type: string;
    source: string | string[];
}

/** .ipynb — markdown cells contribute headings/prose, code cells become code sections. */
export function parseNotebook(raw: string): RawSection[] {
    let nb: { cells?: NotebookCell[] };
    try {
        nb = JSON.parse(raw) as { cells?: NotebookCell[] };
    } catch {
        return [];
    }
    const sections: RawSection[] = [];
    let title = "";
    let anchor: string | undefined;
    let level = 0;
    let buf: string[] = [];
    let codeBuf: string[] = [];

    const flush = () => {
        const content = buf.join("\n\n").trim();
        const code = codeBuf.join("\n\n").trim();
        buf = [];
        codeBuf = [];
        if (!title && !content) return;
        sections.push({
            anchor,
            title,
            kind: classifyKind(title, content, level),
            level,
            content,
            code: code || undefined,
        });
    };

    for (const cell of nb.cells ?? []) {
        const src = Array.isArray(cell.source) ? cell.source.join("") : (cell.source ?? "");
        if (cell.cell_type === "markdown") {
            for (const line of src.split("\n")) {
                const h = /^(#{1,3})\s+(.+?)\s*$/.exec(line);
                if (h) {
                    flush();
                    level = h[1].length;
                    title = h[2].trim();
                    anchor = slugify(title);
                } else if (line.trim()) {
                    buf.push(line.trim());
                }
            }
        } else if (cell.cell_type === "code" && src.trim()) {
            buf.push(src.trim());
            codeBuf.push(src.trim());
        }
    }
    flush();
    return sections;
}
