import { savePendingHighlight, takePendingHighlight } from "./storage";

/**
 * DocSearch-style landing: scroll to the anchored section, flash it, and
 * paint the query terms via the CSS Custom Highlight API (no DOM mutation).
 */

const FLASH_CLASS = "sds-target-flash";
const HIGHLIGHT_NAME = "sds-term";
const FIND_TIMEOUT_MS = 4000;
/** One lifetime for both effects: the section flash and the term paint. */
const HIGHLIGHT_MS = 2400;

export function rememberHighlight(url: string, anchor: string | undefined, query: string): void {
    savePendingHighlight({ url, anchor, terms: tokenizeQuery(query) });
}

/** Try to apply a pending highlight on the current page (SPA + MPA safe). */
export function applyPendingHighlight(): void {
    const pending = takePendingHighlight();
    if (!pending) return;
    void highlightOnPage(pending.anchor, pending.terms);
}

export async function highlightOnPage(
    anchor: string | undefined,
    terms: string[],
): Promise<void> {
    const target = anchor ? await waitForElement(anchor) : null;
    if (target) {
        target.scrollIntoView({ block: "start" });
        flash(sectionContainer(target));
    }
    if (terms.length > 0) {
        paintTerms(target ?? document.body, terms);
    }
}

function sectionContainer(el: Element): Element {
    // headings flash better together with their immediate block
    return el.closest("section, article > *")?.parentElement ? el : el;
}

async function waitForElement(id: string): Promise<Element | null> {
    const started = Date.now();
    for (;;) {
        const el =
            document.getElementById(id) ??
            document.getElementById(decodeURIComponent(id)) ??
            document.querySelector(`[name="${CSS.escape(id)}"]`);
        if (el) return el;
        if (Date.now() - started > FIND_TIMEOUT_MS) return null;
        await new Promise((r) => setTimeout(r, 120));
    }
}

function flash(el: Element): void {
    el.classList.remove(FLASH_CLASS);
    // restart the animation if it was mid-flight
    void (el as HTMLElement).offsetWidth;
    el.classList.add(FLASH_CLASS);
    window.setTimeout(() => el.classList.remove(FLASH_CLASS), HIGHLIGHT_MS);
}

export function tokenizeQuery(query: string): string[] {
    return [
        ...new Set(
            query
                .toLowerCase()
                .split(/[^\p{L}\p{N}_]+/u)
                .filter((t) => t.length >= 2),
        ),
    ].slice(0, 8);
}

interface HighlightRegistry {
    set(name: string, highlight: unknown): void;
    delete(name: string): void;
}

/** Paint terms in the section starting at `from` until the next heading. */
function paintTerms(from: Element, terms: string[]): void {
    const registry =
        typeof CSS !== "undefined"
            ? (CSS as unknown as { highlights?: HighlightRegistry }).highlights
            : undefined;
    const HighlightCtor = (window as unknown as { Highlight?: new (...r: Range[]) => unknown })
        .Highlight;
    if (!registry || !HighlightCtor) return; // no Custom Highlight API — flash only

    const scope = scopeNodes(from);
    const ranges: Range[] = [];
    for (const node of scope) {
        const text = node.textContent?.toLowerCase() ?? "";
        for (const term of terms) {
            let idx = 0;
            while ((idx = text.indexOf(term, idx)) >= 0) {
                const range = document.createRange();
                range.setStart(node, idx);
                range.setEnd(node, idx + term.length);
                ranges.push(range);
                idx += term.length;
                if (ranges.length > 200) break;
            }
        }
    }
    if (ranges.length === 0) return;
    registry.set(HIGHLIGHT_NAME, new HighlightCtor(...ranges));
    window.setTimeout(() => registry.delete(HIGHLIGHT_NAME), HIGHLIGHT_MS);
}

/** Text nodes from the anchor element to the next heading of same-or-higher level. */
function scopeNodes(from: Element): Text[] {
    const isHeading = /^H[1-6]$/.test(from.tagName);
    const stopLevel = isHeading ? Number(from.tagName[1]) : 7;
    const out: Text[] = [];
    collectText(from, out);

    if (isHeading || from.parentElement) {
        let el: Element | null = from.nextElementSibling ?? from.parentElement?.nextElementSibling ?? null;
        let hops = 0;
        while (el && hops < 40) {
            const m = /^H([1-6])$/.exec(el.tagName);
            if (m && Number(m[1]) <= stopLevel) break;
            collectText(el, out);
            el = el.nextElementSibling;
            hops++;
        }
    }
    return out;
}

function collectText(el: Node, out: Text[]): void {
    const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
    let n: Node | null;
    while ((n = walker.nextNode())) {
        if (n.textContent && n.textContent.trim()) out.push(n as Text);
    }
}
