import type { SectionKind } from "@serenedb/docs-search-core";
import type { DocObject } from "@repositories/objects";

/** One indexable unit: a heading-anchored chunk of a document. */
export interface Section {
    /** Stable id: sha1(path + anchor + ordinal). */
    id: string;
    /** Source path (repo-relative file path or crawled page URL). */
    path: string;
    /** Site URL the section resolves to (filled by urlmap; crawler sets directly). */
    url: string;
    anchor?: string;
    title: string;
    /** "Docs › Replication › Read replicas" */
    crumb: string;
    /** Top-level bucket for result grouping, e.g. "Replication". */
    group: string;
    kind: SectionKind;
    /** Heading level the section is anchored at (0 = whole document). */
    level: number;
    content: string;
    /** Concatenated code blocks of the section — ngram-indexed for snippet paste. */
    code: string;
    /** Page title and the headings above this one: "Full-Text Search › Prefix". */
    trail: string;
    /**
     * What full-text matching reads: the section's text plus every section
     * nested under it (a page's top section carries the whole page). It
     * also feeds snippets and Ask AI context; `content` stays the section's
     * own text (embeddings, page reads).
     */
    body: string;
    /** Catalog objects this section documents (not stored in the row). */
    objects: DocObject[];
    /** Page URLs the page links to — on its first section only, not stored. */
    links?: string[];
    /** sha256 of the rendered content — snapshot skip/prune key. */
    hash: string;
}

export interface IndexStats {
    sections: number;
    documents: number;
}
