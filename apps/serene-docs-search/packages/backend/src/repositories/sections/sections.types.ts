import type { SectionKind } from "@serenedb/docs-search-core";

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
    /** sha256 of the rendered content — snapshot skip/prune key. */
    hash: string;
}

export interface IndexStats {
    sections: number;
    documents: number;
}
