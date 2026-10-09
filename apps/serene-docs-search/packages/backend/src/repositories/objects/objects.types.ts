export type DocObjectKind = "function" | "statement" | "command" | "setting" | "type" | "name";

/** Something a reference documents by name (see services/parsing/objects.ts). */
export interface DocObject {
    kind: DocObjectKind;
    /** A heading that is the name, or a row of a reference table. */
    source: "heading" | "table";
    /** As written in the docs: "date_trunc(part, date)", "BIGINT", ".timer". */
    signature: string;
    /** Lowercased lookup keys: name, qualified name, aliases. */
    names: string[];
}

/** One catalog row: a lookup key and the section documenting it. */
export interface CatalogEntry {
    name: string;
    sectionId: string;
    kind: DocObjectKind;
    signature: string;
    source: DocObject["source"];
    /** Heading level of the documenting section (1 = the page itself). */
    level: number;
}
