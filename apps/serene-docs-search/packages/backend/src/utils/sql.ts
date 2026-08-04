/** Escape a string literal for inlining into DDL (params don't work in DDL). */
export function lit(s: string): string {
    return "'" + s.replace(/'/g, "''") + "'";
}

/** Guard identifiers (table/index/dictionary names) coming from config. */
export function ident(s: string): string {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(s)) throw new Error(`Invalid identifier: ${s}`);
    return s;
}
