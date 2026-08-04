import NotConfiguredError from "@utils/errors/notConfiguredError";
import { DbContext } from "./context";

export { DbContext, type DbRuntimeOptions } from "./context";

/*
 * The active context is a swappable module singleton (the serene_site
 * PGClient pattern, adapted for a backend that can be re-configured at
 * runtime): App.applyConfig() builds a fresh DbContext and swaps it in,
 * repositories always read the current one.
 */
let current: DbContext | null = null;

/** Swap the active context (config apply). The caller closes the old one. */
export const setDbContext = (ctx: DbContext | null): void => {
    current = ctx;
};

/** The active context, or null while the backend is unconfigured. */
export const currentDbContext = (): DbContext | null => current;

/** The active context; throws the 409 the API contract expects otherwise. */
export const getDbContext = (): DbContext => {
    if (!current) throw new NotConfiguredError();
    return current;
};
