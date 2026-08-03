import type { Source } from "@serenedb/docs-search-core";
import { fetchBucket } from "./bucket";
import { fetchFolder } from "./folder";
import { fetchGit, remoteHead } from "./git";
import { fetchSite } from "./site";
import type { FetchContext, FetchResult } from "./sources.types";

/** Pulls docs from wherever they live (git / folder / site / S3), one common shape. */
export const SourcesService = {
    fetch: (source: Source, ctx: FetchContext): Promise<FetchResult> => {
        switch (source.type) {
            case "git":
                return fetchGit(source, ctx);
            case "folder":
                return fetchFolder(source.path, ctx);
            case "site":
                return fetchSite(source, ctx);
            case "bucket":
                return fetchBucket(source, ctx);
        }
    },

    /** Cheap `git ls-remote` HEAD probe for the commit-watch scheduler. */
    remoteHead,
};
