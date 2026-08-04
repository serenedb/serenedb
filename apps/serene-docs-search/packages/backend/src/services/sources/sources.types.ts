/** A file pulled from any source, ready for parsing. */
export interface SourceFile {
    /** Source-relative path (repo path, folder path or page URL). */
    path: string;
    /** For site crawls: the final page URL (click-through target). */
    url?: string;
    content: string;
    /** "base64" for binary formats (.pdf); "utf8" otherwise. */
    encoding?: "utf8" | "base64";
    extension: string;
}

export interface FetchResult {
    files: SourceFile[];
    /** Version marker of what was fetched (git commit sha, crawl timestamp…). */
    ref?: string;
}

export interface FetchContext {
    /** Scratch dir the source may use (git clones live here). */
    workDir: string;
    extensions: string[];
    exclude: string[];
    onProgress?: (files: number, detail?: string) => void;
}
