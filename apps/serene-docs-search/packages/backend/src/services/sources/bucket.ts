import path from "node:path";
import picomatch from "picomatch";
import type { BucketSource } from "@serenedb/docs-search-core";
import type { FetchContext, FetchResult, SourceFile } from "./sources.types";

/**
 * S3-compatible source (AWS, R2, MinIO). Credentials come from the standard
 * AWS env vars / shared config on the backend — never from the browser.
 * The SDK is imported lazily so the backend runs without it for other sources.
 */
export async function fetchBucket(source: BucketSource, ctx: FetchContext): Promise<FetchResult> {
    const { S3Client, ListObjectsV2Command, GetObjectCommand } = await import(
        "@aws-sdk/client-s3"
    );
    const m = /^s3:\/\/([^/]+)\/?(.*)$/.exec(source.uri);
    if (!m) throw new Error(`Invalid bucket URI: ${source.uri} (expected s3://bucket/prefix)`);
    const [, bucket, prefix] = m;

    const client = new S3Client({
        region: source.region || process.env.AWS_REGION || "us-east-1",
        ...(source.endpoint ? { endpoint: source.endpoint, forcePathStyle: true } : {}),
    });

    const wanted = new Set(ctx.extensions.map((e) => e.toLowerCase()));
    const isExcluded = ctx.exclude.length ? picomatch(ctx.exclude, { dot: true }) : () => false;

    const keys: string[] = [];
    let token: string | undefined;
    do {
        const page = await client.send(
            new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix || undefined, ContinuationToken: token }),
        );
        for (const obj of page.Contents ?? []) {
            if (!obj.Key || (obj.Size ?? 0) > 5 * 1024 * 1024) continue;
            const ext = path.extname(obj.Key).toLowerCase();
            const rel = prefix ? obj.Key.slice(prefix.length).replace(/^\//, "") : obj.Key;
            if (wanted.has(ext) && !isExcluded(rel)) keys.push(obj.Key);
        }
        token = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (token);

    const files: SourceFile[] = [];
    for (const key of keys) {
        const res = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const ext = path.extname(key).toLowerCase();
        const encoding = ext === ".pdf" ? "base64" : "utf8";
        const content = await res.Body?.transformToString(encoding === "base64" ? "base64" : "utf-8");
        if (content == null) continue;
        const rel = prefix ? key.slice(prefix.length).replace(/^\//, "") : key;
        files.push({ path: rel, content, encoding, extension: ext });
        ctx.onProgress?.(files.length);
    }
    return { files, ref: `s3:${Date.now()}` };
}
