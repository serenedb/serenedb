---
layout: docu
title: HTTP Compression
split: page
---

The HTTP listener (`--listen 'http://…?api=…'`) compresses response bodies
when the client asks for one of the codings below, and decompresses request
bodies sent with one of them, for every endpoint. There is nothing to
configure: every coding is always available in both directions, and a request
that asks for none is answered uncompressed.

| Coding | Token | Notes |
|---|---|---|
| Zstandard | `zstd` | best ratio, and fast; the default pick |
| Brotli | `br` | supported by every modern browser; compresses at quality 5 |
| gzip | `gzip` | zlib-ng; understood by every HTTP client and browser |
| ZXC | `zxc` | [serenedb/zxc](https://github.com/serenedb/zxc), the fastest decode; not an IANA-registered coding, so only clients that opt in ask for it |
| LZ4 frame | `lz4` | fastest to compress; same caveat as `zxc` |
| Snappy | `snappy` | the raw block format, as Prometheus remote write sends it; a body is compressed or decompressed whole, so a streamed response is sent only once it is complete |

Server preference is the order above: `zstd`, `br`, `gzip`, `zxc`, `lz4`,
`snappy`. It picks
between codings the client accepts equally — the client's own `q` weights come
first, so `Accept-Encoding: gzip;q=1.0, zstd;q=0.1` answers gzip.

A response is compressed when all of these hold:

- the client's `Accept-Encoding` asks for one of them (`q` weights and `*` are
  honoured; a coding the client did not list is not used);
- the response has a body: not `HEAD`, not `1xx`/`204`/`304`;
- a fixed-length body is at least 1 KiB (streamed bodies are always
  compressed, and are re-framed as `Transfer-Encoding: chunked` since their
  compressed length is unknown up front).

An encoding that does not make the body smaller is dropped, and the body is
sent as-is.

Compressed responses carry `Content-Encoding: <token>` and
`Vary: Accept-Encoding`.

## Compressed request bodies

A request body sent with `Content-Encoding` is decompressed once the request
is authenticated, before it reaches the endpoint, so every API accepts
compressed bodies the same way. The field may list up to two codings in the
order they were applied (`Content-Encoding: gzip, zstd`); `identity` is
ignored, and the tokens are case-insensitive. A decompressed body is held to
the same size limit as an uncompressed one (64 MiB). A `zstd` body must use a
window of at most 8 MiB, the limit RFC 8878 sets for HTTP: levels 1 to 19
fit, while `--ultra` levels and frames made with `--long` or a larger
`--window` are rejected.

## Rejected requests

| `Accept-Encoding` | Response |
|---|---|
| absent, empty, or asking only for codings we do not have (`compress`) | `200`, uncompressed |
| `identity;q=0` or `*;q=0`, with no coding we have left acceptable | `406 Not Acceptable` — nothing can be sent |
| a weight that is not a qvalue (`gzip;q=huh`, `gzip;q=2`, `gzip;q=nan`) | `400 Bad Request` |

| `Content-Encoding` | Response |
|---|---|
| a coding we do not have (`compress`), or more than two codings | `415 Unsupported Media Type` |
| a body that is corrupt or truncated for its coding, or a `zstd` window above 8 MiB | `400 Bad Request` |
| a body that decompresses past the body size limit | `413 Content Too Large` |

These errors are answered after authentication and before the endpoint runs, as
`{"error": "<reason>"}`, and the connection stays usable for the next
request.

## Limitations

- **`HEAD` is never compressed.** A `HEAD` answers with the uncompressed
  `Content-Length` and no `Content-Encoding`, while the matching `GET` would
  answer chunked. RFC 9110 §9.3.2 permits omitting header fields whose value
  is determined only while generating the content, but a client must not
  size a resource with `HEAD` and expect the `GET` body to match.
- **Streamed bodies are delivered in codec-sized blocks.** The encoders
  buffer internally, so a handler's individual `Write()` calls no longer
  reach the client one by one: bytes appear when the codec's block fills or
  when the response finishes. An endpoint needing incremental delivery
  (long-polling, server-sent events) would have to bypass the encoder; there
  is no API for that yet.
