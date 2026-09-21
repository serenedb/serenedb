---
layout: docu
title: HTTP Response Compression
split: page
---

The HTTP listener (`--listen 'http://…?api=…'`) compresses response bodies
when the client asks for one of the codings below. There is nothing to
configure: every coding is always available, and a request that asks for
none is answered uncompressed.

| Coding | Token | Notes |
|---|---|---|
| Zstandard | `zstd` | best ratio, and fast; the default pick |
| gzip | `gzip` | zlib-ng; understood by every HTTP client and browser |
| ZXC | `zxc` | [serenedb/zxc](https://github.com/serenedb/zxc), the fastest decode; not an IANA-registered coding, so only clients that opt in ask for it |
| LZ4 frame | `lz4` | fastest to compress; same caveat as `zxc` |

Server preference is the order above: `zstd`, `gzip`, `zxc`, `lz4`. It picks
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
`Vary: Accept-Encoding`. Request bodies are not decompressed.

## Rejected requests

| `Accept-Encoding` | Response |
|---|---|
| absent, empty, or asking only for codings we do not have (`br`) | `200`, uncompressed |
| `identity;q=0` or `*;q=0`, with no coding we have left acceptable | `415 Unsupported Media Type` — nothing can be sent |
| a weight that is not a qvalue (`gzip;q=huh`, `gzip;q=2`) | `400 Bad Request` |

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
