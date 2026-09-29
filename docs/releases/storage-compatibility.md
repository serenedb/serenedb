---
title: Storage compatibility
split: page
---

# Storage compatibility

This page describes which SereneDB releases can read search indexes, meaning inverted indexes and search tables, written by other releases.

- **Newer release, older index:** a release reads indexes written by earlier releases.
- **Older release, newer index:** an older release reads an index written by a newer one as long as the index uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A server that refuses an index does not start; its log names the index file and both storage versions, and the index files are left as they are.
- **Breaks:** a release that can't read indexes from earlier releases says so in its release notes, together with the steps to move affected indexes and search tables to it.
- **Integrity:** every index file records CRC32C checksums. The metadata of each file is verified whenever the file is opened, and a damaged file fails to open instead of returning wrong results.

For the meaning of version numbers and release lines, see [Versioning](./versioning.md).
