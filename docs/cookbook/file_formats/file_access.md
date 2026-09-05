---
layout: docu
redirect_from:
- /docs/guides/file_formats/file_access
- /docs/preview/guides/file_formats/file_access
- /docs/stable/guides/file_formats/file_access
title: 'File Access with the file: Protocol'
---

SereneDB supports using the `file:` protocol. It currently supports the following formats:

* `file:/some/path` (host omitted completely)
* `file:///some/path` (empty host)
* `file://localhost/some/path` (`localhost` as host)

Note that the following formats are *not* supported because they are non-standard:

* `file:some/relative/path` (relative path)
* `file://some/path` (double-slash path)

Additionally, the `file:` protocol currently does not support remote (non-localhost) hosts.
