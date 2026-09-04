---
layout: docu
redirect_from:
- /docs/guides/import/http_import
- /docs/guides/network_cloud_storage/http_import
- /docs/preview/guides/network_cloud_storage/http_import
- /docs/stable/guides/network_cloud_storage/http_import
title: HTTP Parquet Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB can read Parquet files over `http(s)`:

<SqlLogicTest id="cookbook/network_cloud_storage/http_import/example_003" />

For example:

<SqlLogicTest id="cookbook/network_cloud_storage/http_import/example_004" />

Moreover, the `read_parquet` function itself can also be omitted thanks to the replacement scan mechanism:

<SqlLogicTest id="cookbook/network_cloud_storage/http_import/example_005" />
