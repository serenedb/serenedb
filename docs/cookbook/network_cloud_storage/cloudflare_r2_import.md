---
layout: docu
redirect_from:
- /docs/guides/import/cloudflare_r2_import
- /docs/guides/network_cloud_storage/cloudflare_r2_import
- /docs/preview/guides/network_cloud_storage/cloudflare_r2_import
- /docs/stable/guides/network_cloud_storage/cloudflare_r2_import
title: Cloudflare R2 Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

For Cloudflare R2, the [S3 Compatibility API](https://developers.cloudflare.com/r2/api/s3/api/) allows you to use SereneDB's built-in S3 support to read and write from R2 buckets.

## Credentials and Configuration

You will need to [generate an S3 auth token](https://developers.cloudflare.com/r2/api/s3/tokens/) and create an `R2` secret in SereneDB:

<SqlLogicTest id="cookbook/network_cloud_storage/cloudflare_r2_import/example_001" />

## Querying

After setting up the R2 credentials, you can query the R2 data using SereneDB's built-in methods, such as `read_csv` or `read_parquet`:

<SqlLogicTest id="cookbook/network_cloud_storage/cloudflare_r2_import/example_002" />
