---
layout: docu
redirect_from:
- /docs/guides/import/s3_import
- /docs/guides/network_cloud_storage/s3_import
- /docs/preview/guides/network_cloud_storage/s3_import
- /docs/stable/guides/network_cloud_storage/s3_import
title: S3 Parquet Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## Credentials and Configuration

To read data from S3, set up the credentials and S3 region:

<SqlLogicTest id="cookbook/network_cloud_storage/s3_import/example_003" />

<DocCallout type="tip">

If you get an IO Error (`Connection error for HTTP HEAD`), configure the endpoint explicitly via `ENDPOINT 's3.⟨your-region⟩.amazonaws.com'`.

</DocCallout>

## Querying

Once the S3 configuration is set correctly, Parquet files can be read from S3 using the following command:

<SqlLogicTest id="cookbook/network_cloud_storage/s3_import/example_005" />

## Google Cloud Storage (GCS) and Cloudflare R2

SereneDB can also handle Google Cloud Storage (GCS) and Cloudflare R2 via the S3 API.
See the relevant guides for details.
