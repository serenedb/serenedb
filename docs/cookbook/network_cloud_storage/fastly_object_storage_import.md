---
layout: docu
redirect_from:
- /docs/guides/import/fastly_object_storage_import
- /docs/guides/network_cloud_storage/fastly_object_storage_import
- /docs/preview/guides/network_cloud_storage/fastly_object_storage_import
- /docs/stable/guides/network_cloud_storage/fastly_object_storage_import
title: Fastly Object Storage Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

For Fastly Object Storage, the [S3 Compatibility API](https://docs.fastly.com/products/object-storage) lets SereneDB read and write from Fastly buckets over HTTP(S) directly.

## Credentials and Configuration

You will need to [generate an S3 auth token](https://docs.fastly.com/en/guides/working-with-object-storage#creating-an-object-storage-access-key) and create an `S3` secret in SereneDB:

<SqlLogicTest id="cookbook/network_cloud_storage/fastly_object_storage_import/example_001" />

* The `ENDPOINT` needs to point to the [Fastly endpoint for the region](https://docs.fastly.com/en/guides/working-with-object-storage#working-with-the-s3-compatible-api) you want to use (e.g., `eu-central.object.fastlystorage.app`).
* `REGION` must use the same region mentioned in `ENDPOINT`.
* `URL_STYLE` needs to use `path`.

## Querying

After setting up the Fastly Object Storage credentials, you can query the data there using SereneDB's built-in methods, such as `read_csv` or `read_parquet`:

<SqlLogicTest id="cookbook/network_cloud_storage/fastly_object_storage_import/example_002" />
