---
layout: docu
redirect_from:
- /docs/guides/import/s3_iceberg_import
- /docs/guides/network_cloud_storage/s3_iceberg_import
- /docs/preview/guides/network_cloud_storage/s3_iceberg_import
- /docs/stable/guides/network_cloud_storage/s3_iceberg_import
selected: S3 Iceberg Import
title: S3 Iceberg Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Credentials

To read Iceberg data from S3, set up the credentials and S3 region. You may either use an access key and secret, or a token.

<SqlLogicTest id="cookbook/network_cloud_storage/s3_import_iceberg/example_003" />

## Loading Iceberg Tables from S3

Once the S3 credentials are configured, Iceberg tables can be read from S3 using the following command:

<SqlLogicTest id="cookbook/network_cloud_storage/s3_import_iceberg/example_005" />

Note that you need to link directly to the manifest file. Otherwise, you'll get an error like this:

```console
IO Error:
Cannot open file "s3://bucket/iceberg_table_folder/metadata/version-hint.text": No such file or directory
```
