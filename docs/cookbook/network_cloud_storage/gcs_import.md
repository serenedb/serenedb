---
layout: docu
redirect_from:
- /docs/guides/import/gcs_import
- /docs/guides/network_cloud_storage/gcs_import
- /docs/preview/guides/network_cloud_storage/gcs_import
- /docs/stable/guides/network_cloud_storage/gcs_import
title: Google Cloud Storage Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB can read and write Google Cloud Storage (GCS) over HTTP(S) directly.

## Credentials and Configuration

You need to create [HMAC keys](https://console.cloud.google.com/storage/settings;tab=interoperability) and declare them:

<SqlLogicTest id="cookbook/network_cloud_storage/gcs_import/example_001" />

## Querying

After setting up the GCS credentials, you can query the GCS data using:

<SqlLogicTest id="cookbook/network_cloud_storage/gcs_import/example_002" />

## Attaching to a Database

You can attach to a database file in read-only mode:

<SqlLogicTest id="cookbook/network_cloud_storage/gcs_import/example_003" />

> Databases in Google Cloud Storage can only be attached in read-only mode.
