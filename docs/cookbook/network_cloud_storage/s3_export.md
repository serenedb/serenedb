---
layout: docu
redirect_from:
- /docs/guides/import/s3_export
- /docs/guides/network_cloud_storage/s3_export
- /docs/preview/guides/network_cloud_storage/s3_export
- /docs/stable/guides/network_cloud_storage/s3_export
title: S3 Parquet Export
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

To write data to S3, set up the credentials. Note that the `region` parameter should match the region of the bucket you want to access.

<SqlLogicTest id="cookbook/network_cloud_storage/export_s3/example_003" />

<DocCallout type="tip">

If you get an IO Error (`Connection error for HTTP HEAD`), configure the endpoint explicitly via `ENDPOINT 's3.⟨your-region⟩.amazonaws.com'`.

</DocCallout>

SereneDB cannot read the AWS credential chain (`PROVIDER credential_chain`): that provider comes from DuckDB's `aws` extension, which is not built in. Pass keys as above, or use one of the other methods in [AWS Credentials](../../configuration/aws_credentials.md).

Once the S3 credentials are configured, Parquet files can be written to S3 using the following command:

<SqlLogicTest id="cookbook/network_cloud_storage/export_s3/example_005" />

Similarly, Google Cloud Storage (GCS) is supported through the Interoperability API.
You need to create [HMAC keys](https://console.cloud.google.com/storage/settings;tab=interoperability) and provide the credentials as follows:

<SqlLogicTest id="cookbook/network_cloud_storage/export_s3/example_006" />

After setting up the GCS credentials, you can export using:

<SqlLogicTest id="cookbook/network_cloud_storage/export_s3/example_007" />
