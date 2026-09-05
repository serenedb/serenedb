---
layout: docu
redirect_from:
- /docs/guides/import/s3_express_one
- /docs/guides/network_cloud_storage/s3_express_one
- /docs/preview/guides/network_cloud_storage/s3_express_one
- /docs/stable/guides/network_cloud_storage/s3_express_one
title: S3 Express One
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

In late 2023, AWS [announced](https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-s3-express-one-zone-storage-class/) the [S3 Express One Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-one-zone.html), a high-speed variant of traditional S3 buckets.
SereneDB can read S3 Express One buckets over HTTP(S) directly.

## Credentials and Configuration

The configuration of S3 Express One buckets is similar to regular S3 buckets with one exception:
you must specify the endpoint according to the following pattern:

<SqlLogicTest id="cookbook/network_cloud_storage/s3_express_one/example_001" />

where the `⟨availability_zone⟩`{:.language-sql .highlight} (e.g., `use1-az5`) can be obtained from the S3 Express One bucket's configuration page and the `⟨region⟩`{:.language-sql .highlight} is the AWS region (e.g., `us-east-1`).

For example, to allow SereneDB to use an S3 Express One bucket, configure the Secrets manager as follows:

<SqlLogicTest id="cookbook/network_cloud_storage/s3_express_one/example_002" />

## Instance Location

For best performance, ensure the EC2 instance is in the same availability zone as the S3 Express One bucket you are querying.
To determine the mapping between zone names and zone IDs, use the `aws ec2 describe-availability-zones` command.

* Zone name to zone ID mapping:

  ```bash
  aws ec2 describe-availability-zones --output json \
      | jq -r '.AvailabilityZones[] | select(.ZoneName == "us-east-1f") | .ZoneId'
  ```

  ```text
  use1-az5
  ```

* Zone ID to zone name mapping:

  ```bash
  aws ec2 describe-availability-zones --output json \
      | jq -r '.AvailabilityZones[] | select(.ZoneId == "use1-az5") | .ZoneName'
  ```

  ```text
  us-east-1f
  ```

## Querying

You can query the S3 Express One bucket like any other S3 bucket:

<SqlLogicTest id="cookbook/network_cloud_storage/s3_express_one/example_003" />

## Performance

The following experiments were run on a `c7gd.12xlarge` instance using the LDBC SF300 Comments `creationDate` Parquet file.

| Experiment | File size | Runtime |
|:-----|--:|--:|
| Loading only from Parquet | 4.1 GB | 3.5 s |
| Creating local table from Parquet | 4.1 GB | 5.1 s |

The “loading only” variant is running the load as part of an [`EXPLAIN ANALYZE`](../performance/profiling.md#the-explain-analyze-statement) statement to measure the runtime without actually creating a local table, while the “creating local table” variant uses `CREATE TABLE ... AS SELECT` to create a persistent table on the local disk.
