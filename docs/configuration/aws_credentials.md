---
title: AWS Credentials
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB can read and write AWS resources — files in S3, and Iceberg tables in the [S3 Tables and Glue catalogs](./iceberg_authentication.md#aws-sigv4-s3-tables-and-glue). Unlike Google Cloud, AWS does not use expiring bearer tokens for these services: every request is individually signed ([Signature Version 4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html)) with a static key pair, so there is nothing to mint or renew — the credential you configure is used as-is, for both the catalog plane and the data files. You configure it once, through the [Secrets manager](./secrets_manager.md).

## Which credential should I use?

| Your situation                                                    | Use                                                        |
| ------------------------------------------------------------------ | ----------------------------------------------------------- |
| Production                                                          | [Access keys](#access-keys) of a dedicated least-privilege IAM identity |
| Temporary credentials from STS / an assumed role                   | [Session tokens](#session-tokens)                           |
| S3-compatible stores (MinIO, Cloudflare R2, Tigris, ...)           | [Custom endpoints](#s3-compatible-stores), or the dedicated [`r2` secret type](../cookbook/network_cloud_storage/cloudflare_r2_import.md) |

<DocCallout type="attention">
    Automatic credential discovery (<code>PROVIDER credential_chain</code> — environment variables, SSO sessions, EC2 instance roles) comes from DuckDB's <code>aws</code> extension, which SereneDB does not currently bundle; attempting it fails with <em>"Secret provider 'credential_chain' for type 's3' does not exist"</em>. Configure explicit keys as shown below.
</DocCallout>

## Access keys

The standard AWS credential: an access key id (`AKIA...`) and a secret access key, belonging to an IAM identity. For a database, create a **dedicated IAM user** so the keys survive employee departures and can be scoped to exactly what SereneDB needs:

```bash
# 1. Create the user
aws iam create-user --user-name serenedb-engine

# 2. Grant it access — attach a least-privilege policy (see below)
aws iam attach-user-policy --user-name serenedb-engine \
    --policy-arn arn:aws:iam::123456789012:policy/serenedb-engine-access

# 3. Create the key pair (the secret is shown exactly once — store it safely)
aws iam create-access-key --user-name serenedb-engine
```

For the policy, start from AWS's managed policies and narrow down: [`AmazonS3TablesReadOnlyAccess` / `AmazonS3TablesFullAccess`](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-iam-awsmanpol-s3tables.html) for S3 Tables, [Glue's IAM actions](https://docs.aws.amazon.com/glue/latest/dg/security-iam.html) for the Glue catalog, and plain [`s3:GetObject`/`s3:PutObject`/`s3:ListBucket`](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html) on the specific buckets for data files.

Configure the keys as an `S3` secret — `REGION` is required (Iceberg catalog attaches derive their endpoint region from it):

<SqlLogicTest id="configuration/aws_credentials/example_config" />

**Production-friendly**, with the usual static-key caveats: the key never expires on its own, so [rotate it](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_RotateAccessKey) on your schedule and delete keys of decommissioned deployments.

## Session tokens

If your organization issues only **temporary credentials** — from [STS](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html), an assumed role, or SSO tooling — they come as a triple that includes a session token:

<SqlLogicTest id="configuration/aws_credentials/example_session" />

<DocCallout type="attention">
    Temporary credentials expire (typically after 1–12 hours) and SereneDB cannot renew them — when they lapse, the secret must be re-created with fresh ones. Fine for interactive sessions and testing; for unattended production use, prefer a dedicated IAM user's permanent keys.
</DocCallout>

## S3-compatible stores

The same `S3` secret type speaks to any S3-compatible service — point it at the service's endpoint:

<SqlLogicTest id="configuration/aws_credentials/example_compatible" />

Cookbooks with service-specific settings: [Cloudflare R2](../cookbook/network_cloud_storage/cloudflare_r2_import.md), [Tigris](../cookbook/network_cloud_storage/tigris_import.md), [Fastly](../cookbook/network_cloud_storage/fastly_object_storage_import.md).

## Scoping secrets to paths

When different buckets need different credentials, give each secret a `SCOPE` — it is matched against data-file paths, longest prefix first:

<SqlLogicTest id="configuration/aws_credentials/example_scope" />

## Putting a credential to work

To attach an S3 Tables or Glue Iceberg catalog with the secret, see [Iceberg catalog authentication](./iceberg_authentication.md#aws-sigv4-s3-tables-and-glue). To read and write plain files, see [S3 Import](../cookbook/network_cloud_storage/s3_import.md), [S3 Export](../cookbook/network_cloud_storage/s3_export.md), and [S3 Iceberg Import](../cookbook/network_cloud_storage/s3_iceberg_import.md).
