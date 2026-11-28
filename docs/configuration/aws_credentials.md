---
title: AWS Credentials
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB can read and write AWS resources — files in S3, and Iceberg tables in the [S3 Tables and Glue catalogs](./iceberg_authentication.md#aws-sigv4-s3-tables-and-glue). Unlike Google Cloud, AWS does not use expiring bearer tokens for these services: every request is individually signed ([Signature Version 4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html)) with a static key pair, so there is nothing to mint or renew — the credential you configure is used as-is, for both the catalog plane and the data files. You configure it once, through the [Secrets manager](./secrets_manager.md).

## Which credential should I use?

| Your situation                                                    | Use                                                        |
| ------------------------------------------------------------------ | ----------------------------------------------------------- |
| Production                                                          | [Access keys](#access-keys) of a dedicated least-privilege IAM identity |
| A server running on AWS: EC2, EKS, ECS                              | [The server's own credentials](#the-servers-own-credentials) |
| Temporary credentials from STS / an assumed role                   | [Session tokens](#session-tokens)                           |
| S3-compatible stores (MinIO, Cloudflare R2, Tigris, ...)           | [Custom endpoints](#s3-compatible-stores), or the dedicated [`r2` secret type](../cookbook/network_cloud_storage/cloudflare_r2_import.md) |

<DocCallout type="attention">
    <code>PROVIDER credential_chain</code> is not available for <code>s3</code> secrets: creating such a secret fails with <em>Secret provider "credential_chain" for type "s3" does not exist in SereneDB</em>. Configure explicit keys as shown below, or rely on <a href="#the-servers-own-credentials">the server's own credentials</a>.
</DocCallout>

## The server's own credentials

Wherever neither a secret nor the `s3_access_key_id` and `s3_secret_access_key` settings name keys, the server signs `s3://` requests with the credentials of its environment, found the way AWS tools find them:

1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`.
2. A web identity: `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`, as EKS sets them for IAM roles for service accounts; the server exchanges the token for credentials with STS in the `AWS_REGION`.
3. The container credentials endpoint: `AWS_CONTAINER_CREDENTIALS_FULL_URI` or `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, with `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or `AWS_CONTAINER_AUTHORIZATION_TOKEN`, as EKS Pod Identity and ECS set them.
4. The EC2 instance metadata service, unless `AWS_EC2_METADATA_DISABLED` is `true`.

Credentials that expire are fetched again five minutes before they do, so a server keeps working on them for as long as it runs, background work included. Setting `s3_access_key_id` or `s3_secret_access_key`, even to an empty string, or turning `enable_global_s3_configuration` off, stops the server from using them. The keys are not settings, so `current_setting('s3_access_key_id')` does not show them.

The environment also gives the defaults of the region (`AWS_REGION` or `AWS_DEFAULT_REGION`) and, for S3-compatible stores, of the endpoint (`DUCKDB_S3_ENDPOINT`), of TLS (`DUCKDB_S3_USE_SSL`) and of the URL style (`DUCKDB_S3_URL_STYLE`). A [server directory on an object store](../releases/storage-compatibility.md#server-directory) is read with these credentials, since the server needs them before it reads its secrets.

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

Keep `s3:ListBucket` next to `s3:GetObject`: without it S3 answers a request for a missing object with 403 instead of 404. SereneDB treats only a 404 as a missing file, so the 403 fails the statement with that error rather than reading the object as absent.

Configure the keys as an `S3` secret. Attaching a Glue catalog needs the `REGION` in the secret; S3 Tables take the region from the table bucket's ARN:

<SqlLogicTest id="configuration/aws_credentials/example_config" />

**Production-friendly**, with the usual static-key caveats: the key never expires on its own, so [rotate it](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_RotateAccessKey) on your schedule and delete keys of decommissioned deployments.

## Session tokens

If your organization issues only **temporary credentials** — from [STS](https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html), an assumed role, or SSO tooling — they come as a triple that includes a session token:

<SqlLogicTest id="configuration/aws_credentials/example_session" />

<DocCallout type="attention">
    Temporary credentials in a secret expire (typically after 1–12 hours) and SereneDB cannot renew them — when they lapse, the secret must be re-created with fresh ones. Fine for interactive sessions and testing; for unattended production use, prefer a dedicated IAM user's permanent keys or <a href="#the-servers-own-credentials">the server's own credentials</a>, which the server renews.
</DocCallout>

## S3-compatible stores

The same `S3` secret type speaks to any S3-compatible service — point it at the service's endpoint:

<SqlLogicTest id="configuration/aws_credentials/example_compatible" />

Cookbooks with service-specific settings: [Cloudflare R2](../cookbook/network_cloud_storage/cloudflare_r2_import.md), [Tigris](../cookbook/network_cloud_storage/tigris_import.md), [Fastly](../cookbook/network_cloud_storage/fastly_object_storage_import.md).

## Scoping secrets to paths

When different buckets need different credentials, give each secret a `SCOPE` — it is matched against data-file paths, longest prefix first:

<SqlLogicTest id="configuration/aws_credentials/example_scope" />

## Keeping keys out of scripts

A key written into a `CREATE SECRET` statement ends up wherever the statement does: a script, a shell history, a notebook. Read it from the environment instead.

In `serenedb shell`, `getenv()` works inside the statement:

```sql
CREATE SECRET (
    TYPE s3,
    KEY_ID getenv('AWS_ACCESS_KEY_ID'),
    SECRET getenv('AWS_SECRET_ACCESS_KEY'),
    REGION 'us-east-1'
);
```

A server has no `getenv()`. From psql 15 or newer, read the variables on the client with `\getenv` and interpolate them:

```sql
\getenv key_id AWS_ACCESS_KEY_ID
\getenv secret AWS_SECRET_ACCESS_KEY
CREATE SECRET (TYPE s3, KEY_ID :'key_id', SECRET :'secret', REGION 'us-east-1');
```

## Putting a credential to work

To attach an S3 Tables or Glue Iceberg catalog with the secret, see [Iceberg catalog authentication](./iceberg_authentication.md#aws-sigv4-s3-tables-and-glue). To read and write plain files, see [S3 Import](../cookbook/network_cloud_storage/s3_import.md), [S3 Export](../cookbook/network_cloud_storage/s3_export.md), and [S3 Iceberg Import](../cookbook/network_cloud_storage/s3_iceberg_import.md).
