---
title: Secrets Manager
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The **Secrets manager** provides a unified user interface for secrets across all backends that use them. Secrets can be scoped, so different storage prefixes can have different secrets, allowing for example to join data across organizations in a single query. Secrets can also be persisted, so that they do not need to be specified every time SereneDB is launched.

<DocCallout type="attention">
    Persistent secrets are stored in an unencrypted format.
</DocCallout>

## Types of Secrets

Secrets are typed, their type identifies which service they are for.
Currently, the following secret types are available:

| Secret type   | Service / protocol             |
| ------------- | ------------------------------ |
| `azure`       | Azure Blob Storage             |
| `gcs`         | Google Cloud Storage           |
| `http`        | HTTP and HTTPS                 |
| `huggingface` | Hugging Face                   |
| `iceberg`     | Iceberg REST Catalog           |
| `postgres`    | PostgreSQL                     |
| `r2`          | Cloudflare R2                  |
| `s3`          | AWS S3                         |

Per-cloud guides cover how to obtain each kind of credential and which one fits your deployment: [Google Cloud credentials](./google_cloud_credentials.md) (`gcs`, and `iceberg` targeting BigLake), [AWS credentials](./aws_credentials.md) (`s3`, and the S3 Tables / Glue catalogs), and [Azure credentials](./azure_credentials.md) (`azure`). For `iceberg` secrets in general — every supported catalog authentication method, and when to use which — see [Iceberg catalog authentication](./iceberg_authentication.md). The remaining types are single-credential and documented where they are used: [`r2`](../cookbook/network_cloud_storage/cloudflare_r2_import.md), [`http`](../cookbook/network_cloud_storage/http_import.md), `huggingface`, and `postgres`.

For each type, there are one or more “secret providers” that specify how the secret is created. Secrets can also have an optional scope, which is a file path prefix that the secret applies to. When fetching a secret for a path, the secret scopes are compared to the path, returning the matching secret for the path. In the case of multiple matching secrets, the longest prefix is chosen.

## Creating a Secret

Secrets can be created using the [`CREATE SECRET` SQL statement](../sql/statements/create_secret/index.md).
Secrets can be **temporary** or **persistent**. Temporary secrets are used by default and live in memory for the lifespan of the SereneDB instance. Persistent secrets are stored in an **unencrypted** format and are automatically loaded when SereneDB starts.

### Secret Providers

To create a secret, a **Secret Provider** needs to be used. A Secret Provider is a mechanism through which a secret is generated. To illustrate this, for the `S3` secret type, SereneDB currently supports two providers: `CONFIG` and `credential_chain`. The `CONFIG` provider requires the user to pass all configuration information into the `CREATE SECRET`, whereas the `credential_chain` provider will automatically try to fetch credentials. When no Secret Provider is specified, the `CONFIG` provider is used.

### Temporary Secrets

To create a temporary unscoped secret to access S3, we can now use the following:

<SqlLogicTest id="configuration/secrets_manager/example_001" />

Note that we implicitly use the default `CONFIG` secret provider here.

### Persistent Secrets

In order to persist secrets between SereneDB database instances, we can now use the `CREATE PERSISTENT SECRET` command, e.g.:

<SqlLogicTest id="configuration/secrets_manager/example_002" />

This writes the secret (unencrypted) so that it is available again the next time SereneDB starts.

## Deleting Secrets

Secrets can be deleted using the [`DROP SECRET` statement](../sql/statements/create_secret/index.md#syntax-for-drop-secret), e.g.:

<SqlLogicTest id="configuration/secrets_manager/example_004" />

## Creating Multiple Secrets for the Same Service Type

If two secrets exist for a service type, the scope can be used to decide which one should be used. For example:

<SqlLogicTest id="configuration/secrets_manager/example_005" />

<SqlLogicTest id="configuration/secrets_manager/example_006" />

Now, if the user queries something from `s3://⟨my-other-bucket⟩/something`, secret `secret2` will be chosen automatically for that request. To see which secret is being used, the `which_secret` scalar function can be used, which takes a path and a secret type as parameters:

<SqlLogicTest id="configuration/secrets_manager/example_007" />

## Listing Secrets

Secrets can be listed using the built-in table-producing function, e.g., by using the [`duckdb_secrets()` table function](../sql/functions/duckdb_table_functions.md#duckdb_secrets):

<SqlLogicTest id="configuration/secrets_manager/example_008" />

Sensitive information will be redacted.
