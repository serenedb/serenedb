---
title: Iceberg Catalog Authentication
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Connecting SereneDB to an [Iceberg REST catalog](https://iceberg.apache.org/rest-catalog-spec/) involves **two separate credential planes**:

1. **The catalog** — the REST API that serves table metadata and coordinates commits. Authenticated by the method you pick on `CREATE SERVER` (directly, or through an `ICEBERG` secret).
2. **The data files** — the Parquet/Avro files in object storage (S3, GCS, R2, ...). Either the catalog hands out storage credentials for you (*credential vending*), or you configure your own storage secret.

This page lists every supported method for both planes, what each is for, and which ones belong in production. Credentials are managed through the [Secrets manager](./secrets_manager.md); see [`CREATE SECRET`](../sql/statements/create_secret/index.md) for the statement itself.

## Which method should I use?

| Your catalog                                                        | Production                                                                   | Development / testing                                       |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Google BigLake (Lakehouse), SereneDB on your own hardware           | [Google service account key](#google-service-account-key)                    | [Your Google account](#google-user-account-adc), [static token](#static-bearer-token) |
| Google BigLake (Lakehouse), SereneDB on GCE/GKE                     | [Attached service account](#google-attached-service-account-metadata-server) | same, or [your Google account](#google-user-account-adc)     |
| AWS S3 Tables / AWS Glue                                            | [SigV4](#aws-sigv4-s3-tables-and-glue)                                       | same                                                          |
| Polaris, Lakekeeper, Nessie, Gravitino — anything behind an OAuth2 IdP | [OAuth2 client credentials](#oauth2-client-credentials)                    | [static token](#static-bearer-token)                          |
| Local unsecured catalog (docker-compose, CI)                        | —                                                                             | [no authentication](#no-authentication)                       |

The rule of thumb: production credentials are **machine identities that renew themselves** (service accounts, OAuth2 clients, SigV4 keys). Anything derived from a *person's* login, and any raw token you paste by hand, is a development convenience.

## Catalog authentication methods

### No authentication

For local catalogs that don't check credentials at all — a docker-compose Lakekeeper or Nessie on your laptop, a CI fixture:

<SqlLogicTest id="configuration/iceberg_authentication/example_none" />

**Testing only.** There is nothing to renew and nothing to leak, because there is nothing at all.

### Static bearer token

Any catalog that accepts a bearer token can be attached with the token pasted directly:

<SqlLogicTest id="configuration/iceberg_authentication/example_static_token" />

**Testing only.** SereneDB has no way to renew a pasted token, so the connection dies when the token expires — for Google-minted tokens (`gcloud auth print-access-token`) that is **one hour**. Every method below hands SereneDB a *long-lived* credential instead, from which it renews short-lived tokens automatically for as long as the server runs. If your working setup "mysteriously stopped after an hour", a static token is almost always why.

### OAuth2 client credentials

The standard [RFC 6749](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4) machine-to-machine flow, used by catalogs fronted by an identity provider — [Apache Polaris](https://polaris.apache.org/), [Lakekeeper](https://docs.lakekeeper.io/) with Keycloak/Entra, and similar. You register a *client* in the IdP and give SereneDB its id and secret:

<SqlLogicTest id="configuration/iceberg_authentication/example_client_credentials" />

**Production-friendly.** The client credential is a machine identity; SereneDB exchanges it for access tokens and renews them before expiry (and once more on an unexpected 401). `OAUTH2_SCOPE` defaults to `PRINCIPAL_ROLE:ALL` (the Polaris convention) — set your IdP's scope if it differs.

### Google service account key

The standard machine credential for Google's [BigLake / Lakehouse Iceberg catalog](../cookbook/network_cloud_storage/biglake_iceberg.md) when SereneDB runs outside Google Cloud. The key is a JSON file with an RSA private key; SereneDB signs short-lived tokens with it locally (the [JWT-bearer exchange](https://developers.google.com/identity/protocols/oauth2/service-account)) — the key itself never crosses the network:

<SqlLogicTest id="configuration/iceberg_authentication/example_google_sa" />

**Production-friendly** — this is what self-hosted engines across the ecosystem (Trino, StarRocks, PyIceberg) take for Google Iceberg access. How to create the account, grant the three required roles, and download the key: [Google Cloud credentials](./google_cloud_credentials.md#service-account-key). `PRIVATE_KEY_ID`, `TOKEN_URI`, and `OAUTH2_SCOPE` (default `https://www.googleapis.com/auth/cloud-platform`) are optional. The `private_key` is stored redacted.

### Google attached service account (metadata server)

When SereneDB runs **on Google Cloud** (GCE VM, GKE), the machine already has an identity and a local metadata endpoint that serves tokens for it. Create the secret with no key fields:

<SqlLogicTest id="configuration/iceberg_authentication/example_google_vm" />

**The most production-friendly option there is**: no credential material exists — nothing to store, rotate, or leak. [Google's recommended setup](https://cloud.google.com/docs/authentication) for workloads on their cloud. Details: [Google Cloud credentials](./google_cloud_credentials.md#attached-service-account).

### Google user account (ADC)

For development, SereneDB can act as *you*, using the refresh token that `gcloud auth application-default login` leaves in `~/.config/gcloud/application_default_credentials.json`:

<SqlLogicTest id="configuration/iceberg_authentication/example_google_adc" />

**Development only.** The credential is a person's identity: it dies with account changes and [Workspace session-length policies](https://support.google.com/a/answer/9368756) (often within days), it needs an interactive browser login to re-create, and audit logs attribute the database's traffic to the person. Details and caveats: [Google Cloud credentials](./google_cloud_credentials.md#your-own-google-account-adc).

### AWS SigV4 (S3 Tables and Glue)

AWS's Iceberg catalogs don't use bearer tokens at all — every request is signed with [Signature Version 4](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html) using ordinary AWS credentials from an `S3` secret. For [S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html), pass the table-bucket ARN — region and endpoint are derived from it:

<SqlLogicTest id="configuration/iceberg_authentication/example_sigv4_s3tables" />

For the [Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html), the warehouse is your 12-digit account id, `:` for the current account's default catalog, or a nested `⟨account⟩:⟨catalog⟩/⟨child⟩` path. Glue takes its endpoint region from the secret, so the `S3` secret must include `REGION`:

<SqlLogicTest id="configuration/iceberg_authentication/example_sigv4_glue" />

**Production-friendly.** SigV4 signing needs no token renewal by construction. How to create the keys and scope them to least privilege: [AWS credentials](./aws_credentials.md).

## Data-file access (the second plane)

### Credential vending (default)

With no `access_delegation_mode` option, SereneDB asks the catalog for **vended credentials** — short-lived, downscoped storage credentials per table:

<SqlLogicTest id="configuration/iceberg_authentication/example_vended" />

**Production-preferred when the catalog supports it** — one credential to manage, and storage access is automatically scoped to exactly the tables you touch. S3 Tables and Polaris vend by default; BigLake only if [credential vending](https://cloud.google.com/biglake/docs/credential-vending) is enabled on the catalog.

### Bring your own storage secret

If the catalog does not vend credentials (a BigLake catalog in its default `END_USER` mode, for example, returns `FAILED_PRECONDITION` when asked), set `access_delegation_mode 'none'` and configure a storage secret of the matching type — [`s3`](./secrets_manager.md), [`gcs`](./google_cloud_credentials.md#hmac-keys), `r2`, `azure`:

<SqlLogicTest id="configuration/iceberg_authentication/example_own_storage" />

The secret is matched to data-file paths by scope, longest prefix first — see the [Secrets manager](./secrets_manager.md). For Google Cloud data files, permanent [HMAC keys](./google_cloud_credentials.md#hmac-keys) are the production-acceptable static option.

## How token renewal works

Every token-based method (everything except SigV4, which signs each request directly, and the static token) obtains a first token at `CREATE SECRET` — a bad credential fails right there, not at first query. After that, SereneDB renews the token shortly before its advertised expiry, and retries exactly once with a fresh token if the catalog unexpectedly answers 401. No configuration is involved.

<DocCallout type="attention">
    Persistent secrets are stored <strong>unencrypted</strong> in the secret directory. Sensitive fields (<code>private_key</code>, <code>client_secret</code>, <code>refresh_token</code>, tokens) are redacted in <code>duckdb_secrets()</code> output, but the files on disk are not — protect the directory like any credential store, and prefer the methods that store little (SigV4 keys, client credentials) or nothing (attached service account) over ones that store broad personal credentials.
</DocCallout>
