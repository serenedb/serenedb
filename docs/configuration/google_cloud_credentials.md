---
title: Google Cloud Credentials
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB can read and write Google Cloud resources — most importantly [Iceberg tables in a BigLake catalog](../cookbook/network_cloud_storage/biglake_iceberg.md) and files in Cloud Storage. Google authenticates every request with an OAuth access token that **expires after one hour**, so the credential you configure is never the token itself: it is a long-lived credential from which SereneDB mints fresh tokens automatically, for as long as the server runs. You configure it once, through the [Secrets manager](./secrets_manager.md).

This page explains the four kinds of Google credentials, how to obtain each one, and which one fits your situation.

## Which credential should I use?

| Your situation                                                       | Use                                                          |
| -------------------------------------------------------------------- | ------------------------------------------------------------ |
| Production, SereneDB runs anywhere (on-prem, AWS, any datacenter)    | [Service account key](#service-account-key)                  |
| Production, SereneDB runs on Google Cloud (GCE, GKE)                 | [Attached service account](#attached-service-account) — no credential material at all |
| Personal use, development, trying things out from a laptop           | [Your own Google account (ADC)](#your-own-google-account-adc) |
| Cloud Storage files only, no Iceberg catalog involved                | [HMAC keys](#hmac-keys) — also usable as the data-plane half of a catalog setup |

## Service account key

A [service account](https://cloud.google.com/iam/docs/service-account-overview) is a machine identity: it belongs to your project, not to a person, so it survives employee departures, ignores Workspace login policies, and can be provisioned by automation. Its key is a JSON file containing an RSA private key; SereneDB uses it to sign short-lived tokens (the [JWT-bearer exchange](https://developers.google.com/identity/protocols/oauth2/service-account)) — the key itself never crosses the network. **This is the credential to put in a production deployment.**

### Creating one

```bash
# 1. Create the service account
gcloud iam service-accounts create serenedb-engine \
    --project=my-project

# 2. Grant it access to the Iceberg catalog and the bucket
gcloud projects add-iam-policy-binding my-project \
    --member=serviceAccount:serenedb-engine@my-project.iam.gserviceaccount.com \
    --role=roles/biglake.editor
gcloud projects add-iam-policy-binding my-project \
    --member=serviceAccount:serenedb-engine@my-project.iam.gserviceaccount.com \
    --role=roles/serviceusage.serviceUsageConsumer
gcloud storage buckets add-iam-policy-binding gs://my-bucket \
    --member=serviceAccount:serenedb-engine@my-project.iam.gserviceaccount.com \
    --role=roles/storage.objectUser

# 3. Download the key (one-time download — store it safely)
gcloud iam service-accounts keys create key.json \
    --iam-account=serenedb-engine@my-project.iam.gserviceaccount.com
```

<DocCallout type="attention">
    Organizations created after May 2024 have service-account key creation <a href="https://cloud.google.com/resource-manager/docs/secure-by-default-organizations">disabled by default</a>. If step 3 fails with a policy error, an organization admin must lift <code>iam.disableServiceAccountKeyCreation</code> for the project:
    <code>gcloud org-policies reset iam.disableServiceAccountKeyCreation --project=my-project</code>
</DocCallout>

### Using it

Open `key.json` and copy three of its fields into a secret. The `private_key` value can be pasted exactly as it appears in the file, `\n` sequences included:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_sa" />

<DocCallout type="attention">
    Paste the key inside plain <code>'...'</code> quotes. PostgreSQL's escape-string syntax (<code>E'...'</code>) would consume the <code>\n</code> sequences and corrupt the key.
</DocCallout>

`PRIVATE_KEY_ID` is optional — it tells Google which of the account's keys (up to 10 can exist, e.g. during rotation) signed the request. `TOKEN_URI` (defaults to Google's token endpoint) and `OAUTH2_SCOPE` (defaults to `https://www.googleapis.com/auth/cloud-platform`) are also accepted. The `private_key` is stored redacted: it never appears in `duckdb_secrets()` output or catalog dumps.

## Attached service account

When SereneDB runs on Google Cloud (a GCE VM, GKE), the machine itself has an identity — the [attached service account](https://cloud.google.com/docs/authentication#service-accounts) — and tokens for it are served by a local metadata endpoint. There is **no credential material at all**: nothing to download, store, rotate, or leak. This is [Google's recommended setup](https://cloud.google.com/docs/authentication) for production workloads on their cloud.

Grant the VM's service account the same three roles as above, make sure the VM has the `cloud-platform` access scope, and create the secret with no key fields:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_vm" />

SereneDB detects that no key was given and fetches tokens from the metadata server instead.

## Your own Google account (ADC)

For development and personal use, you can let SereneDB act as *you*. Run:

```bash
gcloud auth application-default login
```

A browser opens, you approve, and gcloud writes `~/.config/gcloud/application_default_credentials.json` containing a **refresh token** — a long-lived credential tied to your Google account. Copy its three fields into a secret:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_adc" />

(`client_id` and `client_secret` in that file are gcloud's public application identifiers — the same for everyone; only the `refresh_token` is yours.)

<DocCallout type="attention">
    This credential is <strong>your personal identity</strong> and is not suitable for shared or production servers: it stops working if your account is disabled, if your organization enforces a <a href="https://support.google.com/a/answer/9368756">Google Cloud session-length policy</a> (tokens then expire every few hours), or if it gets rotated out by Google's limit of 100 outstanding refresh tokens per account per OAuth client. Queries also appear in Cloud audit logs as you, not as the database.
</DocCallout>

## HMAC keys

[HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys) are an AWS-style permanent key pair for Cloud Storage's S3-compatible endpoint. They never expire and involve no tokens — every request is individually signed. Their limitation: they work **only for Cloud Storage data files**. The BigLake catalog endpoint accepts only OAuth tokens, so HMAC keys cannot attach an Iceberg catalog by themselves — but they can serve as the data-plane credential next to any of the catalog credentials above.

```bash
gcloud storage hmac create serenedb-engine@my-project.iam.gserviceaccount.com \
    --project=my-project
```

This prints an `accessId` (starts with `GOOG1E...`) and a one-time `secret`. Configure them as a `gcs` secret:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_hmac" />

<DocCallout type="attention">
    An HMAC key is a permanent secret stored in plain form. Prefer creating it for a service account (not a user), scope that account's access narrowly, and note that some organizations disable HMAC via the <code>storage.restrictAuthTypes</code> policy.
</DocCallout>

## Putting a credential to work

The credential alone only proves identity. To actually connect to a BigLake Iceberg catalog — including the data-file half of the setup — follow the [BigLake Iceberg cookbook](../cookbook/network_cloud_storage/biglake_iceberg.md). For the full menu of Iceberg catalog authentication methods across clouds, see [Iceberg catalog authentication](./iceberg_authentication.md). To read plain files from Cloud Storage without a catalog, see [GCS Import](../cookbook/network_cloud_storage/gcs_import.md).
