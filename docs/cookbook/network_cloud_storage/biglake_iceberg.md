---
title: Google BigLake Iceberg
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

This guide connects SereneDB to a [BigLake Iceberg REST catalog](https://docs.cloud.google.com/lakehouse/docs/set-up-lakehouse-iceberg-rest-catalog) (Google's managed Iceberg metastore, since renamed *Lakehouse for Apache Iceberg* — the APIs, `gcloud` commands and IAM roles still say `biglake`) with full read **and write** access — `CREATE TABLE`, `INSERT`, and atomic commits included. Once set up, the connection maintains itself: every Google token involved — the catalog token, and the storage credentials a vending catalog hands out per table — lasts one hour and is re-minted automatically for as long as the server runs.

## What you need

- A Google Cloud project with a BigLake Iceberg catalog and its Cloud Storage bucket. If you don't have one yet, follow [Google's setup guide](https://docs.cloud.google.com/lakehouse/docs/set-up-lakehouse-iceberg-rest-catalog).
- A Google credential for the **catalog**. Pick one with the [Google Cloud credentials](../../configuration/google_cloud_credentials.md) page — in short: a **service account key** for production, the **attached service account** if SereneDB runs on Google Cloud, or **your own account (ADC)** for development.
- A credential for the **data files** — an [HMAC key](../../configuration/google_cloud_credentials.md#hmac-keys) in the default setup (see [step 2](#step-2-create-the-data-file-secret)).
- The identity you picked needs `roles/biglake.editor` and `roles/serviceusage.serviceUsageConsumer` on the project, and `roles/storage.objectUser` on the bucket. On a credential-vending catalog the catalog's **own service agent** (`…@gcp-sa-biglakerestcatalog.iam.gserviceaccount.com`, printed when the catalog is created) needs `roles/storage.objectUser` on the bucket too — it is the account that mints the downscoped tokens.

(For the full menu of catalog authentication methods — including AWS and OAuth2 catalogs — see [Iceberg catalog authentication](../../configuration/iceberg_authentication.md).)

## Step 1: Create the catalog secret

For production, use a service account key ([how to create one](../../configuration/google_cloud_credentials.md#service-account-key)) — paste its fields from the downloaded `key.json`:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_sa" />

Running on GCE or GKE? Skip the key entirely — the VM's own identity is used:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_vm" />

For development with [your own account](../../configuration/google_cloud_credentials.md#your-own-google-account-adc), copy the fields from `~/.config/gcloud/application_default_credentials.json`:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_adc" />

The `x-goog-user-project` header attributes quota to a project you name. Google requires it whenever the credential does not carry a project of its own — which covers the user-account (ADC) variant above — and it is harmless for the other two, so the examples set it everywhere. Whenever it is sent, the identity also needs `roles/serviceusage.serviceUsageConsumer` on that project.

## Step 2: Create the data-file secret

BigLake catalogs in the default `END_USER` credential mode do not hand out storage credentials — the data files in the bucket are read with a credential you configure separately. The static option that needs no renewal is an [HMAC key](../../configuration/google_cloud_credentials.md#hmac-keys) for the same service account:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_hmac" />

If your catalog has [credential vending](https://docs.cloud.google.com/lakehouse/docs/credential-vending) enabled instead, skip this step — the catalog will hand SereneDB downscoped per-table storage credentials (and drop the `access_delegation_mode` option in step 3).

A vended storage credential is **not** permanent: BigLake issues it with a one-hour lifetime and advertises the expiry alongside the token. SereneDB tracks that expiry and re-vends the credential from the catalog before it lapses, so a long-running server keeps reading. Two consequences worth knowing:

- Credential vending needs the catalog's own service agent to be able to read the bucket — grant it `roles/storage.objectUser` (the console calls this **Set bucket permissions**). Without it the catalog cannot mint the downscoped token, and table creation fails with a `403` naming the `gcp-sa-biglakerestcatalog` account.
- On SereneDB versions before this renewal shipped, the vended credential was captured once and never renewed: reads of `.parquet` and `metadata/*.avro` began failing with **HTTP 401** about an hour after a connection first touched the table, and stayed failing until the server was restarted. If you are on an older build and cannot upgrade, use `access_delegation_mode 'none'` with an HMAC key (step 2) instead — a static credential has no expiry to lapse.

## Step 3: Attach the catalog

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_server" />

Two options deserve explanation:

- **`warehouse`** uses the `bl://projects/⟨project⟩/catalogs/⟨catalog⟩` form — the identifier for named BigLake catalogs. (Bucket-based catalogs use `gs://⟨bucket⟩` instead; other shapes are rejected by Google with *"Unsupported warehouse name format"*.)
- **`access_delegation_mode 'none'`** tells SereneDB not to ask the catalog for storage credentials (the default `END_USER` catalog mode refuses to vend them) and to use your `gcs` secret from step 2 for the data files instead. On a catalog with credential vending enabled, omit the option — the default `vended_credentials` mode is preferred.

## Step 4: Query and write

The catalog's namespaces appear as schemas of the attached server:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_query" />

Writes go through the catalog's atomic commit protocol, so they are safe alongside other engines (Spark, BigQuery) using the same catalog:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_write" />

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `Unsupported warehouse name format` | The warehouse isn't in `bl://projects/⟨project⟩/catalogs/⟨catalog⟩` (or `gs://⟨bucket⟩`) form. |
| `PERMISSION_DENIED` / HTTP 403 from the catalog | The identity lacks `roles/biglake.editor`, or the `x-goog-user-project` header is missing / the identity lacks `roles/serviceusage.serviceUsageConsumer`. |
| `FAILED_PRECONDITION` on table access | The catalog is in `END_USER` credential mode but was attached with credential vending (the default). Add `access_delegation_mode 'none'` and configure a `gcs` data secret ([step 2](#step-2-create-the-data-file-secret)). |
| HTTP 403 when reading data files | No credential reached the request: no `gcs` secret configured, or its identity lacks `roles/storage.objectUser` on the bucket. |
| HTTP **401** when reading data files | A credential *was* sent and Google rejected it as no longer valid — almost always an expired vended storage credential on a build without renewal. Distinct from the 403 above: 403 is "no credential or not authorised", 401 is "this credential has expired". See [step 2](#step-2-create-the-data-file-secret). |
| HTTP 403 naming a `gcp-sa-biglakerestcatalog` account | The catalog's service agent cannot reach the bucket, so it cannot vend. Grant it `roles/storage.objectUser` ([step 2](#step-2-create-the-data-file-secret)). |
| `X-Iceberg-Access-Delegation header must be present` | The catalog is in credential-vending mode but the request did not advertise vending support. Attach without `access_delegation_mode 'none'` so the default `vended_credentials` applies. |
| `Could not parse 'private_key'` | The key was pasted inside `E'...'` quotes, which consume the `\n` escapes — use plain `'...'` quotes. |
| Everything worked, then died after ~1 hour | Some Google credential expired and was not renewed. Either a raw access token was configured (`TOKEN '...'`), which cannot be refreshed — or, on older builds against a credential-vending catalog, the vended storage credential lapsed ([step 2](#step-2-create-the-data-file-secret)). The two look different: when the catalog credential dies, table listings break too; when a vended storage credential dies, listings and `count(*)` still succeed and only reads of column values fail. |
| Service-account key creation is denied | Your organization blocks it by default — see [the callout here](../../configuration/google_cloud_credentials.md#service-account-key). |
