---
title: Google BigLake Iceberg
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

This guide connects SereneDB to a [BigLake Iceberg REST catalog](https://cloud.google.com/biglake/docs/blms-rest-catalog) (Google's managed Iceberg metastore, also called the Lakehouse catalog) with full read **and write** access — `CREATE TABLE`, `INSERT`, and atomic commits included. Once set up, the connection maintains itself: Google's one-hour catalog tokens are re-minted automatically for as long as the server runs.

## What you need

- A Google Cloud project with a BigLake Iceberg catalog and its Cloud Storage bucket. If you don't have one yet, follow [Google's setup guide](https://cloud.google.com/biglake/docs/blms-rest-catalog).
- A Google credential for the **catalog**. Pick one with the [Google Cloud credentials](../../configuration/google_cloud_credentials.md) page — in short: a **service account key** for production, the **attached service account** if SereneDB runs on Google Cloud, or **your own account (ADC)** for development.
- A credential for the **data files** — an [HMAC key](../../configuration/google_cloud_credentials.md#hmac-keys) in the default setup (see [step 2](#step-2-create-the-data-file-secret)).
- The identity you picked needs `roles/biglake.editor` and `roles/serviceusage.serviceUsageConsumer` on the project, and `roles/storage.objectUser` on the bucket.

(For the full menu of catalog authentication methods — including AWS and OAuth2 catalogs — see [Iceberg catalog authentication](../../configuration/iceberg_authentication.md).)

## Step 1: Create the catalog secret

For production, use a service account key ([how to create one](../../configuration/google_cloud_credentials.md#service-account-key)) — paste its fields from the downloaded `key.json`:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_sa" />

Running on GCE or GKE? Skip the key entirely — the VM's own identity is used:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_vm" />

For development with [your own account](../../configuration/google_cloud_credentials.md#your-own-google-account-adc), copy the fields from `~/.config/gcloud/application_default_credentials.json`:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_adc" />

The `x-goog-user-project` header is required by Google for quota attribution — set it to your project ID in all variants.

## Step 2: Create the data-file secret

BigLake catalogs in the default `END_USER` credential mode do not hand out storage credentials — the data files in the bucket are read with a credential you configure separately. The static option that needs no renewal is an [HMAC key](../../configuration/google_cloud_credentials.md#hmac-keys) for the same service account:

<SqlLogicTest id="cookbook/network_cloud_storage/biglake_iceberg/example_secret_hmac" />

If your catalog has [credential vending](https://cloud.google.com/biglake/docs/credential-vending) enabled instead, skip this step — the catalog will hand SereneDB downscoped per-table storage credentials (and drop the `access_delegation_mode` option in step 3).

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
| HTTP 403 when reading data files | No `gcs` secret configured, or its identity lacks `roles/storage.objectUser` on the bucket. |
| `Could not parse 'private_key'` | The key was pasted inside `E'...'` quotes, which consume the `\n` escapes — use plain `'...'` quotes. |
| Everything worked, then died after ~1 hour | A raw access token was configured (`TOKEN '...'`) instead of one of the credentials above. Raw tokens cannot be refreshed. |
| Service-account key creation is denied | Your organization blocks it by default — see [the callout here](../../configuration/google_cloud_credentials.md#service-account-key). |
