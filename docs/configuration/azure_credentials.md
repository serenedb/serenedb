---
title: Azure Credentials
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB reads and writes files in Azure Blob Storage (including Data Lake Storage / ABFSS paths) through `azure` secrets in the [Secrets manager](./secrets_manager.md). Azure authenticates with several very different credential kinds; this page explains each one, how to obtain it, and which one fits your deployment.

## Which credential should I use?

| Your situation                                              | Use                                                                 |
| ------------------------------------------------------------ | -------------------------------------------------------------------- |
| Production, SereneDB runs anywhere (on-prem, other clouds)  | [Service principal](#service-principal)                              |
| Production, SereneDB runs on Azure (VM, AKS)                | [Managed identity](#managed-identity) — no credential material at all |
| Personal use, development, trying things out from a laptop  | [Credential chain](#credential-chain) with `az login`                |
| Legacy/simple setups with the storage account key           | [Connection string](#connection-string)                              |
| One-off tests with a token you already have                 | [Access token](#access-token)                                        |

## Service principal

A [service principal](https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals) is Azure's machine identity — the analogue of a Google service account or a dedicated IAM user. Create one and grant it access to the storage account:

```bash
az ad sp create-for-rbac --name serenedb-engine \
    --role "Storage Blob Data Contributor" \
    --scopes /subscriptions/⟨sub⟩/resourceGroups/⟨rg⟩/providers/Microsoft.Storage/storageAccounts/⟨account⟩
```

This prints `tenant`, `appId`, and `password` — configure them as a secret:

<SqlLogicTest id="configuration/azure_credentials/example_service_principal" />

**Production-friendly**: a scoped machine identity whose secret can be rotated independently of any person. A `CLIENT_CERTIFICATE_PATH` can be used instead of `CLIENT_SECRET` for certificate-based authentication.

## Managed identity

When SereneDB runs **on Azure** (a VM or AKS), the machine itself has a [managed identity](https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview) and tokens for it come from the platform — there is **no credential material at all**: nothing to store, rotate, or leak. Assign the identity, grant it *Storage Blob Data Contributor* on the account, and create the secret with no secrets in it:

<SqlLogicTest id="configuration/azure_credentials/example_managed_identity" />

**The most production-friendly option on Azure's own compute** — the same pattern as an [attached service account on Google Cloud](./google_cloud_credentials.md#attached-service-account). `CLIENT_ID` (or `OBJECT_ID`/`RESOURCE_ID`) selects a user-assigned identity; omit them for the system-assigned one.

## Credential chain

The chain resolves credentials automatically from where you already are — the Azure CLI session (`az login`), environment variables, or the machine's identity, in the order you list:

<SqlLogicTest id="configuration/azure_credentials/example_chain" />

Accepted chain links: `cli`, `env`, `managed_identity`, `workload_identity`, `default` (the [Azure SDK default chain](https://learn.microsoft.com/en-us/azure/developer/intro/azure-developer-auth-overview)). **Development-friendly**: `CHAIN 'cli'` after `az login` is the quickest way to query from a laptop as yourself — with the usual personal-credential caveats (it is *your* identity, and it stops working when your session does).

## Connection string

The classic all-in-one credential containing the **storage account key**:

<SqlLogicTest id="configuration/azure_credentials/example_connection_string" />

Works everywhere and never expires — which is exactly its risk: the account key grants full access to the whole storage account and cannot be scoped down. If you use it, [rotate the key](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage) on a schedule and prefer a service principal where you can.

## Access token

If you already hold a bearer token (e.g. `az account get-access-token --resource https://storage.azure.com`), you can paste it directly:

<SqlLogicTest id="configuration/azure_credentials/example_access_token" />

**Testing only** — like every raw token, it expires (typically after ~1 hour) and SereneDB cannot renew it.

## Putting a credential to work

With a secret in place, `az://` and `abfss://` paths just work:

<SqlLogicTest id="configuration/azure_credentials/example_query" />

Secrets can be scoped to specific containers via `SCOPE`, longest prefix first — see the [Secrets manager](./secrets_manager.md). For the AWS and Google Cloud equivalents of this page, see [AWS credentials](./aws_credentials.md) and [Google Cloud credentials](./google_cloud_credentials.md).
