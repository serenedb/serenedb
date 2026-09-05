---
title: Encryption
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports reading and writing encrypted Parquet files.
SereneDB broadly follows the [Parquet Modular Encryption specification](https://github.com/apache/parquet-format/blob/master/Encryption.md) with some [limitations](#limitations).

## Reading and Writing Encrypted Files

Using the `PRAGMA add_parquet_key` function, named encryption keys of 128, 192, or 256 bits can be added to a session. These keys are stored in-memory:

<SqlLogicTest id="data_import_and_export/parquet/encryption/example_001" />

### Writing Encrypted Parquet Files

After specifying the key (e.g., `key256`), files can be encrypted as follows:

<SqlLogicTest id="data_import_and_export/parquet/encryption/example_002" />

### Reading Encrypted Parquet Files

An encrypted Parquet file using a specific key (e.g., `key256`), can then be read as follows:

<SqlLogicTest id="data_import_and_export/parquet/encryption/example_003" />

Or:

<SqlLogicTest id="data_import_and_export/parquet/encryption/example_004" />

## Interoperability

SereneDB can read uniformly encrypted Parquet files written by the Arrow C++ API (e.g., via PyArrow), as long as the same encryption key is used for both the footer and all columns.

## Limitations

SereneDB's Parquet encryption currently has the following limitations.

SereneDB encrypts the footer and all columns using the `footer_key`. The Parquet specification allows encryption of individual columns with different keys, e.g.:

<SqlLogicTest id="data_import_and_export/parquet/encryption/example_005" />

However, this is unsupported at the moment and will cause an error to be thrown (for now).

## Performance Implications

Note that encryption has some performance implications: reading and writing encrypted Parquet files is slower than reading and writing the unencrypted equivalents.
