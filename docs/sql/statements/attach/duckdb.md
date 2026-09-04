---
title: DuckDB
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

A DuckDB database file is SereneDB's native attachable database. `ATTACH` opens a DuckDB file for reading and writing, and the alias is inferred from the file name unless an explicit one is given. For the general `ATTACH` and `DETACH` syntax, see [ATTACH AND DETACH](./index.md).

Attach the database `file.db` with the alias inferred from the name (`file`):

<SqlLogicTest id="sql/statements/attach/index/example_001" />

Attach the database `file.db` with an explicit alias (`file_db`):

<SqlLogicTest id="sql/statements/attach/index/example_002" />

Attach the database `file.db` only if the inferred alias `file` does not yet exist:

<SqlLogicTest id="sql/statements/attach/index/example_008" />

Attach the database `file.db` only if the explicit alias `file_db` does not yet exist:

<SqlLogicTest id="sql/statements/attach/index/example_009" />

Attach the database `file2.db` as alias `file_db`, detaching and replacing the existing alias if it exists:

<SqlLogicTest id="sql/statements/attach/index/example_010" />

Create a table in the attached database with alias `file`:

<SqlLogicTest id="sql/statements/attach/index/example_011" />

Detach the database with alias `file`:

<SqlLogicTest id="sql/statements/attach/index/example_012" />

Show a list of all attached databases:

<SqlLogicTest id="sql/statements/attach/index/example_013" />

Change the default database that is used to the database `file`:

<SqlLogicTest id="sql/statements/attach/index/example_014" />

## Options

Zero or more options may be provided within parentheses following the `ATTACH` statement. Parameter values can be passed in with or without wrapping in single quotes. Arbitrary expressions may be used for parameter values.

| Name                | Description                                                                                                                 | Type      | Default value |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------- | --------- | ------------- |
| `ACCESS_MODE`       | Access mode of the database (`AUTOMATIC`, `READ_ONLY`, or `READ_WRITE`).                                                    | `VARCHAR` | `automatic`   |
| `COMPRESS`          | Whether the database is compressed. Only applicable for in-memory databases.                                                | `VARCHAR` | `false`       |
| `TYPE`              | The database type. `DUCKDB` for a file, or `postgres` deduced from a connection string.                                     | `VARCHAR` | `DUCKDB`      |
| `BLOCK_SIZE`        | The block size of a new database file. Must be a power of two and within [16384, 262144]. Cannot be set for existing files. | `UBIGINT` | `262144`      |
| `ROW_GROUP_SIZE`    | The row group size of a new database file.                                                                                  | `UBIGINT` | `122880`      |
| `STORAGE_VERSION`   | The version of the storage used.                                                                                            | `VARCHAR` | `v1.0.0`      |
| `ENCRYPTION_KEY`    | The encryption key used for encrypting the database.                                                                        | `VARCHAR` | -             |
| `ENCRYPTION_CIPHER` | The encryption cipher used for encrypting the database (`CTR` or `GCM`).                                                     | `VARCHAR` | -             |
| `RECOVERY_MODE`     | Recovery mode for the database. `no_wal_writes` disables WAL writes, improving performance at the cost of crash recovery.   | `VARCHAR` | -             |

Attach the database `file.db` in read only mode:

<SqlLogicTest id="sql/statements/attach/index/example_003" />

Attach the database `file.db` with a block size of 16 kB:

<SqlLogicTest id="sql/statements/attach/index/example_004" />

Attach the database `file.db` with a row group size of 2048 rows:

<SqlLogicTest id="sql/statements/attach/index/example_005" />

Attach the database `file.db` with WAL writes disabled for improved performance:

<SqlLogicTest id="sql/statements/attach/index/example_006" />

## Explicit Storage Versions

SereneDB allows explicitly specifying the storage version. Using this, you can opt-in to newer forwards-incompatible features:

<SqlLogicTest id="sql/statements/attach/index/example_017" />

This setting specifies the minimum SereneDB version that should be able to read the database file. When database files are written with this option, the resulting files cannot be opened by older SereneDB versions than the specified version. They can be read by the specified version and all newer versions of SereneDB.

## Database Encryption

SereneDB supports database encryption. By default, it uses [AES encryption](https://en.wikipedia.org/wiki/Advanced_Encryption_Standard) with a key length of 256 bits using the recommended [GCM](https://en.wikipedia.org/wiki/Galois/Counter_Mode) mode. The encryption covers the main database file, the write-ahead-log (WAL) file and even temporary files. To attach to an encrypted database, use the `ATTACH` statement with an `ENCRYPTION_KEY`:

<SqlLogicTest id="sql/statements/attach/index/example_018" />

To change the AES mode to [CTR](https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation#Counter_%28CTR%29), use the `ENCRYPTION_CIPHER` option:

<SqlLogicTest id="sql/statements/attach/index/example_020" />

<DocCallout type="tip">
SereneDB's encryption does not yet meet the official [NIST requirements](https://csrc.nist.gov/projects/cryptographic-standards-and-guidelines).
</DocCallout>

## Remote files (HTTP / S3)

`ATTACH` supports HTTP and S3 endpoints. For these, it creates a read-only connection by default. Therefore, the following two commands are equivalent:

<SqlLogicTest id="sql/statements/attach/index/example_015" />

Similarly, the following two commands connecting to S3 are equivalent:

<SqlLogicTest id="sql/statements/attach/index/example_016" />
