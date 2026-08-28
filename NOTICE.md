## SereneDB

Copyright 2025, SereneDB GmbH

Licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).

This project includes the software components developed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0):

#### IResearch

[IResearch](libs/iresearch) has been archived and is no longer actively maintained in its [original repository](https://github.com/iresearch-toolkit/iresearch). This project continues the development of IResearch with ongoing modifications, improvements, and maintenance.

The original work is licensed under the Apache 2.0 license.

Modifications and new code added are licensed under corresponding [license](LICENSE).

#### GNU C Library (glibc)

Release binaries are statically linked against [glibc](https://www.gnu.org/software/libc/), which is licensed under the [LGPL-2.1](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html). License files from glibc are included in the distribution under `glibc/`.

#### GEOS

Release binaries are statically linked against [GEOS](https://libgeos.org),
which is licensed under the [LGPL-2.1](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html).
GEOS provides the geometry predicates and overlay operations of the spatial
extension. Its license file is included in the distribution under
`third_party/geos/COPYING`, and the exact source revision is the pinned
`third_party/geos` submodule of this repository, so a recipient of a binary
can relink it against a modified GEOS.

#### ArangoDB

Some code is based on [ArangoDB](https://github.com/arangodb/arangodb), specifically incorporating code from [commit](https://github.com/arangodb/arangodb/commit/bdac13f0edef5ff69d7d9ae5758a30072bd6d312).

The original work is licensed under the Apache 2.0 license.

Modifications and new code added are licensed under corresponding [license](LICENSE).

#### clickhouse-cpp

[clickhouse-cpp](third_party/clickhouse-cpp) is the C++ client library for
ClickHouse, used by the ClickHouse connector. Copyright 2018-2023 ClickHouse,
Inc. and Copyright 2017 Pavel Artemkin, licensed under the Apache 2.0 license.

Modifications and new code added are licensed under corresponding [license](LICENSE).
