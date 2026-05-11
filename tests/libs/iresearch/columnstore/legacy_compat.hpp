////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
////////////////////////////////////////////////////////////////////////////////

#pragma once

// Test-only adapter that satisfies the legacy ColumnstoreReader /
// ColumnstoreWriter / ColumnReader / ColumnOutput interfaces while
// forwarding all data through the new cs (`.cs` file). Production has
// already migrated; the only remaining consumers are iresearch unit
// tests, which exercise the format API end-to-end. Once those tests
// either go away or move to the new cs API directly, this adapter can
// be deleted along with the legacy interfaces themselves.
//
// Limitations (by design):
//   - All columns are persisted as BLOB. The legacy ColumnInfo's
//     value_type is ignored.
//   - `ColumnReader::payload()` always returns an empty span -- the
//     `ColumnFinalizer` header-bytes channel is dropped. Tests that
//     assert on payload bytes must be moved off this path.
//   - `ColumnHint::PrevDoc` is unsupported; iterators built with that
//     hint throw on construction.
//   - Primary-sort is not supported here -- caller must not pass a
//     non-trivial Comparer when flushing through this writer.
//   - The DatabaseInstance backing the new cs codecs is a per-process
//     singleton owned for the lifetime of the test binary. No leaks at
//     normal exit (unique_ptr in a function-local static).

#include <memory>

#include "iresearch/formats/formats.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs::columnstore::legacy {

// Returns the per-process DuckDB DatabaseInstance the wrapper uses for
// codec lookups + buffer manager. Lazily initialized on first call.
duckdb::DatabaseInstance& TestOnlyDatabase();

ColumnstoreWriter::ptr MakeWriter(bool consolidation);

ColumnstoreReader::ptr MakeReader();

// Installs the wrapper as the legacy columnstore factory pair on
// FormatImpl. Call this once at test-binary startup (e.g. from main()
// or a global static). Production binaries don't link this file and
// thus see the default null factories.
void RegisterAsFormatFactories();

}  // namespace irs::columnstore::legacy
