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
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

namespace duckdb {

class DatabaseInstance;
}

namespace sdb::connector {

// Register the sdb_term_stats('[schema.]index') table function: one row per
// (segment, column, term) of an inverted index's string fields, with the
// term's per-segment document frequency and total occurrence count. The
// primary consumer is corpus-driven dictionary tuning, e.g. deriving a
// shingle dictionary's `frequentwords` list from document-frequency ratios.
void RegisterTermStatsFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
