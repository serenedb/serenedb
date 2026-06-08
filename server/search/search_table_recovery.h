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

namespace sdb::search {

// Search-table (StorageKind::kSearch) WAL recovery (WAL_DESIGN.md §11). Called
// once from SearchEngine::start after InitInvertedIndexes. For each database,
// replays the per-db central WAL's committed-but-unpublished records into their
// shards' iresearch writers (reconstructing PKs via the shared
// WriteChunkToSearchSink), then RefreshCommit + sets num_rows from the index.
void RunSearchTableRecovery(bool skip_wal_recovery);

}  // namespace sdb::search
