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

// Replays each database's search WAL into its shards' iresearch writers, then
// commits and resyncs num_rows.
void RunSearchTableRecovery(bool skip_wal_recovery);

// Starts background maintenance (commit/consolidation/GC) for every search
// table. Must run AFTER RunSearchTableRecovery so no background commit
// publishes a half-replayed index.
void StartSearchTableMaintenance();

}  // namespace sdb::search
