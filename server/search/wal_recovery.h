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

#include <cstdint>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <span>
#include <vector>

#include "catalog/walk.h"
#include "search/store.h"
#include "search/table.h"

namespace sdb::search {

struct TableSnapshot {
  std::shared_ptr<Store> store;
  std::shared_ptr<const TableState> state;
};

void RecoverSearchStorages(
  duckdb::ClientContext& context,
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> attached);
void StartSearchStorageTasks();
void WriteOutTables();
std::vector<TableSnapshot> CaptureSnapshot(Tick snapshot,
                                           catalog::WalkCounters& walks);
std::vector<std::vector<uint8_t>> SnapshotRecords(
  std::span<const TableSnapshot> tables);

}  // namespace sdb::search
