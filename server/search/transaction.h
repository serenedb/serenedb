////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/index/index_writer.hpp>
#include <ranges>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/identifier.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "search/data_store.h"
#include "storage_engine/table_shard.h"

namespace sdb::search {

class Transaction {
 public:
  Transaction(std::shared_ptr<catalog::Snapshot> snapshot)
    : _snapshot(snapshot) {}

  void AcquireTransactionsForTable(ObjectId table_id);

  auto GetTransactionsFromTable(ObjectId table_id) {
    auto it = _transactions.find(table_id);
    SDB_ASSERT(it != _transactions.end());
    return it->second | std::views::transform(
                          [](auto& index) -> irs::IndexWriter::Transaction& {
                            return index.second;
                          });
  }

  void Commit();
  void Abort();

 private:
  std::shared_ptr<catalog::Snapshot> _snapshot;
  containers::FlatHashMap<
    ObjectId, containers::FlatHashMap<ObjectId, irs::IndexWriter::Transaction>>
    _transactions;
};
}  // namespace sdb::search
