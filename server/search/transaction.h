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

#include "basics/containers/flat_hash_map.h"
#include "basics/identifier.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::search {

class Transaction {
 public:
  Transaction(std::shared_ptr<catalog::Snapshot> snapshot)
    : _snapshot(snapshot) {}

  void AcquireTransaction(ObjectId index_id);

  auto GetTransactionsFromTable(ObjectId table_id) {
    return _transaction_ids | std::views::filter([&](const auto& entry) {
             return entry.second.first == table_id;
           }) |
           std::views::transform(
             [&](auto& entry) -> irs::IndexWriter::Transaction& {
               return entry.second.second;
             });
  }

  void Commit();
  void Abort();

 private:
  std::shared_ptr<catalog::Snapshot> _snapshot;
  containers::FlatHashMap<basics::Identifier,
                          std::pair<ObjectId, irs::IndexWriter::Transaction>>
    _transaction_ids;
};
}  // namespace sdb::search
