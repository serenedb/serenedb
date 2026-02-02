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
#include "search/transaction.h"

#include "basics/errors.h"

namespace sdb::search {
void Transaction::AcquireTransactionsForTable(ObjectId table_id) {
  auto table_shard = _snapshot->GetTableShard(table_id);
  for (auto& index_id : table_shard->GetIndexes()) {
    auto index_shard = _snapshot->GetIndexShard(index_id);
    auto data_store = basics::downCast<DataStore>(index_shard);
    if (!data_store) {
      continue;
    }
    auto transaction = data_store->GetTransaction();
    _transactions[table_id][data_store->GetRelationId()] =
      std::move(transaction);
  }
}

void Transaction::Commit() {
  for (auto& [id, data] : _transactions) {
    for (auto& [relation_id, transaction] : data) {
      transaction.Commit();
    }
    _transactions.erase(id);
  }
}

void Transaction::Abort() {
  for (auto& [id, data] : _transactions) {
    for (auto& [relation_id, transaction] : data) {
      transaction.Abort();
    }
    _transactions.erase(id);
  }
  _transactions.clear();
}

}  // namespace sdb::search
