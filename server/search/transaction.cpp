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
  SDB_ASSERT(table_shard);
  for (auto& index_id : table_shard->GetIndexes()) {
    auto index_shard = _snapshot->GetIndexShard(index_id);
    auto data_store = basics::downCast<DataStore>(index_shard);
    if (!data_store) {
      continue;
    }
    auto transaction = data_store->GetTransaction();
    _transactions[table_id][index_id] = std::move(transaction);
  }
}

std::vector<irs::IndexWriter::Transaction*>
Transaction::GetTransactionsFromTable(ObjectId table_id) {
  auto it = _transactions.find(table_id);
  if (it == _transactions.end()) {
    return {};
  }
  std::vector<irs::IndexWriter::Transaction*> result;
  result.reserve(it->second.size());
  for (auto& [relation_id, transaction] : it->second) {
    result.push_back(&transaction);
  }
  return result;
}

void Transaction::Commit() {
  for (auto& [id, data] : _transactions) {
    for (auto& [relation_id, transaction] : data) {
      transaction.Commit();
    }
  }
  _transactions.clear();
}

void Transaction::Abort() {
  for (auto& [id, data] : _transactions) {
    for (auto& [relation_id, transaction] : data) {
      transaction.Abort();
    }
  }
  _transactions.clear();
}

}  // namespace sdb::search
