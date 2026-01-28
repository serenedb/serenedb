#include "search/transaction.h"

#include "basics/errors.h"

namespace sdb::search {
void Transaction::AcquireTransaction(ObjectId index_id) {
  auto data_store = _snapshot->GetDataStore(index_id);
  auto transaction = data_store->GetTransaction();
  _transaction_ids[data_store->GetId()] = {data_store->GetRelationId(),
                                           std::move(transaction)};
}

void Transaction::Commit() {
  for (auto& [id, data] : _transaction_ids) {
    data.second.Commit();
  }
  _transaction_ids.clear();
}

void Transaction::Abort() {
  for (auto& [id, data] : _transaction_ids) {
    data.second.Abort();
  }
  _transaction_ids.clear();
}

}  // namespace sdb::search
