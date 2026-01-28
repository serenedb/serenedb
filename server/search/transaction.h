#pragma once

#include <iresearch/index/index_writer.hpp>
#include <ranges>

#include "basics/containers/flat_hash_map.h"
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
  containers::FlatHashMap<ObjectId,
                          std::pair<ObjectId, irs::IndexWriter::Transaction>>
    _transaction_ids;
};
}  // namespace sdb::search
