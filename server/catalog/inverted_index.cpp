#include "catalog/inverted_index.h"

#include "search/data_store.h"
namespace sdb::catalog {

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, vpack::Slice args) const {
  // TODO(codeworse): parse args into DataStoreOptions
  auto data_store = std::make_shared<search::DataStore>(
    *this, search::DataStoreOptions{}, is_new);
  data_store->StartTasks();
  return data_store;
}

void InvertedIndex::WriteInternal(vpack::Builder& builder) const {
  Index::WriteInternal(builder);
}

}  // namespace sdb::catalog
