#include "catalog/inverted_index.h"

#include "search/inverted_index_shard.h"
namespace sdb::catalog {

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, vpack::Slice args) const {
  // TODO(codeworse): parse args into InvertedIndexShardOptions
  auto inverted_index_shard = std::make_shared<search::InvertedIndexShard>(
    *this, search::InvertedIndexShardOptions{}, is_new);
  return inverted_index_shard;
}

void InvertedIndex::WriteInternal(vpack::Builder& builder) const {
  Index::WriteInternal(builder);
}

}  // namespace sdb::catalog
