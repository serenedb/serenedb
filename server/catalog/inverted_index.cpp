#include "catalog/inverted_index.h"

#include "search/inverted_index_shard.h"
namespace sdb::catalog {

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, vpack::Slice args) const {
  // TODO(codeworse): parse args into InvertedIndexShardOptions
  search::InvertedIndexShardOptions options;
  options.commit_interval_ms = 1000;
  options.consolidation_interval_ms = 10000;
  // cleanup step?
  auto inverted_index_shard =
    search::InvertedIndexShard::Create(*this, options, is_new);
  return inverted_index_shard;
}

void InvertedIndex::WriteInternal(vpack::Builder& builder) const {
  Index::WriteInternal(builder);
}

}  // namespace sdb::catalog
