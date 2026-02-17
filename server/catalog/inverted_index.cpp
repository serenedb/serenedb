#include "catalog/inverted_index.h"

#include <expected>

#include "search/inverted_index_shard.h"
#include "vpack/serializer.h"
namespace sdb::catalog {

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, vpack::Slice args) const {
  // TODO(codeworse): parse args into InvertedIndexShardOptions
  search::InvertedIndexShardOptions options;
  if (auto r = vpack::ReadObjectNothrow(args, options); !r.ok()) {
    return std::unexpected<Result>(std::in_place, r.errorNumber(),
                                   r.errorMessage());
  }
  auto inverted_index_shard =
    std::make_shared<search::InvertedIndexShard>(*this, options, is_new);
  return inverted_index_shard;
}

void InvertedIndex::WriteInternal(vpack::Builder& builder) const {
  Index::WriteInternal(builder);
}

}  // namespace sdb::catalog
