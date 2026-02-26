#include "catalog/inverted_index.h"

#include <iresearch/analysis/analyzers.hpp>

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

ColumnAnalyzer InvertedIndex::GetColumnAnalyzer(
  catalog::Column::Id column_id) const {
  // TODO(Dronplane): implement analyzer pool for caching. And do not create
  // analyzer on demand! implement analyzer options storage - store in catalog
  // or smth.
  auto options = vpack::Slice::emptyObjectSlice();
  return {.analyzer = irs::analysis::analyzers::Get(
            _options.analyzer_name, irs::Type<irs::text_format::VPack>::get(),
            {options.startAs<char>(), options.byteSize()}),
          .features = _options.features};
}

}  // namespace sdb::catalog
