#include "catalog/inverted_index.h"

#include <iresearch/analysis/analyzers.hpp>

#include "basics/down_cast.h"
#include "catalog/index.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, ObjectId id, IndexShardOptions& options) const {
  auto& shard_options =
    basics::downCast<search::InvertedIndexShardOptions>(options);
  auto inverted_index_shard =
    search::InvertedIndexShard::Create(id, *this, shard_options, is_new);
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
