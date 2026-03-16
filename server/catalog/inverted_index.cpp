#include "catalog/inverted_index.h"

#include <iresearch/analysis/analyzers.hpp>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
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
  if (!_options.text_dictionary.isSet()) {
    // TODO(Dronplane): implement default text dictionary like in PG
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "Default text dictionary is not implemented.");
  }
  auto snapshot = GetCatalog().GetSnapshot();

  auto dict = snapshot->GetObject<Tokenizer>(_options.text_dictionary);
  SDB_ENSURE(dict, ERROR_INTERNAL,
             "Dictionary for inverted index does not exists");
  auto tokenizer = dict->GetTokenizer();
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  return {.analyzer = *std::move(tokenizer), .features = _options.features};
}

}  // namespace sdb::catalog
