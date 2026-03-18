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
  vpack::ObjectBuilder scope_object(&builder);
  Index::WriteInternal(builder);
  vpack::ArrayBuilder ob(&builder, kIndexImplOptions);
  vpack::WriteTuple(builder, _options);
}

ColumnAnalyzer InvertedIndex::GetColumnAnalyzer(
  catalog::Column::Id column_id) const {
  auto it = _options.columns.find(column_id);
  if (it == _options.columns.end()) {
    SDB_THROW(ERROR_INTERNAL, "Column id ", column_id,
              " not found in the index definition");
  }

  // TODO(Dronplane): implement default text dictionary like in PG
  SDB_ASSERT(it->second.text_dictionary.isSet(),
             "Default text dictionary is not implemented.");

  auto snapshot = GetCatalog().GetSnapshot();

  auto dict = snapshot->GetObject<Tokenizer>(it->second.text_dictionary);
  SDB_ENSURE(dict, ERROR_INTERNAL,
             "Dictionary for inverted index does not exists");
  auto tokenizer = dict->GetTokenizer();
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  return {.analyzer = *std::move(tokenizer),
          .features = it->second.features.GetIndexFeatures()};
}

}  // namespace sdb::catalog
