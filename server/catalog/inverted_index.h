#pragma once

#include <iresearch/index/index_features.hpp>

#include "basics/object_pool.hpp"
#include "catalog/index.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

struct InvertedIndexColumnInfo {
  ObjectId text_dictionary = ObjectId::none();
  bool store_values = false;
  search::Features features;
};

struct InvertedIndexOptionsImpl {
  containers::FlatHashMap<Column::Id, InvertedIndexColumnInfo> columns;
};

struct ColumnAnalyzer {
  Tokenizer::AnalyzerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
};

using InvertedIndexOptions = IndexOptions<InvertedIndexOptionsImpl>;

class InvertedIndex final : public Index {
 public:
  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id, InvertedIndexOptions options)
    : Index{database_id, schema_id, id, relation_id, std::move(options.base)},
      _options{std::move(options.impl)} {}

  void WriteInternal(vpack::Builder& builder) const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, IndexShardOptions&) const final;

  ColumnAnalyzer GetColumnAnalyzer(catalog::Column::Id columnd_id) const;

 private:
  // TODO(codeworse): Add inverted index specific options
  InvertedIndexOptionsImpl _options;
};

}  // namespace sdb::catalog
