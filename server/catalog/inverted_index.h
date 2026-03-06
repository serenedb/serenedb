#pragma once

#include <iresearch/index/index_features.hpp>

#include "basics/object_pool.hpp"
#include "catalog/index.h"
#include "catalog/search_analyzer_impl.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

struct InvertedIndexOptions {
  // TODO(Dronplane): make this configurable
  std::string_view analyzer_name = "segmentation";
  // We want to run PHRASE queries so need this for now
  irs::IndexFeatures features =
    irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
  bool store_values = false;
};

struct ColumnAnalyzer {
  irs::analysis::Analyzer::ptr analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
};

class InvertedIndex final : public Index {
 public:
  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id,
                IndexOptions<InvertedIndexOptions> options)
    : Index{database_id, schema_id, id, relation_id, std::move(options.base)},
      _options{std::move(options.impl)} {}

  void WriteInternal(vpack::Builder& builder) const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, IndexShardOptions&) const final;

  ColumnAnalyzer GetColumnAnalyzer(catalog::Column::Id columnd_id) const;

 private:
  // TODO(codeworse): Add inverted index specific options
  InvertedIndexOptions _options;
};

}  // namespace sdb::catalog
