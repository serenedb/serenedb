#pragma once

#include <iresearch/index/index_features.hpp>

#include "catalog/index.h"

namespace sdb::catalog {

struct InvertedIndexOptions {
  std::string_view analyzer_name;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  bool store_values = false;
};

class InvertedIndex final : public Index {
 public:
  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id,
                IndexOptions<InvertedIndexOptions> options)
    : Index{database_id, schema_id, id, relation_id, std::move(options.base)} {}

  void WriteInternal(vpack::Builder& builder) const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, vpack::Slice args) const final;

 private:
  // TODO(codeworse): Add inverted index specific options
};

}  // namespace sdb::catalog
