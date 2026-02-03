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
  InvertedIndex(IndexOptions<InvertedIndexOptions> options,
                ObjectId database_id)
    : Index{std::move(options.base), database_id} {}

  void WriteInternal(vpack::Builder& builder) const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, vpack::Slice args) const final;

 private:
  // TODO(codeworse): Add inverted index specific options
};

}  // namespace sdb::catalog
