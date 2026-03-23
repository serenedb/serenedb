////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

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

struct InvertedIndexOptionsWrapper : public IndexImplOptionsBaseWrapper {
  InvertedIndexOptionsWrapper(IndexBaseOptions&& options)
    : IndexImplOptionsBaseWrapper{std::move(options)} {}

  InvertedIndexOptionsImpl impl;
};

struct ColumnAnalyzer {
  Tokenizer::AnalyzerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
};

class InvertedIndex final : public Index {
 public:
  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id, InvertedIndexOptionsWrapper options)
    : Index{database_id, schema_id, id, relation_id, std::move(options.base)},
      _options{std::move(options.impl)} {}

  void WriteInternal(vpack::Builder& builder) const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, IndexShardOptions&) const final;

  ColumnAnalyzer GetColumnAnalyzer(catalog::Column::Id columnd_id) const;

  containers::FlatHashSet<ObjectId> GetTokenizers() const final;

 private:
  // TODO(codeworse): Add inverted index specific options
  InvertedIndexOptionsImpl _options;
};

}  // namespace sdb::catalog
