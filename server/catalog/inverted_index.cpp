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
  Index::WriteInternalImpl(builder,
                           [&] { vpack::WriteTuple(builder, _options); });
}

ColumnAnalyzer InvertedIndex::GetColumnAnalyzer(
  catalog::Column::Id column_id) const {
  auto it = _options.columns.find(column_id);
  if (it == _options.columns.end()) {
    SDB_THROW(ERROR_INTERNAL, "Column id ", column_id,
              " not found in the index definition");
  }

  if (!it->second.text_dictionary.isSet()) {
    return {};
  }

  auto snapshot = GetCatalog().GetSnapshot();

  auto dict = snapshot->GetObject<Tokenizer>(it->second.text_dictionary);
  SDB_ENSURE(dict, ERROR_INTERNAL,
             "Dictionary for inverted index does not exists");
  auto tokenizer = dict->GetTokenizer();
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  return {.analyzer = *std::move(tokenizer),
          .features = it->second.features.GetIndexFeatures()};
}

containers::FlatHashSet<ObjectId> InvertedIndex::GetTokenizers() const {
  containers::FlatHashSet<ObjectId> res;
  for (const auto& col : _options.columns) {
    if (col.second.text_dictionary.isSet()) {
      res.insert(col.second.text_dictionary);
    }
  }
  return res;
}

}  // namespace sdb::catalog
