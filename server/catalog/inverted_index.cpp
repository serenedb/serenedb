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

#include <faiss/MetricType.h>
#include <vpack/serializer.h>

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, ObjectId id, IndexShardOptions& options) const {
  auto& shard_options =
    basics::downCast<search::InvertedIndexShardOptions>(options);
  return search::InvertedIndexShard::Create(id, *this, shard_options, is_new);
}

std::shared_ptr<InvertedIndex> InvertedIndex::ReadInternal(vpack::Slice slice,
                                                           ReadContext ctx) {
  auto name_slice = slice.get("name");
  if (!name_slice.isString()) {
    return nullptr;
  }

  std::vector<Column::Id> column_ids;
  if (auto r = vpack::ReadTupleNothrow(slice.get("column_ids"), column_ids);
      !r.ok()) {
    return nullptr;
  }

  ColumnOptions columns;
  if (auto r = vpack::ReadTupleNothrow(slice.get("columns"), columns);
      !r.ok()) {
    return nullptr;
  }

  return std::make_shared<InvertedIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::string{name_slice.stringView()}, std::move(column_ids),
    std::move(columns));
}

void InvertedIndex::WriteInternal(vpack::Builder& b) const {
  b.openObject();
  WriteObject(b, [&](vpack::Builder& b) {
    b.add("column_ids");
    vpack::WriteTuple(b, _column_ids);
    b.add("columns");
    vpack::WriteTuple(b, _columns);
  });
  b.close();
}

ColumnTokenizer InvertedIndex::GetColumnAnalyzer(
  const std::shared_ptr<const Snapshot>& snapshot,
  catalog::Column::Id column_id) const {
  auto it = _columns.find(column_id);
  if (it == _columns.end()) {
    SDB_THROW(ERROR_INTERNAL, "Column id ", column_id,
              " not found in the index definition");
  }

  if (!it->second.text_dictionary.isSet()) {
    auto analyzer = std::make_unique<irs::StringTokenizer>();
    return {.analyzer = {analyzer.release(), Tokenizer::Deleter{nullptr}}};
  }

  auto dict = snapshot->GetObject<Tokenizer>(it->second.text_dictionary);
  SDB_ENSURE(dict, ERROR_INTERNAL,
             "Dictionary for inverted index does not exists");
  auto tokenizer = dict->GetTokenizer();
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  return {.analyzer = *std::move(tokenizer),
          .features = it->second.features.GetIndexFeatures()};
}

std::optional<irs::HNSWInfo> InvertedIndex::GetColumnHNSWInfo(
  catalog::Column::Id column_id) const {
  auto it = _columns.find(column_id);
  if (it == _columns.end() || !it->second.hnsw_config) {
    return std::nullopt;
  }
  const auto& cfg = *it->second.hnsw_config;
  return irs::HNSWInfo{
    .max_doc = 0,
    .d = cfg.d,
    .m = cfg.m,
    .metric = cfg.metric,
    .ef_construction = cfg.ef_construction,
  };
}

containers::FlatHashSet<ObjectId> InvertedIndex::GetTokenizers() const {
  containers::FlatHashSet<ObjectId> res;
  for (const auto& col : _columns) {
    if (col.second.text_dictionary.isSet()) {
      res.insert(col.second.text_dictionary);
    }
  }
  return res;
}

std::shared_ptr<Object> InvertedIndex::Clone() const {
  vpack::Builder b;
  WriteInternal(b);
  return ReadInternal(b.slice(), {
                                   .id = GetId(),
                                   .database_id = GetDatabaseId(),
                                   .schema_id = GetSchemaId(),
                                   .relation_id = GetRelationId(),
                                 });
}

}  // namespace sdb::catalog
