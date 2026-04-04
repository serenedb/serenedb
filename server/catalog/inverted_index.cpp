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
#include <iresearch/analysis/tokenizers.hpp>
#include <vpack/serializer.h>

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

std::shared_ptr<InvertedIndex> InvertedIndex::ReadInternal(
  vpack::Slice slice, ReadContext ctx) {
  auto name_slice = slice.get("name");
  if (!name_slice.isString()) {
    return nullptr;
  }

  struct BaseOpts {
    std::vector<Column::Id> column_ids;
  };
  BaseOpts base;
  if (auto r = vpack::ReadTupleNothrow(slice.get("base"), base); !r.ok()) {
    return nullptr;
  }

  ColumnOptions columns;
  if (auto r = vpack::ReadTupleNothrow(slice.get("impl"), columns); !r.ok()) {
    return nullptr;
  }

  return std::make_shared<InvertedIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::string{name_slice.stringView()}, std::move(base.column_ids),
    std::move(columns));
}

void InvertedIndex::WriteInternal(vpack::Builder& b) const {
  WriteObject(b, [&](vpack::Builder& b) {
    struct BaseOpts {
      IndexType type;
      std::span<const Column::Id> column_ids;
    };
    b.add("base");
    vpack::WriteTuple(b, BaseOpts{.type = GetIndexType(), .column_ids = _column_ids});
    b.add("impl");
    vpack::WriteTuple(b, _columns);
  });
}

ColumnAnalyzer InvertedIndex::GetColumnAnalyzer(
  const std::shared_ptr<const Snapshot>& snapshot,
  catalog::Column::Id column_id) const {
  auto it = _columns.find(column_id);
  if (it == _columns.end()) {
    SDB_THROW(ERROR_INTERNAL, "Column id ", column_id,
              " not found in the index definition");
  }

  if (!it->second.text_dictionary.isSet()) {
    auto analyzer = std::make_unique<irs::StringTokenizer>();
    return {.analyzer = Tokenizer::AnalyzerWrapper{
              analyzer.release(), Tokenizer::Deleter{nullptr}}};
  }

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
  for (const auto& col : _columns) {
    if (col.second.text_dictionary.isSet()) {
      res.insert(col.second.text_dictionary);
    }
  }
  return res;
}

}  // namespace sdb::catalog
