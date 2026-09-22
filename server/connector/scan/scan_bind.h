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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <functional>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/scorers/scorer.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"
#include "connector/score_emit.h"
#include "connector/view_fast_path.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {

class SearchTableEntry;

}  // namespace sdb::catalog
namespace sdb::connector {

struct OffsetsBindData;

enum class ScanEntryKind : uint8_t {
  InvertedIndex,
  SearchTable,
  SearchTableIndex,
};

struct SearchSpec {
  std::shared_ptr<irs::Filter> filter;
  std::vector<std::shared_ptr<irs::Scorer>> filter_scorers;
  search::InvertedIndexSnapshotPtr snapshot;

  bool MatchAll() const noexcept { return !filter; }
};

struct ScoreSpec {
  std::optional<catalog::ScorerOptions> text;
  std::optional<VectorScorerOptions> vector;
  std::optional<catalog::ScorerOptions> prune;
  std::optional<duckdb::OrderType> order;
  std::optional<size_t> top_k;
  size_t top_offset = 0;
  bool top_n_consumed = false;
  float static_floor = std::numeric_limits<float>::lowest();
};

struct ScanOrderSpec {
  ColumnId column;
  duckdb::OrderType order_type;
  duckdb::OrderByNullType null_order;
  duckdb::OrderByStatistics order_by;
  duckdb::OrderByColumnType column_type;
};

struct OffsetsRequest {
  ColumnId column_id;
  ColumnId display_id = kInvalidColumnId;
  size_t limit = std::numeric_limits<size_t>::max();
  duckdb::idx_t get_col_idx = 0;
  OffsetsBindData* bind = nullptr;
};

struct OffsetsSpec {
  std::vector<OffsetsRequest> requests;

  bool Active() const noexcept { return !requests.empty(); }
};

enum class TsDictTermUses : uint8_t {
  None = 0,
  Full = 1,
  Min = 2,
  Max = 4,
};

ENABLE_BITMASK_ENUM(TsDictTermUses);

struct TsDictRequest {
  irs::field_id field_id = irs::field_limits::invalid();
  irs::field_id display_id = irs::field_limits::invalid();
  irs::field_id null_field_id = irs::field_limits::invalid();
  std::shared_ptr<irs::Filter> having_filter;
  duckdb::idx_t term_col_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t term_raw_col_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t count_col_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t freq_col_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t score_col_idx = duckdb::DConstants::INVALID_INDEX;
  TsDictTermUses term_uses = TsDictTermUses::None;
};

struct TsDictSpec {
  std::vector<TsDictRequest> requests;

  bool Active() const noexcept { return !requests.empty(); }

  TsDictRequest& For(irs::field_id field_id);
};

struct LookupSpec {
  std::string label;
  bool supports_filters = true;
};

struct ViewSpec {
  duckdb::idx_t id = 0;
  std::string name;
  std::vector<std::string> column_names;
  std::optional<ViewFastPath> fast_path;
};

struct ScanColumns {
  std::vector<ColumnId> ids;
  std::vector<duckdb::LogicalType> types;
};

struct RelationSpec {
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;
  duckdb::optional_ptr<const catalog::InvertedIndexEntry> inverted_index;
  std::shared_ptr<const catalog::InvertedIndexConfig> inverted_config;
  ScanEntryKind kind = ScanEntryKind::InvertedIndex;
  uint32_t row_group_size = 0;

  bool IsInvertedIndex() const noexcept {
    return kind != ScanEntryKind::SearchTable;
  }
  bool IsSearchTable() const noexcept {
    return kind != ScanEntryKind::InvertedIndex;
  }
  const catalog::InvertedIndexConfig& ScannedIndex() const noexcept {
    SDB_ASSERT(inverted_config);
    return *inverted_config;
  }
  catalog::IndexTokenizers ResolveTokenizers(
    duckdb::ClientContext& context) const {
    return {context,
            inverted_index ? inverted_index->catalog : table_entry->catalog,
            *inverted_config};
  }
};

struct ScanBindData final : duckdb::FunctionData {
  ScanColumns columns;
  RelationSpec relation;
  SearchSpec search;
  ScoreSpec score;
  std::optional<ScanOrderSpec> scan_order;
  OffsetsSpec offsets;
  TsDictSpec ts_dict;
  LookupSpec lookup;
  std::optional<ViewSpec> view;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<ScanBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final;

  bool IsViewBacked() const noexcept { return view.has_value(); }
  bool IsMatchAll() const noexcept {
    return search.MatchAll() && !score.vector.has_value();
  }

  using ColumnVisitor =
    std::function<void(ColumnId, const duckdb::LogicalType&)>;

  std::string_view ColumnNameById(ColumnId col_id) const;
  duckdb::LogicalType ColumnTypeById(ColumnId col_id) const;
  std::string DisplayColumnName(ColumnId col_id) const;
  bool IsColumnNotNull(ColumnId col_id) const;
  void IterateColumns(const ColumnVisitor& cb) const;

  bool IsHnswScored() const noexcept;

  duckdb::idx_t RelationId() const {
    return view ? view->id : relation.table_entry->oid;
  }
  std::string_view RelationName() const;
  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const;

  void AppendSummary(
    duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) const;
};

duckdb::unique_ptr<duckdb::FunctionData> ScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names);

inline bool IsSereneDBScan(const duckdb::LogicalGet& get) {
  return get.function.bind == &ScanBind;
}

duckdb::TableFunction BindSearchTableScan(
  duckdb::ClientContext& context, catalog::SearchTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data);

std::optional<duckdb::LogicalType> GeneratedPkTypeOf(const ScanBindData& bind);

std::optional<PkSpec> ViewPkSpecOf(const ScanBindData& bind);

inline const irs::Scorer* ResolvePruneScorer(
  const std::optional<catalog::ScorerOptions>& topk,
  const irs::Scorer* scorer) {
  return topk && scorer && scorer->Compatible(*topk) ? scorer : nullptr;
}

}  // namespace sdb::connector
