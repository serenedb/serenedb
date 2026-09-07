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

#include <cmath>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <functional>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <optional>
#include <string_view>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/down_cast.h"
#include "basics/system-compiler.h"
#include "catalog1/entry/inverted_index.h"
#include "connector/view_fast_path.h"

namespace irs {

class IndexReader;
}

#include "connector/column_id.h"
#include "search/inverted_index_storage.h"

namespace sdb::catalog {

class SearchTableEntry;
class SystemTableEntry;

}  // namespace sdb::catalog
namespace sdb::connector {

struct OffsetsBindData;

struct SereneDBScanBindData;

// Maps the per-doc score to the user-facing value. The score is already
// "larger = nearer" for every metric (ResolveScoringDistance negates the
// distance kernels), so the emit is applied directly -- no negation here.
enum class ScoreEmit : uint8_t {
  Identity,  // score        (cosine_similarity, inner_product)
  SqrtNeg,   // sqrt(-score) (l2_distance / `<->` / l2_norm)
  OneMinus,  // 1 - score    (cosine_distance)
  Negate,    // -score       (l1, l2_sqr, negative_ip, l1_norm)
};

// Maps one raw "larger = nearer" score to its user-facing value. Single source
// of truth: applied to the output scores at the emit boundary, and baked into
// the pushed score-column filter so the predicate is evaluated in the same
// (user-facing) space it was written in.
inline float ApplyScoreEmit(ScoreEmit emit, float score) {
  switch (emit) {
    case ScoreEmit::Identity:
      return score;
    case ScoreEmit::SqrtNeg:
      return std::sqrt(-score);
    case ScoreEmit::OneMinus:
      return 1.0F - score;
    case ScoreEmit::Negate:
      return -score;
  }
  SDB_UNREACHABLE();
}

struct VectorScorerOptions {
  irs::field_id field_id;
  std::vector<float> query_vector;
  irs::VectorMetric metric;
  ScoreEmit score_emit;
  duckdb::OrderType natural_order;
  irs::field_id centroids_id = irs::field_limits::invalid();
  irs::field_id postings_id = irs::field_limits::invalid();
  irs::VectorQuantization quant = irs::VectorQuantization::None;
  uint32_t nprobe = 1;
  float radius = std::numeric_limits<float>::max();
  bool radius_inclusive = false;

  float EffectiveRadius() const {
    if (radius == std::numeric_limits<float>::max()) {
      return radius;
    }
    // The radius filter runs on the natural (positive) distance kernel, so map
    // the user radius into that space -- the collector-side score negation does
    // not apply here.
    switch (score_emit) {
      case ScoreEmit::Identity:
        return radius;
      case ScoreEmit::SqrtNeg:
        return radius * radius;
      case ScoreEmit::OneMinus:
        return 1.0f - radius;
      case ScoreEmit::Negate:
        return (metric == irs::VectorMetric::L2Sqr ||
                metric == irs::VectorMetric::L1)
                 ? radius
                 : -radius;
    }
    SDB_UNREACHABLE();
  }
};

irs::Filter::ptr MakeVectorFilter(const VectorScorerOptions& vs,
                                  std::shared_ptr<const irs::Filter> inner,
                                  float radius);

enum class TsDictTermUses : uint8_t {
  kNone = 0,
  kFull = 1,
  kMin = 2,
  kMax = 4,
};

ENABLE_BITMASK_ENUM(TsDictTermUses);

// The scorer `index`'s persisted per-block bounds may be pruned against, or
// null when they cannot be: no index, no query scorer, no bounds, or bounds a
// different scorer wrote.
const irs::Scorer* ResolvePruneScorer(
  const std::optional<catalog::ScorerOptions>& index_top_k_scorer,
  const irs::Scorer* scorer);

enum class ScanEntryKind : uint8_t {
  BaseTable,
  InvertedIndex,
  // A TableEngine::Search table: its iresearch store IS the table, so every
  // column is covered in `.col` and there is no separate lookup source.
  SearchTable,
};

struct SereneDBScanBindData : public duckdb::FunctionData {
  enum class Kind : uint8_t {
    Table,
    View,
  };

  std::vector<ColumnId> column_ids;
  std::vector<duckdb::LogicalType> column_types;
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;
  ScanEntryKind entry_kind = ScanEntryKind::BaseTable;

  duckdb::optional_ptr<const catalog::InvertedIndexEntry> inverted_index;
  std::shared_ptr<const catalog::InvertedIndexConfig> inverted_config;
  std::optional<catalog::ScorerOptions> index_top_k_scorer;

  // The iresearch snapshot plus the query's search configuration (stored
  // filter, scorer, offsets, ts-dict requests). Every scan bound through this
  // table function is a search scan, so `snapshot` is always set.
  std::shared_ptr<irs::Filter> stored_filter;
  std::vector<std::shared_ptr<irs::Scorer>> filter_scorers;
  search::InvertedIndexSnapshotPtr snapshot;

  std::optional<catalog::ScorerOptions> text_scorer;
  std::optional<VectorScorerOptions> vector_scorer;
  std::optional<size_t> score_top_k;
  std::optional<duckdb::OrderType> score_order;

  // Static score lower bound consumed at filter pushdown (Lucene min_score):
  // a text score filter that IS a lower bound (`score > c` / `>= c`) is
  // dropped from the plan and enforced by this floor instead -- the emitted
  // scores are compacted with `score > floor`, the top-k collectors start at
  // it, and it seeds the streaming prune threshold. lowest() = no bound.
  float score_static_floor = std::numeric_limits<float>::lowest();

  // ORDER BY <covered .col column> LIMIT accepted via set_scan_order: segments
  // are iterated best-first by the column's per-file statistics, the
  // whole-file analogue of duckdb's RowGroupReorderer.
  struct ScanOrder {
    ColumnId column;
    duckdb::OrderType order_type;
    duckdb::OrderByNullType null_order;
    duckdb::OrderByStatistics order_by;
    duckdb::OrderByColumnType column_type;
  };
  std::optional<ScanOrder> scan_order;

  struct OffsetsRequest {
    ColumnId column_id;
    size_t limit = std::numeric_limits<size_t>::max();
    duckdb::idx_t get_col_idx = 0;
    OffsetsBindData* bind = nullptr;
  };
  std::vector<OffsetsRequest> offsets;

  struct TsDictRequest {
    irs::field_id field_id = irs::field_limits::invalid();
    // Valid for a nullable facet: the scan appends a per-segment NULL-term
    // row counting the null-marker field under the claimed document filter.
    irs::field_id null_field_id = irs::field_limits::invalid();
    std::shared_ptr<irs::Filter> having_filter;
    duckdb::idx_t term_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t term_raw_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t count_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t freq_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t score_col_idx = duckdb::DConstants::INVALID_INDEX;
    TsDictTermUses term_uses = TsDictTermUses::kNone;
  };
  // One entry per enumerated field; multiple ts_dict aggregations over
  // different fields in one query each get their own request.
  std::vector<TsDictRequest> ts_dicts;

  std::string lookup_label;
  // Whether the lookup source applies pushed table filters (native storage +
  // parquet yes; csv/json/text no). Filters on lookup columns are pushed only
  // when true -- see IResearchSupportsPushdownType. Table-backed (sdb store)
  // scans keep the default; view-backed scans set it from the fast path.
  bool lookup_supports_filters = true;

  bool IsMatchAll() const noexcept { return !stored_filter && !vector_scorer; }
  bool EmitOffsets() const { return !offsets.empty(); }
  bool TsDictMode() const { return !ts_dicts.empty(); }
  TsDictRequest& TsDictFor(irs::field_id field_id) {
    const auto it =
      std::ranges::find(ts_dicts, field_id, &TsDictRequest::field_id);
    if (it != ts_dicts.end()) {
      return *it;
    }
    return ts_dicts.emplace_back(TsDictRequest{.field_id = field_id});
  }
  void AppendSummary(
    duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) const;

  Kind GetKind() const noexcept { return _kind; }
  bool IsViewBacked() const noexcept { return _kind == Kind::View; }
  bool IsInvertedIndexEntry() const noexcept {
    return entry_kind == ScanEntryKind::InvertedIndex;
  }
  bool IsSearchTableEntry() const noexcept {
    return entry_kind == ScanEntryKind::SearchTable;
  }

  template<typename T>
  T& As() & {
    return basics::downCast<T>(*this);
  }
  template<typename T>
  const T& As() const& {
    return basics::downCast<const T>(*this);
  }

  virtual duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const = 0;

  virtual duckdb::idx_t RelationId() const = 0;

  virtual std::string_view RelationName() const = 0;

  virtual ColumnId ColumnIdByName(std::string_view name) const = 0;

  virtual std::string_view ColumnNameById(ColumnId col_id) const = 0;

  virtual duckdb::LogicalType ColumnTypeById(ColumnId col_id) const = 0;

  std::string DisplayColumnName(ColumnId col_id) const;

  using ColumnVisitor =
    std::function<void(ColumnId, const duckdb::LogicalType&)>;
  virtual void IterateColumns(const ColumnVisitor& cb) const = 0;

  bool IsColumnNotNull(ColumnId col_id) const;

 protected:
  explicit SereneDBScanBindData(Kind k) : _kind{k} {}

 private:
  Kind _kind;
};

// A scan of a relation's own rows -- the base table, its inverted index read
// by name, or a search table. Everything about the columns is read off
// `table_entry`, which lists them with the same ids the postings carry.
struct TableScanBindData final : public SereneDBScanBindData {
  TableScanBindData() : SereneDBScanBindData(Kind::Table) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final;
  bool Equals(const duckdb::FunctionData& other) const final;

  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const final;
  duckdb::idx_t RelationId() const final;
  std::string_view RelationName() const final;
  ColumnId ColumnIdByName(std::string_view name) const final;
  std::string_view ColumnNameById(ColumnId col_id) const final;
  duckdb::LogicalType ColumnTypeById(ColumnId col_id) const final;
  void IterateColumns(const ColumnVisitor& cb) const final;
};

struct ViewScanBindData final : public SereneDBScanBindData {
  // The view the scan projects, as the scan reads it: its identity, its name
  // and its column names, which is everything the accessors below answer. The
  // types are the column_types above. The definition stays on the entry.
  duckdb::idx_t view_id;
  std::string view_name;
  std::vector<std::string> column_names;
  std::optional<ViewFastPath> fast_path;

  ViewScanBindData() : SereneDBScanBindData(Kind::View) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final;
  bool Equals(const duckdb::FunctionData& other) const final;

  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const final;
  duckdb::idx_t RelationId() const final;
  std::string_view RelationName() const final;
  ColumnId ColumnIdByName(std::string_view name) const final;
  std::string_view ColumnNameById(ColumnId col_id) const final;
  duckdb::LogicalType ColumnTypeById(ColumnId col_id) const final;
  void IterateColumns(const ColumnVisitor& cb) const final;
};

duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names);

inline bool IsSereneDBScan(const duckdb::LogicalGet& get) {
  return get.bind_data && get.function.bind == &SereneDBScanBind;
}

uint32_t ReadBoundedIntSetting(duckdb::ClientContext& context,
                               std::string_view name, int32_t min_inclusive,
                               uint32_t default_value);

std::optional<duckdb::LogicalType> GeneratedPkTypeOf(
  const SereneDBScanBindData& bind);

std::optional<PkSpec> ViewPkSpecOf(const SereneDBScanBindData& bind);

duckdb::TableFunction CreateIResearchScanFunction();

duckdb::TableFunction BindSearchTableScan(
  catalog::SearchTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data);

duckdb::TableFunction BindSystemTableScan(
  catalog::SystemTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data);

void RegisterIResearchScanFunction(duckdb::DatabaseInstance& db);
void RegisterSystemTableScanFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
