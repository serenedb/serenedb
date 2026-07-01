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
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <functional>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/utils/string.hpp>
#include <memory>
#include <optional>
#include <string_view>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/table.h"
#include "catalog/view.h"

namespace irs {

class IndexReader;
}

#include "search/inverted_index_storage.h"

namespace sdb::connector {

struct OffsetsBindData;

struct SereneDBScanBindData;

enum class ScanSourceKind : uint8_t {
  FullTable,
  Search,
};

struct ScanSource {
  ScanSourceKind Kind() const { return _kind; }

  virtual void AppendSummary(
    const SereneDBScanBindData& /*bind*/,
    duckdb::InsertionOrderPreservingMap<std::string>& /*out*/) const {}

  // Subclasses with non-copyable state (e.g. prepared queries) return a
  // default FullTableScan.
  virtual std::unique_ptr<ScanSource> Clone() const = 0;

  bool IsSearchLike() const noexcept { return _kind == ScanSourceKind::Search; }

  template<typename T>
  const T& Cast() const {
    auto* p = basics::downCast<T>(this);
    SDB_ASSERT(p != nullptr, "ScanSource::Cast: null result");
    return *p;
  }
  template<typename T>
  T& Cast() {
    auto* p = basics::downCast<T>(this);
    SDB_ASSERT(p != nullptr, "ScanSource::Cast: null result");
    return *p;
  }

  virtual ~ScanSource() = default;

 protected:
  explicit ScanSource(ScanSourceKind k) : _kind{k} {}

 private:
  ScanSourceKind _kind;
};

struct FullTableScan : ScanSource {
  FullTableScan() : ScanSource(ScanSourceKind::FullTable) {}
  std::unique_ptr<ScanSource> Clone() const final;
};

enum class ScoreEmit : uint8_t {
  Identity,  // stored as-is (l1, l2_sqr, cosine_distance, negative_ip, l1_norm)
  Sqrt,      // sqrt(stored)        (l2_distance / `<->` / l2_norm)
  OneMinus,  // 1 - stored          (cosine_similarity)
  Negate,    // -stored             (inner_product, from NegativeIP storage)
};

struct VectorScorerOptions {
  irs::field_id field_id;
  std::vector<float> query_vector;
  irs::HNSWMetric metric;
  ScoreEmit score_emit;
  duckdb::OrderType natural_order;
  float radius = std::numeric_limits<float>::max();

  float EffectiveRadius() const {
    if (radius == std::numeric_limits<float>::max()) {
      return radius;
    }
    return score_emit == ScoreEmit::Sqrt ? radius * radius : radius;
  }

  float TransformDistance(float stored) const {
    switch (score_emit) {
      case ScoreEmit::Identity:
        return stored;
      case ScoreEmit::Sqrt:
        return std::sqrt(stored);
      case ScoreEmit::OneMinus:
        return 1.0f - stored;
      case ScoreEmit::Negate:
        return -stored;
    }
    SDB_UNREACHABLE();
  }
};

struct SearchScan : ScanSource {
  SearchScan() : ScanSource(ScanSourceKind::Search) {}

  // The prepared `Query` is built in SearchFullScanInitGlobal so prepare
  // happens exactly once per execution, with the scorer if requested.
  // This struct only carries the unprepared filter; the reader lives on
  // `snapshot` and callers reach it via `snapshot->reader`.
  std::shared_ptr<irs::Filter> stored_filter;
  search::InvertedIndexSnapshotPtr snapshot;

  bool IsMatchAll() const noexcept;

  std::optional<catalog::ScorerOptions> text_scorer;
  std::optional<VectorScorerOptions> vector_scorer;
  std::optional<size_t> score_top_k;

  struct OffsetsRequest {
    catalog::Column::Id column_id;
    size_t limit = std::numeric_limits<size_t>::max();
    duckdb::idx_t get_col_idx = 0;
    OffsetsBindData* bind = nullptr;
  };
  std::vector<OffsetsRequest> offsets;

  bool EmitOffsets() const { return !offsets.empty(); }

  struct TsDictRequest {
    irs::field_id field_id = irs::field_limits::invalid();
    duckdb::idx_t term_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t term_raw_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t count_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t freq_col_idx = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t score_col_idx = duckdb::DConstants::INVALID_INDEX;
  };
  // One entry per enumerated field; multiple ts_dict aggregations over
  // different fields in one query each get their own request.
  std::vector<TsDictRequest> ts_dicts;

  bool TsDictMode() const { return !ts_dicts.empty(); }

  TsDictRequest& TsDictFor(irs::field_id field_id) {
    for (auto& req : ts_dicts) {
      if (req.field_id == field_id) {
        return req;
      }
    }
    ts_dicts.push_back(TsDictRequest{.field_id = field_id});
    return ts_dicts.back();
  }

  bool count_only = false;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

bool WandEnabled(const catalog::InvertedIndex* index,
                 const std::optional<catalog::ScorerOptions>& scorer);

enum class ScanEntryKind : uint8_t {
  BaseTable,
  InvertedIndex,
  SecondaryIndex,
};

constexpr catalog::Column::Id kInvalidColumnId = catalog::Column::kInvalidId;

struct SereneDBScanBindData : public duckdb::FunctionData {
  enum class Kind : uint8_t { Table, View };

  std::vector<catalog::Column::Id> column_ids;
  std::vector<duckdb::LogicalType> column_types;
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;
  ScanEntryKind entry_kind = ScanEntryKind::BaseTable;

  // Null for base-table and secondary-index scans.
  std::shared_ptr<const catalog::InvertedIndex> inverted_index;

  std::unique_ptr<ScanSource> scan_source = std::make_unique<FullTableScan>();

  // EXPLAIN "Lookup:" label. Empty for non-index scans.
  std::string lookup_label;

  Kind GetKind() const noexcept { return _kind; }
  bool IsViewBacked() const noexcept { return _kind == Kind::View; }
  bool IsInvertedIndexEntry() const noexcept {
    return entry_kind == ScanEntryKind::InvertedIndex;
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

  virtual ObjectId RelationId() const = 0;

  virtual std::string_view RelationName() const = 0;

  // Returns kInvalidColumnId when the name is not on the relation.
  virtual catalog::Column::Id ColumnIdByName(std::string_view name) const = 0;

  // Returns an empty view when the id is not on the relation.
  virtual std::string_view ColumnNameById(catalog::Column::Id col_id) const = 0;

  virtual duckdb::LogicalType ColumnTypeById(
    catalog::Column::Id col_id) const = 0;

  using ColumnVisitor =
    std::function<void(catalog::Column::Id, const duckdb::LogicalType&)>;
  virtual void IterateColumns(const ColumnVisitor& cb) const = 0;

 protected:
  explicit SereneDBScanBindData(Kind k) : _kind{k} {}

 private:
  Kind _kind;
};

struct TableScanBindData final : public SereneDBScanBindData {
  std::shared_ptr<catalog::Table> table;

  TableScanBindData() : SereneDBScanBindData(Kind::Table) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final;
  bool Equals(const duckdb::FunctionData& other) const final;

  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const final;
  ObjectId RelationId() const final;
  std::string_view RelationName() const final;
  catalog::Column::Id ColumnIdByName(std::string_view name) const final;
  std::string_view ColumnNameById(catalog::Column::Id col_id) const final;
  duckdb::LogicalType ColumnTypeById(catalog::Column::Id col_id) const final;
  void IterateColumns(const ColumnVisitor& cb) const final;
};

struct ViewScanBindData final : public SereneDBScanBindData {
  std::shared_ptr<const catalog::PgSqlView> view;

  ViewScanBindData() : SereneDBScanBindData(Kind::View) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final;
  bool Equals(const duckdb::FunctionData& other) const final;

  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const final;
  ObjectId RelationId() const final;
  std::string_view RelationName() const final;
  catalog::Column::Id ColumnIdByName(std::string_view name) const final;
  std::string_view ColumnNameById(catalog::Column::Id col_id) const final;
  duckdb::LogicalType ColumnTypeById(catalog::Column::Id col_id) const final;
  void IterateColumns(const ColumnVisitor& cb) const final;
};

// Public bind callback shared by every SereneDB scan -- IsSereneDBScan
// checks for it.
duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names);

inline bool IsSereneDBScan(const duckdb::LogicalGet& get) {
  return get.bind_data && get.function.bind == &SereneDBScanBind;
}

// Full-table scan for search-backed (TableEngine::Search) tables. Selected by
// SereneDBTableEntry::GetScanFunction; iresearch columnstore -> DuckDB
// DataChunk via ColumnstoreMaterializer::Scan.
duckdb::TableFunction CreateSearchTableScanFunction();

duckdb::TableFunction CreateIResearchScanFunction();

// True when the scan projects no real columns (e.g. COUNT(*)), so the operator
// can answer from the live row count instead of materialising columns.
bool IsCountOnlyScan(const SereneDBScanBindData& bind_data,
                     const duckdb::TableFunctionInitInput& input);

inline auto MakeFieldNameResolver(const SereneDBScanBindData& bind_data,
                                  const catalog::InvertedIndex& index) {
  return [&bind_data, &index](catalog::Column::Id col_id) -> std::string {
    const auto fid = static_cast<irs::field_id>(col_id);
    auto base = std::string{bind_data.ColumnNameById(col_id)};
    const auto column_type = bind_data.ColumnTypeById(col_id);
    const bool found_type = column_type.id() != duckdb::LogicalTypeId::INVALID;
    const auto lookup = index.LookupField(fid);
    auto entry_base = [&](irs::field_id entry_fid) {
      std::string s;
      const auto* expr = index.ExpressionByFieldId(entry_fid);
      if (expr && !expr->pretty_printed.empty()) {
        s = expr->pretty_printed;
      } else {
        s = bind_data.ColumnNameById(catalog::Column::Id{entry_fid});
      }
      if (s.empty()) {
        s = absl::StrCat("col", entry_fid);
      }
      return s;
    };
    if (lookup.entry_field_id == catalog::term_dict::kPKFieldId) {
      const auto name =
        bind_data.ColumnNameById(catalog::Column::kGeneratedPKId);
      return std::string{name.empty() ? std::string_view{"sdb_generated_pk"}
                                      : name} +
             "(pk)";
    }
    if (lookup.entry) {
      const auto& entry = *lookup.entry;
      if (fid == lookup.entry_field_id) {
        const auto* expr = index.ExpressionByFieldId(fid);
        if (base.empty() && expr && !expr->pretty_printed.empty()) {
          base = expr->pretty_printed;
        }
        if (base.empty()) {
          base = absl::StrCat("col", fid);
        }
        if (expr) {
          catalog::InvertedIndex::AppendKindSuffix(base, expr->return_type);
        } else if (found_type) {
          catalog::InvertedIndex::AppendKindSuffix(base, column_type);
        } else if (entry.text_dictionary.isSet()) {
          base += "(string)";
        }
        return base;
      }
      if (fid == entry.null_field_id) {
        return entry_base(lookup.entry_field_id) + "(null)";
      }
      if (fid == entry.bool_field_id) {
        return entry_base(lookup.entry_field_id) + "(bool)";
      }
      if (fid == entry.numeric_field_id) {
        return entry_base(lookup.entry_field_id) + "(numeric)";
      }
      if (fid == entry.synthetic_column) {
        return entry_base(lookup.entry_field_id) + "(synthetic)";
      }
    }
    if (base.empty()) {
      base = absl::StrCat("col", fid);
    }
    if (found_type) {
      catalog::InvertedIndex::AppendKindSuffix(base, column_type);
    }
    return base;
  };
}

}  // namespace sdb::connector
