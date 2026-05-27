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

#include <__memory/shared_ptr.h>

#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <functional>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
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
#include "connector/rocksdb_filter.hpp"

namespace irs {

class IndexReader;
}

#include "search/inverted_index_shard.h"

namespace sdb::connector {

struct SereneDBScanBindData;

enum class ScanSourceKind : uint8_t {
  FullTable,
  Search,
  Count,
  SecondaryIndex,
  Ann,
  RangeSearch,
  PkPoint,
  PkRange,
  SkPoint,
  SkRange,
};

struct ScanSource {
  ScanSourceKind Kind() const { return _kind; }

  virtual void AppendSummary(
    const SereneDBScanBindData& /*bind*/,
    duckdb::InsertionOrderPreservingMap<std::string>& /*out*/) const {}

  // Subclasses with non-copyable state (e.g. prepared queries) return a
  // default FullTableScan.
  virtual std::unique_ptr<ScanSource> Clone() const = 0;

  bool IsSearchLike() const noexcept {
    return _kind == ScanSourceKind::Search || _kind == ScanSourceKind::Count ||
           _kind == ScanSourceKind::Ann || _kind == ScanSourceKind::RangeSearch;
  }

  bool IsSkLike() const noexcept {
    return _kind == ScanSourceKind::SecondaryIndex ||
           _kind == ScanSourceKind::SkPoint || _kind == ScanSourceKind::SkRange;
  }

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

struct SearchScan : ScanSource {
  SearchScan() : ScanSource(ScanSourceKind::Search) {}

  // The prepared `Query` is built in SearchFullScanInitGlobal so prepare
  // happens exactly once per execution, with the scorer if requested.
  // This struct only carries the unprepared filter; the reader lives on
  // `snapshot` and callers reach it via `snapshot->reader`.
  std::shared_ptr<irs::Filter> stored_filter;
  search::InvertedIndexSnapshotPtr snapshot;
  // Empty when the filter is trivial.
  std::string filter_summary;
  // True when stored_filter is the trivial match-all (irs::All). The
  // bulk-scan shortcut keys on this rather than string-comparing
  // filter_summary.
  bool match_all = false;

  // Scorer parsed from `ORDER BY BM25(idx.tableoid, ...)` / `TFIDF(...)`
  // / etc. Empty when the query has no scoring projection.
  std::optional<catalog::ScorerOptions> scorer;
  std::optional<size_t> score_top_k;

  // Catalog-side topk scorer carried verbatim from the InvertedIndex. WAND
  // pruning is engaged at runtime iff this matches `scorer`.
  std::optional<catalog::ScorerOptions> topk_scorer;

  struct OffsetsRequest {
    catalog::Column::Id column_id;
    size_t limit = std::numeric_limits<size_t>::max();
    duckdb::idx_t get_col_idx = 0;
  };
  std::vector<OffsetsRequest> offsets;

  bool EmitOffsets() const { return !offsets.empty(); }
  bool WandEnabled() const { return topk_scorer && topk_scorer == scorer; }

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

// Iresearch row-count scan: emits zero-column rows whose cardinality equals
// the number of docs matching `query` (or the reader's live_docs_count when
// `query` is null). Swapped in by the iresearch_plan rule in pass 2 when a
// LogicalGet has no projected columns and the underlying SearchScan /
// FullTableScan carries no scorer or offsets. Mirrors the Velox
// SearchCountDataSource design: the aggregate above (count_star()) just sums
// chunk cardinalities, so we never materialise PKs or column values.
struct CountScan : ScanSource {
  CountScan() : ScanSource(ScanSourceKind::Count) {}

  // Null `stored_filter` => match-all short-circuit via
  // IndexReader::live_docs_count(). Otherwise the prepared `Query` is
  // built once in SearchCountScanInitGlobal so every CountScan is
  // prepared exactly once per execution. The reader lives on `snapshot`
  // and callers reach it via `snapshot->reader`.
  std::shared_ptr<irs::Filter> stored_filter;
  search::InvertedIndexSnapshotPtr snapshot;
  // Demangled boolean-filter tree for EXPLAIN. Empty when query is null.
  std::string filter_summary;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

struct SecondaryIndexScan : ScanSource {
  SecondaryIndexScan() : ScanSource(ScanSourceKind::SecondaryIndex) {}

  ObjectId shard_id;
  bool is_unique = false;

  std::unique_ptr<ScanSource> Clone() const final;
};

struct VectorSearchScan : ScanSource {
  VectorSearchScan(ScanSourceKind kind) : ScanSource{kind} {}

  search::InvertedIndexSnapshotPtr snapshot;
  irs::field_id field_id{};
  std::vector<float> query_vector;
  duckdb::unique_ptr<duckdb::Expression> filter_expression;
  std::vector<catalog::Column::Id> filter_column_ids;

  std::unique_ptr<irs::Filter> stored_text_filter;
  irs::Filter* text_filter_root = nullptr;
};

struct ANNScan : VectorSearchScan {
  ANNScan() : VectorSearchScan{ScanSourceKind::Ann} {}

  size_t top_k = 0;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

struct RangeSearchScan : VectorSearchScan {
  RangeSearchScan() : VectorSearchScan{ScanSourceKind::RangeSearch} {}

  // Radius as the user wrote it (in the unit of the requested distance
  // function). Displayed in EXPLAIN.
  float radius = 0.0f;
  // Radius in the unit the iresearch index actually compares against. Equal
  // to `radius` for most metrics; squared when the user wrote l2_distance
  // (`<->`) but the index stores L2-squared distances.
  float effective_radius = 0.0f;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

struct PkPointScan : ScanSource {
  PkPointScan() : ScanSource(ScanSourceKind::PkPoint) {}

  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedPoint> points;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

struct PkRangeScan : ScanSource {
  PkRangeScan() : ScanSource(ScanSourceKind::PkRange) {}

  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedRange> ranges;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

struct SkPointScan : ScanSource {
  SkPointScan() : ScanSource(ScanSourceKind::SkPoint) {}

  ObjectId shard_id;
  bool is_unique = false;
  std::vector<catalog::Column::Id> column_ids;  // SK columns in order
  std::vector<ResolvedPoint> points;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

struct SkRangeScan : ScanSource {
  SkRangeScan() : ScanSource(ScanSourceKind::SkRange) {}

  ObjectId shard_id;
  bool is_unique = false;
  std::vector<catalog::Column::Id> column_ids;  // SK columns in order
  std::vector<ResolvedRange> ranges;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const final;
  std::unique_ptr<ScanSource> Clone() const final;
};

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
  bool has_rowid = false;
  // Set by BindCreateIndex on the underlying LogicalGet's bind data so the
  // scan-init layer knows it is feeding a CREATE INDEX backfill rather than
  // a user query. Used to relax the read-side check on sdb_indexonly columns
  // (the backfill is allowed to project them; for empty/lossless cases it
  // simply finds no data, which is the intended outcome).
  bool is_create_index = false;
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
  bool IsSecondaryIndexEntry() const noexcept {
    return entry_kind == ScanEntryKind::SecondaryIndex;
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

duckdb::TableFunction CreateTableFullscanFunction();

// Full-table scan for search-backed tables (StorageKind::kSearch).
// Selected by SereneDBTableEntry::GetScanFunction when the target
// shard is a SearchTableShard; iresearch columnstore -> DuckDB
// DataChunk via ColumnstoreMaterializer::Scan.
duckdb::TableFunction CreateSearchTableScanFunction();

duckdb::TableFunction CreatePKPointsLookupFunction();

duckdb::TableFunction CreatePKRangesScanFunction();

duckdb::TableFunction CreateSKFullscanFunction();

duckdb::TableFunction CreateSKPointsLookupFunction();

duckdb::TableFunction CreateSKRangesScanFunction();

duckdb::TableFunction CreateIResearchScanFunction();

duckdb::TableFunction CreateIResearchCountFunction();

duckdb::TableFunction CreateIResearchANNScanFunction();

duckdb::TableFunction CreateIResearchANNRangeScanFunction();

}  // namespace sdb::connector
