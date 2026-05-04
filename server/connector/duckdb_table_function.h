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

  template<class T>
  const T& Cast() const {
    auto* p = basics::downCast<T>(this);
    SDB_ASSERT(p != nullptr, "ScanSource::Cast: null result");
    return *p;
  }
  template<class T>
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
  std::unique_ptr<ScanSource> Clone() const override;
};

struct SearchScan : ScanSource {
  SearchScan() : ScanSource(ScanSourceKind::Search) {}

  irs::Filter::Query::ptr query;
  // Held to outlive `query`: prepare() is const but stats need the scorer.
  std::shared_ptr<irs::Filter> stored_filter;
  search::InvertedIndexSnapshotPtr snapshot;
  const irs::IndexReader* reader = nullptr;
  // Empty when the filter is trivial.
  std::string filter_summary;

  enum class ScorerKind : uint8_t {
    None,
    Bm25,
    Tfidf,
    RawTf,
    LmJm,
    LmDirichlet,
    IndriDirichlet,
    Dfi,
  };
  // Must stay in sync with irs::DFIMeasure.
  enum class DfiMeasure : uint8_t {
    Standardized,
    Saturated,
    ChiSquared,
  };
  struct ScorerParams {
    struct Bm25 {
      double k1 = 1.2;
      double b = 0.75;
    };
    struct Tfidf {
      bool with_norms = false;
    };
    struct LmJm {
      double lambda = 0.1;
    };
    struct LmDirichlet {
      double mu = 2000.0;
    };
    struct IndriDirichlet {
      double mu = 2000.0;
    };
    struct Dfi {
      DfiMeasure measure = DfiMeasure::Standardized;
    };

    ScorerKind kind = ScorerKind::None;
    union {
      Bm25 bm25{};
      Tfidf tfidf;
      LmJm lm_jm;
      LmDirichlet lm_dirichlet;
      IndriDirichlet indri_dirichlet;
      Dfi dfi;
    };
  };
  ScorerParams scorer;
  std::optional<size_t> score_top_k;

  struct OffsetsRequest {
    catalog::Column::Id column_id;
    size_t limit = 0;
  };
  std::vector<OffsetsRequest> offsets;

  bool EmitOffsets() const { return !offsets.empty(); }

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
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

  // Null => match-all short-circuit via IndexReader::live_docs_count().
  irs::Filter::Query::ptr query;
  // Kept alive like SearchScan.stored_filter so filter_summary references
  // remain valid and for debuggability.
  std::shared_ptr<irs::Filter> stored_filter;
  search::InvertedIndexSnapshotPtr snapshot;
  const irs::IndexReader* reader = nullptr;
  // Demangled boolean-filter tree for EXPLAIN. Empty when query is null.
  std::string filter_summary;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

struct SecondaryIndexScan : ScanSource {
  SecondaryIndexScan() : ScanSource(ScanSourceKind::SecondaryIndex) {}

  ObjectId shard_id;
  bool is_unique = false;

  std::unique_ptr<ScanSource> Clone() const override;
};

struct VectorSearchScan : ScanSource {
  VectorSearchScan(ScanSourceKind kind) : ScanSource{kind} {}

  ObjectId index_id;
  std::string field_name;
  std::vector<float> query_vector;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> filter_expressions;
  std::vector<catalog::Column::Id> filter_column_ids;
};

struct ANNScan : VectorSearchScan {
  ANNScan() : VectorSearchScan{ScanSourceKind::Ann} {}

  size_t top_k = 0;
  int ef_search = 0;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

struct RangeSearchScan : VectorSearchScan {
  RangeSearchScan() : VectorSearchScan{ScanSourceKind::RangeSearch} {}

  float radius = 0.0f;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

struct PkPointScan : ScanSource {
  PkPointScan() : ScanSource(ScanSourceKind::PkPoint) {}

  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedPoint> points;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

struct PkRangeScan : ScanSource {
  PkRangeScan() : ScanSource(ScanSourceKind::PkRange) {}

  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedRange> ranges;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

struct SkPointScan : ScanSource {
  SkPointScan() : ScanSource(ScanSourceKind::SkPoint) {}

  ObjectId shard_id;
  bool is_unique = false;
  std::vector<catalog::Column::Id> column_ids;  // SK columns in order
  std::vector<ResolvedPoint> points;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

struct SkRangeScan : ScanSource {
  SkRangeScan() : ScanSource(ScanSourceKind::SkRange) {}

  ObjectId shard_id;
  bool is_unique = false;
  std::vector<catalog::Column::Id> column_ids;  // SK columns in order
  std::vector<ResolvedRange> ranges;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

enum class ScanEntryKind : uint8_t {
  BaseTable,
  InvertedIndex,
  SecondaryIndex,
};

constexpr catalog::Column::Id kInvalidColumnId =
  std::numeric_limits<catalog::Column::Id>::max();

struct SereneDBScanBindData : public duckdb::FunctionData {
  enum class Kind : uint8_t { Table, View };

  std::vector<catalog::Column::Id> column_ids;
  std::vector<duckdb::LogicalType> column_types;
  bool has_rowid = false;
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

  template<class T>
  T& As() & {
    return basics::downCast<T>(*this);
  }
  template<class T>
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

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
  bool Equals(const duckdb::FunctionData& other) const override;

  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const override;
  ObjectId RelationId() const override;
  std::string_view RelationName() const override;
  catalog::Column::Id ColumnIdByName(std::string_view name) const override;
  std::string_view ColumnNameById(catalog::Column::Id col_id) const override;
  void IterateColumns(const ColumnVisitor& cb) const override;
};

struct ViewScanBindData final : public SereneDBScanBindData {
  std::shared_ptr<const catalog::PgSqlView> view;

  ViewScanBindData() : SereneDBScanBindData(Kind::View) {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
  bool Equals(const duckdb::FunctionData& other) const override;

  duckdb::unique_ptr<duckdb::NodeStatistics> Cardinality(
    duckdb::ClientContext& context) const override;
  ObjectId RelationId() const override;
  std::string_view RelationName() const override;
  catalog::Column::Id ColumnIdByName(std::string_view name) const override;
  std::string_view ColumnNameById(catalog::Column::Id col_id) const override;
  void IterateColumns(const ColumnVisitor& cb) const override;
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
