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
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <memory>
#include <optional>
#include <string_view>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "connector/rocksdb_filter.hpp"

namespace irs {

class IndexReader;
}

#include "search/inverted_index_shard.h"

namespace sdb::connector {

struct SereneDBScanBindData;

// Identifies the concrete scan-source subclass without RTTI. Executor
// code uses Kind() to pick paths and ScanSource::Cast<T>() to downcast.
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

// Base class for the different scan specialisations carried on a
// SereneDBScanBindData. The optimizer swaps the bind data's ScanSource
// instance (and simultaneously the LogicalGet's TableFunction) when a
// predicate pattern matches; the executor then downcasts to the concrete
// type it was paired with.
struct ScanSource {
  ScanSourceKind Kind() const { return _kind; }

  // Extend the EXPLAIN output with per-kind fields (Filter, TopK, ...).
  virtual void AppendSummary(
    const SereneDBScanBindData& /*bind*/,
    duckdb::InsertionOrderPreservingMap<std::string>& /*out*/) const {}

  // Deep copy for duckdb::FunctionData::Copy(). Subclasses that hold
  // non-copyable fields (e.g. SearchScan's prepared iresearch query)
  // return a default FullTableScan instead -- the variant-based predecessor
  // silently dropped such state on copy, and we preserve that behaviour.
  virtual std::unique_ptr<ScanSource> Clone() const = 0;

  // Covers SearchScan / CountScan / ANNScan / RangeSearchScan -- the four
  // that need stricter transaction isolation in duckdb_scan_base.
  bool IsSearchLike() const noexcept {
    return _kind == ScanSourceKind::Search || _kind == ScanSourceKind::Count ||
           _kind == ScanSourceKind::Ann || _kind == ScanSourceKind::RangeSearch;
  }

  // Covers SecondaryIndexScan / SkPointScan / SkRangeScan.
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

// Default: full prefix iteration over the table's RocksDB keyspace.
// Also the placeholder state for inverted-index FROM-entries until an
// iresearch_plan rule swaps in a specialised SearchScan / ANNScan /
// RangeSearchScan.
struct FullTableScan : ScanSource {
  FullTableScan() : ScanSource(ScanSourceKind::FullTable) {}
  std::unique_ptr<ScanSource> Clone() const override;
};

// Iresearch boolean-filter scan with optional score / offsets add-ons.
// The iresearch_plan rule (Phase 5d) picks which add-ons apply for a
// given SQL pattern and fills out the struct. Covers these cases from
// the rule design:
//
//   case 1: filter + optional produce score + optional produce offsets
//   case 2: filter + topk-scores + optional produce offsets
//
// ANN (vector distance) cases -- topk and range -- are NOT layered on
// top of a boolean filter in this struct; they live in their own
// ANNScan / RangeSearchScan subclasses below.
struct SearchScan : ScanSource {
  SearchScan() : ScanSource(ScanSourceKind::Search) {}

  // Boolean filter side (plain text/search predicates).
  irs::Filter::Query::ptr query;
  // Stored filter for re-preparation with a scorer (see
  // SearchFullScanInitGlobal). Kept alive here because prepare() is const and
  // stats depend on the scorer.
  std::shared_ptr<irs::Filter> stored_filter;
  search::InvertedIndexSnapshotPtr snapshot;
  const irs::IndexReader* reader = nullptr;
  // Human-readable repr of the boolean filter tree, captured before
  // `query` was prepared. Rendered by irs::ToStringDemangled (from
  // search_filter_printer.hpp) using the table's column names. Empty
  // when the filter is trivial (all-rows match).
  std::string filter_summary;

  // Optional: fulltext scoring. When set, the scan emits one score
  // column per row. We treat the scorer as opaque -- iresearch's
  // `Scorer` is an abstract base (BM25 / TF-IDF / etc. are concrete
  // subclasses with their own parameters, e.g. BM25's k1 and b). The
  // plan just carries the prepared scorer reference; neither the rule
  // nor to_string branches on which concrete scorer it is.
  //
  // `score_top_k` is non-empty only when the plan has a pullup LIMIT
  // we can prune against (case 2: filter + topk-scores). When scoring
  // is requested without a pruneable LIMIT (case 1: filter + score),
  // we still carry the scorer but score_top_k stays empty.
  // Typed scorer configuration resolved at compile time. The rule
  // extracts the scorer kind + parameters from the projection's
  // bm25(...) / tfidf(...) call and stores them here so the runtime
  // executor can build an irs::Scorer without re-parsing expressions.
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
  // DFI independence measure. Must stay in sync with irs::DFIMeasure.
  enum class DfiMeasure : uint8_t {
    Standardized,
    Saturated,
    ChiSquared,
  };
  // Scorer parameters are tagged by `kind`: only the matching union arm is
  // live. All variants are trivial types so the union is trivially
  // constructible; one arm (RawTf -- empty) carries the default-member
  // initializer so ScorerParams is itself default-constructible.
  // Writers set `kind` and assign into the matching arm; readers switch on
  // `kind` before accessing any arm.
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

  // Optional: positions/offsets output. Each entry records the catalog
  // column whose offsets will be emitted, and a per-doc limit on the
  // number of offset pairs. The runtime produces one
  // LIST(BIGINT) output column per entry, in this vector's order.
  // Empty when no OFFSETS() projection was claimed.
  struct OffsetsRequest {
    catalog::Column::Id column_id;
    size_t limit = 0;
  };
  std::vector<OffsetsRequest> offsets;

  // Convenience for to_string / runtime checks.
  bool emit_offsets() const { return !offsets.empty(); }

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

// ANN (top-k nearest-neighbour) scan using an HNSW index.
// Populated by iresearch_plan (previously ann_search_plan) when it
// detects the pattern:
//   ORDER BY distance_func(col, const_vector) ASC LIMIT k
struct ANNScan : ScanSource {
  ANNScan() : ScanSource(ScanSourceKind::Ann) {}

  ObjectId index_id;
  // Field name: big-endian catalog::Column::Id bytes, no MangleString
  std::string field_name;
  std::vector<float> query_vector;
  size_t top_k = 0;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

// Range search scan using an HNSW index.
// Populated by iresearch_plan (previously range_search_plan) when it
// detects the pattern:
//   WHERE distance_func(col, const_vector) < radius
struct RangeSearchScan : ScanSource {
  RangeSearchScan() : ScanSource(ScanSourceKind::RangeSearch) {}

  ObjectId index_id;
  std::string field_name;
  std::vector<float> query_vector;
  float radius = 0.0f;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

// Primary-key point lookup. Populated by the rocksdb_plan optimizer when it
// detects the pattern:  WHERE pk = const  (or  pk IN (...)).
// `points` are the fully-resolved PK values per point (values per PK
// column in PK order). Byte-encoding into a MultiGet key happens at
// runtime via the scan executor.
struct PkPointScan : ScanSource {
  PkPointScan() : ScanSource(ScanSourceKind::PkPoint) {}

  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedPoint> points;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

// Primary-key range scan. Populated by the rocksdb_plan optimizer when it
// detects PK range predicates (<, <=, >, >=, BETWEEN), or a conjunctive
// equality prefix on a composite PK combined with a trailing range.
// `ranges` hold the prefix-value + bound on the range column per
// disjoint region.
struct PkRangeScan : ScanSource {
  PkRangeScan() : ScanSource(ScanSourceKind::PkRange) {}

  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedRange> ranges;

  void AppendSummary(
    const SereneDBScanBindData& bind,
    duckdb::InsertionOrderPreservingMap<std::string>& out) const override;
  std::unique_ptr<ScanSource> Clone() const override;
};

// Secondary-key point lookup: SK probe -> PK list -> MultiGet. Populated
// by the rocksdb_plan optimizer when it detects equality / IN predicates
// covering the SK column set.
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

// Secondary-key range scan: SK range -> PK stream -> MultiGet. Populated
// by the rocksdb_plan optimizer when it detects range predicates on the
// leading SK columns.
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

struct SereneDBScanBindData : public duckdb::FunctionData {
  std::shared_ptr<catalog::Table> table;
  std::vector<catalog::Column::Id> column_ids;
  std::vector<duckdb::LogicalType> column_types;
  bool has_rowid = false;
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

  // Always non-null. Default-constructed bind data starts as FullTableScan;
  // optimizer rules swap in a different concrete subclass when a matching
  // pattern is found.
  std::unique_ptr<ScanSource> scan_source = std::make_unique<FullTableScan>();

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
  bool Equals(const duckdb::FunctionData& other) const override;
};

// Default scan over a SereneDB RocksDB table: full prefix iteration.
// Optimizer rules may swap LogicalGet.function to a more specialised
// function below when query patterns warrant it.
duckdb::TableFunction CreateTableFullscanFunction();

// PK point lookup: RocksDB MultiGet over the PK byte sequences in
// PkPointScan bind data. Swapped in by the rocksdb_plan rule when it
// detects PK equality / IN predicates above the LogicalGet.
duckdb::TableFunction CreatePKPointsLookupFunction();

// PK range scan: bounded prefix iterator(s) over RocksDB. Swapped in by
// the rocksdb_plan rule when it detects PK range predicates (<, <=, >, >=,
// BETWEEN) or a composite-PK equality prefix + trailing range.
duckdb::TableFunction CreatePKRangesScanFunction();

// Default for SK-index entries (FROM sk_index_name): full SK iteration ->
// PK stream -> MultiGet. Stub for now: same body as the full table scan,
// just a distinct name so EXPLAIN shows when an index entry is bound.
// The rocksdb_plan rule (Phase 4) swaps to SkPoint / SkRange when SK
// predicates fire.
duckdb::TableFunction CreateSKFullscanFunction();

// SK point lookup: SK probe -> PK list -> MultiGet. Swapped in by the
// rocksdb_plan rule when SK equality / IN predicates match the leading
// columns of a secondary index (either auto-chosen over PK for a regular
// table scan, or the designated index when FROM idx_name).
duckdb::TableFunction CreateSKPointsLookupFunction();

// SK range scan: SK range -> PK stream -> MultiGet. Swapped in by the
// rocksdb_plan rule when SK range predicates match.
duckdb::TableFunction CreateSKRangesScanFunction();

// Default for inverted-index entries (FROM iresearch_index_name): full
// iresearch doc iteration -> PK stream -> MultiGet. Stub for now: same
// body as the full table scan, just a distinct name. The iresearch_plan
// rule (Phase 5) swaps to specialised iresearch search/ANN/range scans
// when the corresponding predicates fire.
duckdb::TableFunction CreateIResearchFullscanFunction();

// Iresearch phrase / term_eq search. Swapped in by iresearch_plan when
// the filter contains one or more sdb_phrase/sdb_term_eq predicates over
// the inverted index. bind_data.scan_source becomes SearchScan with the
// prepared iresearch query.
duckdb::TableFunction CreateIResearchScanFunction();

// Iresearch row-count: emits zero-column rows of cardinality == match count.
// Swapped in by iresearch_plan on LogicalGet with no projected columns
// (COUNT(*) / COUNT(1) / EXISTS(SELECT 1 ...) / ...).
duckdb::TableFunction CreateIResearchCountFunction();

// HNSW approximate-nearest-neighbour top-k. Swapped in by iresearch_plan
// on the pattern ORDER BY distance_fn(col, const_vec) ASC LIMIT k.
// bind_data.scan_source becomes ANNScan.
duckdb::TableFunction CreateIResearchANNFullscanFunction();

// HNSW bounded-radius range search. Swapped in by iresearch_plan on
// WHERE distance_fn(col, const_vec) < radius. bind_data.scan_source
// becomes RangeSearchScan.
duckdb::TableFunction CreateIResearchANNRangeScanFunction();

}  // namespace sdb::connector
