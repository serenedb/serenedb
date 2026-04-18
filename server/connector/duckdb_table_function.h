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
#include <optional>
#include <variant>

#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "connector/rocksdb_filter.hpp"

namespace irs {

class IndexReader;
}

#include "search/inverted_index_shard.h"

namespace sdb::connector {

// Scan source variants -- determines how the scan function reads data.
struct FullTableScan {};

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
// ANNScan / RangeSearchScan variants below.
struct SearchScan {
  // Boolean filter side (plain text/search predicates).
  irs::Filter::Query::ptr query;
  // Stored filter for re-preparation with a scorer (see
  // SereneDBScanInitGlobal). Kept alive here because prepare() is const and
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
  enum class ScorerKind : uint8_t { None, Bm25, Tfidf };
  struct ScorerParams {
    ScorerKind kind = ScorerKind::None;
    // bm25(k1, b) -- iresearch defaults per Bm25.
    double bm25_k1 = 1.2;
    double bm25_b = 0.75;
    // tfidf(with_norms) -- default false = no length normalisation.
    bool tfidf_with_norms = false;
  };
  ScorerParams scorer;
  std::optional<size_t> score_top_k;

  // Optional: positions/offsets output. Each entry pairs a target
  // column id (whose offsets will be emitted) with the projection's
  // output column index (so the scan knows where to place the LIST).
  // Empty when no sdb_offsets() projection was claimed.
  struct OffsetsRequest {
    catalog::Column::Id column_id;
    duckdb::idx_t projection_index;  // matched projection.expressions slot
  };
  std::vector<OffsetsRequest> offsets;

  // Convenience for to_string / runtime checks.
  bool emit_offsets() const { return !offsets.empty(); }
};

struct SecondaryIndexScan {
  ObjectId shard_id;
  bool is_unique = false;
};

// ANN (top-k nearest-neighbour) scan using an HNSW index.
// Populated by iresearch_plan (previously ann_search_plan) when it
// detects the pattern:
//   ORDER BY distance_func(col, const_vector) ASC LIMIT k
struct ANNScan {
  ObjectId index_id;
  // Field name: big-endian catalog::Column::Id bytes, no MangleString
  std::string field_name;
  std::vector<float> query_vector;
  size_t top_k = 0;
};

// Range search scan using an HNSW index.
// Populated by iresearch_plan (previously range_search_plan) when it
// detects the pattern:
//   WHERE distance_func(col, const_vector) < radius
struct RangeSearchScan {
  ObjectId index_id;
  std::string field_name;
  std::vector<float> query_vector;
  float radius = 0.0f;
};

// Primary-key point lookup. Populated by the rocksdb_plan optimizer when it
// detects the pattern:  WHERE pk = const  (or  pk IN (...)).
// `points` are the fully-resolved PK values per point (values per PK
// column in PK order). Byte-encoding into a MultiGet key happens at
// runtime via the scan executor.
struct PkPointScan {
  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedPoint> points;
};

// Primary-key range scan. Populated by the rocksdb_plan optimizer when it
// detects PK range predicates (<, <=, >, >=, BETWEEN), or a conjunctive
// equality prefix on a composite PK combined with a trailing range.
// `ranges` hold the prefix-value + bound on the range column per
// disjoint region.
struct PkRangeScan {
  std::vector<catalog::Column::Id> column_ids;  // PK columns in order
  std::vector<ResolvedRange> ranges;
};

// Secondary-key point lookup: SK probe -> PK list -> MultiGet. Populated
// by the rocksdb_plan optimizer when it detects equality / IN predicates
// covering the SK column set.
struct SkPointScan {
  ObjectId shard_id;
  bool is_unique = false;
  std::vector<catalog::Column::Id> column_ids;  // SK columns in order
  std::vector<ResolvedPoint> points;
};

// Secondary-key range scan: SK range -> PK stream -> MultiGet. Populated
// by the rocksdb_plan optimizer when it detects range predicates on the
// leading SK columns.
struct SkRangeScan {
  ObjectId shard_id;
  bool is_unique = false;
  std::vector<catalog::Column::Id> column_ids;  // SK columns in order
  std::vector<ResolvedRange> ranges;
};

using ScanSource = std::variant<FullTableScan, SearchScan, SecondaryIndexScan,
                                ANNScan, RangeSearchScan, PkPointScan,
                                PkRangeScan, SkPointScan, SkRangeScan>;

struct SereneDBScanBindData : public duckdb::FunctionData {
  std::shared_ptr<catalog::Table> table;
  std::vector<catalog::Column::Id> column_ids;
  std::vector<duckdb::LogicalType> column_types;
  bool has_rowid = false;
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry;

  ScanSource scan_source;

  bool IsSearchScan() const {
    return std::holds_alternative<SearchScan>(scan_source);
  }
  bool IsSecondaryIndexScan() const {
    return std::holds_alternative<SecondaryIndexScan>(scan_source);
  }
  bool IsANNScan() const {
    return std::holds_alternative<ANNScan>(scan_source);
  }
  bool IsRangeSearchScan() const {
    return std::holds_alternative<RangeSearchScan>(scan_source);
  }
  bool IsPkPointScan() const {
    return std::holds_alternative<PkPointScan>(scan_source);
  }
  bool IsPkRangeScan() const {
    return std::holds_alternative<PkRangeScan>(scan_source);
  }
  bool IsSkPointScan() const {
    return std::holds_alternative<SkPointScan>(scan_source);
  }
  bool IsSkRangeScan() const {
    return std::holds_alternative<SkRangeScan>(scan_source);
  }

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
  bool Equals(const duckdb::FunctionData& other) const override;
};

// Default scan over a SereneDB RocksDB table: full prefix iteration.
// Optimizer rules may swap LogicalGet.function to a more specialised
// function below when query patterns warrant it.
duckdb::TableFunction CreateSereneDBScanFunction();

// PK point lookup: RocksDB MultiGet over the PK byte sequences in
// PkPointScan bind data. Swapped in by the rocksdb_plan rule when it
// detects PK equality / IN predicates above the LogicalGet.
duckdb::TableFunction CreatePkPointScanFunction();

// PK range scan: bounded prefix iterator(s) over RocksDB. Swapped in by
// the rocksdb_plan rule when it detects PK range predicates (<, <=, >, >=,
// BETWEEN) or a composite-PK equality prefix + trailing range.
duckdb::TableFunction CreatePkRangeScanFunction();

// Default for SK-index entries (FROM sk_index_name): full SK iteration ->
// PK stream -> MultiGet. Stub for now: same body as the full table scan,
// just a distinct name so EXPLAIN shows when an index entry is bound.
// The rocksdb_plan rule (Phase 4) swaps to SkPoint / SkRange when SK
// predicates fire.
duckdb::TableFunction CreateFullSkScanFunction();

// SK point lookup: SK probe -> PK list -> MultiGet. Swapped in by the
// rocksdb_plan rule when SK equality / IN predicates match the leading
// columns of a secondary index (either auto-chosen over PK for a regular
// table scan, or the designated index when FROM idx_name).
duckdb::TableFunction CreateSkPointScanFunction();

// SK range scan: SK range -> PK stream -> MultiGet. Swapped in by the
// rocksdb_plan rule when SK range predicates match.
duckdb::TableFunction CreateSkRangeScanFunction();

// Default for inverted-index entries (FROM iresearch_index_name): full
// iresearch doc iteration -> PK stream -> MultiGet. Stub for now: same
// body as the full table scan, just a distinct name. The iresearch_plan
// rule (Phase 5) swaps to specialised iresearch search/ANN/range scans
// when the corresponding predicates fire.
duckdb::TableFunction CreateFullIresearchScanFunction();

// Iresearch phrase / term_eq search. Swapped in by iresearch_plan when
// the filter contains one or more sdb_phrase/sdb_term_eq predicates over
// the inverted index. bind_data.scan_source becomes SearchScan with the
// prepared iresearch query.
duckdb::TableFunction CreateIresearchSearchScanFunction();

// HNSW approximate-nearest-neighbour top-k. Swapped in by iresearch_plan
// on the pattern ORDER BY distance_fn(col, const_vec) ASC LIMIT k.
// bind_data.scan_source becomes ANNScan.
duckdb::TableFunction CreateIresearchAnnScanFunction();

// HNSW bounded-radius range search. Swapped in by iresearch_plan on
// WHERE distance_fn(col, const_vec) < radius. bind_data.scan_source
// becomes RangeSearchScan.
duckdb::TableFunction CreateIresearchRangeScanFunction();

}  // namespace sdb::connector
