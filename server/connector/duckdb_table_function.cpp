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

#include "connector/duckdb_table_function.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/mangling.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_pk_full_scan.hpp"
#include "connector/duckdb_pk_point_lookup.hpp"
#include "connector/duckdb_pk_range_scan.hpp"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_search_ann_scan.hpp"
#include "connector/duckdb_search_count_scan.hpp"
#include "connector/duckdb_search_full_scan.hpp"
#include "connector/duckdb_search_range_scan.hpp"
#include "connector/duckdb_sk_full_scan.hpp"
#include "connector/duckdb_sk_point_lookup.hpp"
#include "connector/duckdb_sk_range_scan.hpp"
#include "connector/rocksdb_filter.hpp"
#include "connector/row_materializer.h"
#include "functions/search.h"
#include "pg/connection_context.h"

namespace sdb::connector {

// --- SereneDBScanBindData ---

duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBindData::Copy() const {
  auto copy = duckdb::make_uniq<SereneDBScanBindData>();
  copy->table = table;
  copy->column_ids = column_ids;
  copy->column_types = column_types;
  copy->has_rowid = has_rowid;
  copy->table_entry = table_entry;
  copy->scan_source = scan_source->Clone();
  return copy;
}

static duckdb::BindInfo SereneDBGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& data =
    const_cast<SereneDBScanBindData&>(bind_data->Cast<SereneDBScanBindData>());
  if (data.table_entry) {
    return duckdb::BindInfo(*data.table_entry);
  }
  return duckdb::BindInfo(duckdb::ScanType::TABLE);
}

bool SereneDBScanBindData::Equals(const duckdb::FunctionData& other) const {
  auto& o = other.Cast<SereneDBScanBindData>();
  return table == o.table && column_ids == o.column_ids;
}

// --- Bind stub ---

static duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  throw duckdb::InternalException(
    "SereneDBScanBind: should be provided via GetScanFunction");
}

// --- pushdown_complex_filter: kept as no-op ---

static void SereneDBPushdownComplexFilter(
  duckdb::ClientContext& /*context*/, duckdb::LogicalGet& /*get*/,
  duckdb::FunctionData* /*bind_data_ptr*/,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& /*filters*/) {
  // No-op: all iresearch / rocksdb predicate claims now happen in the
  // sdb::optimizer::Register{Iresearch,RocksDB}PlanOptimizer extensions
  // (which run after the built-in FilterPushdown pass). Built-in
  // FilterPushdown still calls this hook, so we keep it wired up but
  // don't claim anything here -- otherwise the rules would see a
  // pre-claimed scan and skip it.
}

// --- cardinality / rows_scanned / virtual columns ---

static duckdb::unique_ptr<duckdb::NodeStatistics> SereneDBScanCardinality(
  duckdb::ClientContext& context, const duckdb::FunctionData* bind_data) {
  if (!bind_data) {
    return nullptr;
  }
  auto& bind = bind_data->Cast<SereneDBScanBindData>();
  if (!bind.table) {
    return nullptr;
  }
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto shard = snapshot->GetTableShard(bind.table->GetId());
  if (!shard) {
    return nullptr;
  }
  auto stats = shard->GetTableStats();
  return duckdb::make_uniq<duckdb::NodeStatistics>(
    static_cast<duckdb::idx_t>(stats.num_rows));
}

static duckdb::virtual_column_map_t SereneDBScanGetVirtualColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData> bind_p) {
  duckdb::virtual_column_map_t result;
  if (!bind_p) {
    return result;
  }
  auto& bind = bind_p->Cast<SereneDBScanBindData>();
  if (bind.table_entry) {
    result = bind.table_entry->GetVirtualColumns();
  }
  return result;
}

static duckdb::vector<duckdb::column_t> SereneDBScanGetRowIdColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData> bind_p) {
  duckdb::vector<duckdb::column_t> result;
  if (!bind_p) {
    return result;
  }
  auto& bind = bind_p->Cast<SereneDBScanBindData>();
  if (bind.table_entry) {
    result = bind.table_entry->GetRowIdColumns();
  }
  return result;
}

// --- to_string helpers ---

std::string_view FullTableScan::KindName() const {
  return "rocksdb_table_fullscan";
}
std::unique_ptr<ScanSource> FullTableScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::string_view SecondaryIndexScan::KindName() const {
  return "rocksdb_secondary_key_fullscan";
}
std::unique_ptr<ScanSource> SecondaryIndexScan::Clone() const {
  auto copy = std::make_unique<SecondaryIndexScan>();
  copy->shard_id = shard_id;
  copy->is_unique = is_unique;
  return copy;
}

std::string_view SearchScan::KindName() const {
  const bool off = emit_offsets();
  if (scorer.kind != SearchScan::ScorerKind::None) {
    if (score_top_k) {
      return off ? "iresearch_score_topk_scan_with_offsets"
                 : "iresearch_score_topk_scan";
    }
    return off ? "iresearch_score_scan_with_offsets" : "iresearch_score_scan";
  }
  return off ? "iresearch_lookup_with_offsets" : "iresearch_lookup";
}
std::unique_ptr<ScanSource> SearchScan::Clone() const {
  // SearchScan owns a prepared iresearch query + filter tree that we can't
  // duplicate; preserve the pre-refactor behaviour of falling back to the
  // default FullTableScan on copy.
  return std::make_unique<FullTableScan>();
}

std::string_view CountScan::KindName() const { return "iresearch_count"; }
std::unique_ptr<ScanSource> CountScan::Clone() const {
  // Same fallback as SearchScan: the prepared iresearch query + filter
  // tree aren't duplicable; Copy() paths land on FullTableScan.
  return std::make_unique<FullTableScan>();
}

std::string_view ANNScan::KindName() const { return "iresearch_ann_topk_scan"; }
std::unique_ptr<ScanSource> ANNScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::string_view RangeSearchScan::KindName() const {
  return "iresearch_ann_range_scan";
}
std::unique_ptr<ScanSource> RangeSearchScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::string_view PkPointScan::KindName() const {
  return "rocksdb_primary_key_points_lookup";
}
std::unique_ptr<ScanSource> PkPointScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::string_view PkRangeScan::KindName() const {
  return "rocksdb_primary_key_ranges_scan";
}
std::unique_ptr<ScanSource> PkRangeScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::string_view SkPointScan::KindName() const {
  return "rocksdb_secondary_key_points_lookup";
}
std::unique_ptr<ScanSource> SkPointScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::string_view SkRangeScan::KindName() const {
  return "rocksdb_secondary_key_ranges_scan";
}
std::unique_ptr<ScanSource> SkRangeScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

static std::string ColumnNameFor(const catalog::Table& table,
                                 catalog::Column::Id col_id) {
  for (const auto& c : table.Columns()) {
    if (c.id == col_id) {
      return c.name;
    }
  }
  return absl::StrCat("col", col_id);
}

static std::string FormatResolvedPoint(
  const ResolvedPoint& point, const catalog::Table& table,
  std::span<const catalog::Column::Id> column_ids) {
  std::string out = "(";
  for (size_t i = 0; i < point.size(); ++i) {
    if (i) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, ColumnNameFor(table, column_ids[i]), "=",
                    point[i].ToString());
  }
  absl::StrAppend(&out, ")");
  return out;
}

static std::string FormatResolvedRange(
  const ResolvedRange& range, const catalog::Table& table,
  std::span<const catalog::Column::Id> column_ids) {
  std::string out = "{";
  for (size_t i = 0; i < range.prefix.size(); ++i) {
    if (i) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, ColumnNameFor(table, column_ids[i]), "=",
                    range.prefix[i].ToString());
  }
  const auto range_col_idx = range.prefix.size();
  if (range_col_idx < column_ids.size()) {
    if (!range.prefix.empty()) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, ColumnNameFor(table, column_ids[range_col_idx]), "=",
                    range.range_column.toString());
  }
  absl::StrAppend(&out, "}");
  return out;
}

template<typename PointsOrRanges, typename FormatOne>
static std::string FormatClaimList(const PointsOrRanges& items,
                                   FormatOne&& format_one) {
  constexpr size_t kMaxShown = 8;
  std::string out;
  for (size_t i = 0; i < items.size() && i < kMaxShown; ++i) {
    if (i) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, format_one(items[i]));
  }
  if (items.size() > kMaxShown) {
    absl::StrAppend(&out, ", ... (+", items.size() - kMaxShown, " more)");
  }
  return out;
}

void PkPointScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!points.empty() && bind.table) {
    auto& table = *bind.table;
    auto cols = std::span<const catalog::Column::Id>(column_ids);
    out.insert("Filter", FormatClaimList(points, [&](const ResolvedPoint& pt) {
                 return FormatResolvedPoint(pt, table, cols);
               }));
  }
}

void PkRangeScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!ranges.empty() && bind.table) {
    auto& table = *bind.table;
    auto cols = std::span<const catalog::Column::Id>(column_ids);
    out.insert("Filter", FormatClaimList(ranges, [&](const ResolvedRange& rr) {
                 return FormatResolvedRange(rr, table, cols);
               }));
  }
}

void SkPointScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!points.empty() && bind.table) {
    auto& table = *bind.table;
    auto cols = std::span<const catalog::Column::Id>(column_ids);
    out.insert("Filter", FormatClaimList(points, [&](const ResolvedPoint& pt) {
                 return FormatResolvedPoint(pt, table, cols);
               }));
  }
  if (is_unique) {
    out.insert("Unique", "true");
  }
}

void SkRangeScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!ranges.empty() && bind.table) {
    auto& table = *bind.table;
    auto cols = std::span<const catalog::Column::Id>(column_ids);
    out.insert("Filter", FormatClaimList(ranges, [&](const ResolvedRange& rr) {
                 return FormatResolvedRange(rr, table, cols);
               }));
  }
  if (is_unique) {
    out.insert("Unique", "true");
  }
}

void ANNScan::AppendSummary(
  const SereneDBScanBindData& /*bind*/,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  out.insert("TopK", std::to_string(top_k));
  out.insert("Dims", std::to_string(query_vector.size()));
}

void RangeSearchScan::AppendSummary(
  const SereneDBScanBindData& /*bind*/,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  out.insert("Radius", std::to_string(radius));
  out.insert("Dims", std::to_string(query_vector.size()));
}

void CountScan::AppendSummary(
  const SereneDBScanBindData& /*bind*/,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  out.insert("Filter", filter_summary.empty() ? "all-rows" : filter_summary);
  out.insert("Output", "row-count only");
}

void SearchScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!filter_summary.empty()) {
    out.insert("Filter", filter_summary);
  }
  switch (scorer.kind) {
    case SearchScan::ScorerKind::Bm25:
      out.insert("Score", absl::StrCat("bm25(k1=", scorer.bm25_k1,
                                       ", b=", scorer.bm25_b, ")"));
      break;
    case SearchScan::ScorerKind::Tfidf:
      out.insert("Score",
                 absl::StrCat("tfidf(with_norms=",
                              scorer.tfidf_with_norms ? "true" : "false", ")"));
      break;
    case SearchScan::ScorerKind::None:
      break;
  }
  if (score_top_k) {
    out.insert("TopK", std::to_string(*score_top_k));
  }
  if (emit_offsets()) {
    std::string cols;
    for (size_t i = 0; i < offsets.size(); ++i) {
      if (i) {
        absl::StrAppend(&cols, ", ");
      }
      absl::StrAppend(&cols, ColumnNameFor(*bind.table, offsets[i].column_id));
    }
    out.insert("Offsets", std::move(cols));
  }
}

static duckdb::InsertionOrderPreservingMap<std::string> SereneDBScanToString(
  duckdb::TableFunctionToStringInput& input) {
  duckdb::InsertionOrderPreservingMap<std::string> result;
  if (!input.bind_data) {
    return result;
  }
  auto& bind = input.bind_data->Cast<SereneDBScanBindData>();
  if (bind.table) {
    result.insert("Table", std::string{bind.table->GetName()});
  }
  result.insert("Strategy", std::string{bind.scan_source->KindName()});
  // Surface which RowMaterializer the search-scan path will use to
  // resolve PKs from the iresearch index. Only emit for strategies
  // that actually run the iresearch pk -> row pipeline.
  if (bind.scan_source->IsSearchLike()) {
    result.insert("Materializer", std::string{RowMaterializerName(bind)});
  }
  bind.scan_source->AppendSummary(bind, result);
  return result;
}

// --- Factory helpers ---

// Populate the common callbacks shared by every scan function variant.
static void SetCommonCallbacks(duckdb::TableFunction& func) {
  func.bind = SereneDBScanBind;
  func.get_bind_info = SereneDBGetBindInfo;
  func.init_local = CommonScanInitLocal;
  func.projection_pushdown = true;
  func.filter_pushdown = false;
  func.pushdown_complex_filter = SereneDBPushdownComplexFilter;
  func.to_string = SereneDBScanToString;
  func.cardinality = SereneDBScanCardinality;
  func.rows_scanned = CommonScanRowsScanned;
  func.get_virtual_columns = SereneDBScanGetVirtualColumns;
  func.get_row_id_columns = SereneDBScanGetRowIdColumns;
}

// --- Factory ---

duckdb::TableFunction CreateSereneDBScanFunction() {
  duckdb::TableFunction func("rocksdb_table_fullscan", {}, PKFullScanFunction,
                             SereneDBScanBind);
  func.init_global = PKFullScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreatePkPointScanFunction() {
  duckdb::TableFunction func("rocksdb_primary_key_points_lookup", {},
                             PKPointLookupFunction, SereneDBScanBind);
  func.init_global = PKPointLookupInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreatePkRangeScanFunction() {
  duckdb::TableFunction func("rocksdb_primary_key_ranges_scan", {},
                             PKRangeScanFunction, SereneDBScanBind);
  func.init_global = PKRangeScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateFullSkScanFunction() {
  duckdb::TableFunction func("rocksdb_secondary_key_fullscan", {},
                             SKFullScanFunction, SereneDBScanBind);
  func.init_global = SKFullScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateSkPointScanFunction() {
  duckdb::TableFunction func("rocksdb_secondary_key_points_lookup", {},
                             SKPointLookupFunction, SereneDBScanBind);
  func.init_global = SKPointLookupInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateSkRangeScanFunction() {
  duckdb::TableFunction func("rocksdb_secondary_key_ranges_scan", {},
                             SKRangeScanFunction, SereneDBScanBind);
  func.init_global = SKRangeScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateFullIresearchScanFunction() {
  duckdb::TableFunction func("iresearch_fullscan", {}, PKFullScanFunction,
                             SereneDBScanBind);
  func.init_global = PKFullScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIresearchSearchScanFunction() {
  duckdb::TableFunction func("iresearch_lookup", {}, SearchFullScanFunction,
                             SereneDBScanBind);
  func.init_global = SearchFullScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIresearchCountScanFunction() {
  duckdb::TableFunction func("iresearch_count", {}, SearchCountScanFunction,
                             SereneDBScanBind);
  func.init_global = SearchCountScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIresearchAnnScanFunction() {
  duckdb::TableFunction func("iresearch_ann_topk_scan", {},
                             SearchAnnScanFunction, SereneDBScanBind);
  func.init_global = SearchAnnScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIresearchRangeScanFunction() {
  duckdb::TableFunction func("iresearch_ann_range_scan", {},
                             SearchRangeScanFunction, SereneDBScanBind);
  func.init_global = SearchRangeScanInitGlobal;
  SetCommonCallbacks(func);
  return func;
}

}  // namespace sdb::connector
