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
#include "connector/duckdb_search_full_scan.hpp"
#include "connector/duckdb_search_range_scan.hpp"
#include "connector/duckdb_sk_full_scan.hpp"
#include "connector/duckdb_sk_point_lookup.hpp"
#include "connector/duckdb_sk_range_scan.hpp"
#include "connector/rocksdb_filter.hpp"
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
  // SearchScan has non-copyable fields -- only copy for secondary/full
  if (std::holds_alternative<SecondaryIndexScan>(scan_source)) {
    copy->scan_source = std::get<SecondaryIndexScan>(scan_source);
  }
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

static std::string_view ScanSourceKindName(const ScanSource& s) {
  if (const auto* ss = std::get_if<SearchScan>(&s)) {
    const bool off = ss->emit_offsets();
    if (ss->scorer.kind != SearchScan::ScorerKind::None) {
      if (ss->score_top_k) {
        return off ? "iresearch_score_topk_scan_with_offsets"
                   : "iresearch_score_topk_scan";
      }
      return off ? "iresearch_score_scan_with_offsets" : "iresearch_score_scan";
    }
    return off ? "iresearch_lookup_with_offsets" : "iresearch_lookup";
  }
  if (std::holds_alternative<SecondaryIndexScan>(s)) {
    return "rocksdb_secondary_key_fullscan";
  }
  if (std::holds_alternative<ANNScan>(s)) {
    return "iresearch_ann_topk_scan";
  }
  if (std::holds_alternative<RangeSearchScan>(s)) {
    return "iresearch_ann_range_scan";
  }
  if (std::holds_alternative<PkPointScan>(s)) {
    return "rocksdb_primary_key_points_lookup";
  }
  if (std::holds_alternative<PkRangeScan>(s)) {
    return "rocksdb_primary_key_ranges_scan";
  }
  if (std::holds_alternative<SkPointScan>(s)) {
    return "rocksdb_secondary_key_points_lookup";
  }
  if (std::holds_alternative<SkRangeScan>(s)) {
    return "rocksdb_secondary_key_ranges_scan";
  }
  return "rocksdb_table_fullscan";
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

static void AppendClaimSummary(
  duckdb::InsertionOrderPreservingMap<std::string>& result,
  const SereneDBScanBindData& bind) {
  const auto& s = bind.scan_source;
  if (const auto* p = std::get_if<PkPointScan>(&s)) {
    if (!p->points.empty() && bind.table) {
      auto& table = *bind.table;
      auto cols = std::span<const catalog::Column::Id>(p->column_ids);
      result.insert("Filter",
                    FormatClaimList(p->points, [&](const ResolvedPoint& pt) {
                      return FormatResolvedPoint(pt, table, cols);
                    }));
    }
    return;
  }
  if (const auto* r = std::get_if<PkRangeScan>(&s)) {
    if (!r->ranges.empty() && bind.table) {
      auto& table = *bind.table;
      auto cols = std::span<const catalog::Column::Id>(r->column_ids);
      result.insert("Filter",
                    FormatClaimList(r->ranges, [&](const ResolvedRange& rr) {
                      return FormatResolvedRange(rr, table, cols);
                    }));
    }
    return;
  }
  if (const auto* p = std::get_if<SkPointScan>(&s)) {
    if (!p->points.empty() && bind.table) {
      auto& table = *bind.table;
      auto cols = std::span<const catalog::Column::Id>(p->column_ids);
      result.insert("Filter",
                    FormatClaimList(p->points, [&](const ResolvedPoint& pt) {
                      return FormatResolvedPoint(pt, table, cols);
                    }));
    }
    if (p->is_unique) {
      result.insert("Unique", "true");
    }
    return;
  }
  if (const auto* r = std::get_if<SkRangeScan>(&s)) {
    if (!r->ranges.empty() && bind.table) {
      auto& table = *bind.table;
      auto cols = std::span<const catalog::Column::Id>(r->column_ids);
      result.insert("Filter",
                    FormatClaimList(r->ranges, [&](const ResolvedRange& rr) {
                      return FormatResolvedRange(rr, table, cols);
                    }));
    }
    if (r->is_unique) {
      result.insert("Unique", "true");
    }
    return;
  }
  if (const auto* a = std::get_if<ANNScan>(&s)) {
    result.insert("TopK", std::to_string(a->top_k));
    result.insert("Dims", std::to_string(a->query_vector.size()));
    return;
  }
  if (const auto* a = std::get_if<RangeSearchScan>(&s)) {
    result.insert("Radius", std::to_string(a->radius));
    result.insert("Dims", std::to_string(a->query_vector.size()));
    return;
  }
  if (const auto* ss = std::get_if<SearchScan>(&s)) {
    if (!ss->filter_summary.empty()) {
      result.insert("Filter", ss->filter_summary);
    }
    switch (ss->scorer.kind) {
      case SearchScan::ScorerKind::Bm25:
        result.insert("Score", absl::StrCat("bm25(k1=", ss->scorer.bm25_k1,
                                            ", b=", ss->scorer.bm25_b, ")"));
        break;
      case SearchScan::ScorerKind::Tfidf:
        result.insert(
          "Score",
          absl::StrCat("tfidf(with_norms=",
                       ss->scorer.tfidf_with_norms ? "true" : "false", ")"));
        break;
      case SearchScan::ScorerKind::None:
        break;
    }
    if (ss->score_top_k) {
      result.insert("TopK", std::to_string(*ss->score_top_k));
    }
    if (ss->emit_offsets()) {
      std::string cols;
      for (size_t i = 0; i < ss->offsets.size(); ++i) {
        if (i) {
          absl::StrAppend(&cols, ", ");
        }
        absl::StrAppend(&cols,
                        ColumnNameFor(*bind.table, ss->offsets[i].column_id));
      }
      result.insert("Offsets", std::move(cols));
    }
    return;
  }
  // SecondaryIndexScan / FullTableScan: no extra info.
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
  result.insert("Strategy", std::string{ScanSourceKindName(bind.scan_source)});
  AppendClaimSummary(result, bind);
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
  func.order_preservation_type = duckdb::OrderPreservationType::FIXED_ORDER;
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
  func.order_preservation_type = duckdb::OrderPreservationType::FIXED_ORDER;
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
  func.order_preservation_type = duckdb::OrderPreservationType::FIXED_ORDER;
  return func;
}

duckdb::TableFunction CreateIresearchSearchScanFunction() {
  duckdb::TableFunction func("iresearch_lookup", {}, SearchFullScanFunction,
                             SereneDBScanBind);
  func.init_global = SearchFullScanInitGlobal;
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
