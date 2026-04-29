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
#include "connector/duckdb_search_ann_scan.h"
#include "connector/duckdb_search_count_scan.hpp"
#include "connector/duckdb_search_full_scan.hpp"
#include "connector/duckdb_search_range_scan.h"
#include "connector/duckdb_sk_full_scan.hpp"
#include "connector/duckdb_sk_point_lookup.hpp"
#include "connector/duckdb_sk_range_scan.hpp"
#include "connector/rocksdb_filter.hpp"
#include "functions/search.h"
#include "pg/connection_context.h"

namespace sdb::connector {

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

std::unique_ptr<ScanSource> FullTableScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::unique_ptr<ScanSource> SecondaryIndexScan::Clone() const {
  return std::make_unique<SecondaryIndexScan>(*this);
}

std::unique_ptr<ScanSource> SearchScan::Clone() const {
  // SearchScan owns a prepared iresearch query + filter tree that we can't
  // duplicate; preserve the pre-refactor behaviour of falling back to the
  // default FullTableScan on copy.
  return std::make_unique<FullTableScan>();
}

std::unique_ptr<ScanSource> CountScan::Clone() const {
  // Same fallback as SearchScan: the prepared iresearch query + filter
  // tree aren't duplicable; Copy() paths land on FullTableScan.
  return std::make_unique<FullTableScan>();
}

std::unique_ptr<ScanSource> ANNScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::unique_ptr<ScanSource> RangeSearchScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::unique_ptr<ScanSource> PkPointScan::Clone() const {
  return std::make_unique<PkPointScan>(*this);
}

std::unique_ptr<ScanSource> PkRangeScan::Clone() const {
  return std::make_unique<PkRangeScan>(*this);
}

std::unique_ptr<ScanSource> SkPointScan::Clone() const {
  return std::make_unique<SkPointScan>(*this);
}

std::unique_ptr<ScanSource> SkRangeScan::Clone() const {
  return std::make_unique<SkRangeScan>(*this);
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
  std::string out;
  for (size_t i = 0; i < items.size(); ++i) {
    if (i) {
      absl::StrAppend(&out, "\n");
    }
    absl::StrAppend(&out, format_one(items[i]));
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
  if (!filter_expressions.empty()) {
    std::string summary;
    for (const auto& expr : filter_expressions) {
      if (!summary.empty()) {
        summary += " AND ";
      }
      summary += expr->ToString();
    }
    out.insert("Filter", summary);
  }
}

void RangeSearchScan::AppendSummary(
  const SereneDBScanBindData& /*bind*/,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  out.insert("Radius", std::to_string(radius));
  out.insert("Dims", std::to_string(query_vector.size()));
  if (!filter_expressions.empty()) {
    std::string summary;
    for (const auto& expr : filter_expressions) {
      if (!summary.empty()) {
        summary += " AND ";
      }
      summary += expr->ToString();
    }
    out.insert("Filter", summary);
  }
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
      out.insert("Score", absl::StrCat("bm25(k1=", scorer.bm25.k1,
                                       ", b=", scorer.bm25.b, ")"));
      break;
    case SearchScan::ScorerKind::Tfidf:
      out.insert("Score",
                 absl::StrCat("tfidf(with_norms=",
                              scorer.tfidf.with_norms ? "true" : "false", ")"));
      break;
    case SearchScan::ScorerKind::RawTf:
      out.insert("Score", "raw_tf()");
      break;
    case SearchScan::ScorerKind::LmJm:
      out.insert("Score",
                 absl::StrCat("lm_jm(lambda=", scorer.lm_jm.lambda, ")"));
      break;
    case SearchScan::ScorerKind::LmDirichlet:
      out.insert("Score",
                 absl::StrCat("lm_dirichlet(mu=", scorer.lm_dirichlet.mu, ")"));
      break;
    case SearchScan::ScorerKind::IndriDirichlet:
      out.insert(
        "Score",
        absl::StrCat("indri_dirichlet(mu=", scorer.indri_dirichlet.mu, ")"));
      break;
    case SearchScan::ScorerKind::Dfi: {
      const char* m = "standardized";
      switch (scorer.dfi.measure) {
        case SearchScan::DfiMeasure::Standardized:
          m = "standardized";
          break;
        case SearchScan::DfiMeasure::Saturated:
          m = "saturated";
          break;
        case SearchScan::DfiMeasure::ChiSquared:
          m = "chi_squared";
          break;
      }
      out.insert("Score", absl::StrCat("dfi(measure=", m, ")"));
      break;
    }
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
  // Surface which lookup backend the search-scan path will use to resolve
  // PKs from the iresearch index. Only emit for strategies that actually
  // run the iresearch pk -> row pipeline.
  if (bind.scan_source->IsSearchLike()) {
    const auto& table = *bind.table;
    auto name = [&]() -> std::string {
      switch (table.GetTableType()) {
        using enum TableType;
        case File: {
          const auto& fi = table.GetFileInfo();
          SDB_ASSERT(fi.storage_options);
          std::string_view path = fi.storage_options->Path();
          auto dot = path.rfind('.');
          if (dot == std::string_view::npos) {
            return "file";
          }
          return std::string{path.substr(dot + 1)};
        }
        case RocksDB:
          return "rocksdb";
        case Unknown:
          SDB_UNREACHABLE();
      }
    }();

    result.insert("Lookup", std::move(name));
  }
  bind.scan_source->AppendSummary(bind, result);
  return result;
}

// Populate the common callbacks shared by every scan function variant.
static void SetCommonCallbacks(duckdb::TableFunction& func) {
  // TODO(mbkkt) Maybe we can use bind_replace/bind_operator to make indexes?
  func.init_local = CommonScanInitLocal;  // TODO: Use separate callbacks
  // TODO: Provide statistics
  // TODO: Better cardinality estimates
  func.cardinality = SereneDBScanCardinality;
  func.rows_scanned = CommonScanRowsScanned;
  // TODO: Use pushdown_complex_filter instead of RBO approach for
  // indexes/primary keys
  // TODO: Use pushdown_expression this instead of RBO approach for
  // scoring/offsets
  func.to_string = SereneDBScanToString;
  // TODO: Implement dynamic_to_string
  // TODO: Implement table_scan_progress
  // TODO: Use get_partition_data for partition pruning of partitioned
  // tables/indexes
  func.get_bind_info = SereneDBGetBindInfo;
  // TODO: Why type_pushdown is needed? Is it about cast for text-formats?
  // TODO: Is get_multi_file_reader helpful for us? Will it allow us
  // faster/simpler implementation of multi-threaded scanning?
  // TODO: Implement supports_pushdown_extract for struct extract pushdown
  // (e.g., JSON fields)
  // TODO: Implement get_partition_info and get_partition_stats for partitioned
  // tables/indexes
  func.get_virtual_columns = SereneDBScanGetVirtualColumns;
  func.get_row_id_columns = SereneDBScanGetRowIdColumns;
  // TODO: Implement set_scan_order for order by primary key columns in PK/RBO
  // scans, and for order by indexed columns in SK scans
  func.verify_serialization = false;
  func.projection_pushdown = true;
  // TODO: Use filter_pushdown, filter_prune instead of RBO approach for
  // indexes/primary keys
  // TODO: Use sampling_pushdown for sampling from rocksdb/etc
  // TODO: Use late_materialization instead of our materialization approach for
  // indexes/primary keys
  // TODO: Better order preservation types for different scan strategies, e.g.,
  // PK scans preserve insertion order, SK scans don't guarantee any order, but
  // could be made to preserve index order if we implement set_scan_order
  func.order_preservation_type = duckdb::OrderPreservationType::NO_ORDER;
  // TODO: We can init_global on schedule for some scan types, with
  // global_initialization, but why?
}

// --- Factory ---

duckdb::TableFunction CreateTableFullscanFunction() {
  duckdb::TableFunction func{
    "rocksdb_table_fullscan", {}, PKFullScanFunction, SereneDBScanBind,
    PKFullScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreatePKPointsLookupFunction() {
  duckdb::TableFunction func{
    "rocksdb_pk_points_lookup", {}, PKPointLookupFunction, SereneDBScanBind,
    PKPointLookupInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreatePKRangesScanFunction() {
  duckdb::TableFunction func{
    "rocksdb_pk_ranges_scan", {}, PKRangeScanFunction, SereneDBScanBind,
    PKRangeScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateSKFullscanFunction() {
  duckdb::TableFunction func{
    "rocksdb_sk_fullscan", {}, SKFullScanFunction, SereneDBScanBind,
    SKFullScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateSKPointsLookupFunction() {
  duckdb::TableFunction func{
    "rocksdb_sk_points_lookup", {}, SKPointLookupFunction, SereneDBScanBind,
    SKPointLookupInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateSKRangesScanFunction() {
  duckdb::TableFunction func{
    "rocksdb_sk_ranges_scan", {}, SKRangeScanFunction, SereneDBScanBind,
    SKRangeScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIResearchFullscanFunction() {
  duckdb::TableFunction func{
    "iresearch_fullscan", {}, PKFullScanFunction, SereneDBScanBind,
    PKFullScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIResearchScanFunction() {
  duckdb::TableFunction func{
    "iresearch_scan",         {}, SearchFullScanFunction, SereneDBScanBind,
    SearchFullScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIResearchCountFunction() {
  duckdb::TableFunction func{
    "iresearch_count",         {}, SearchCountScanFunction, SereneDBScanBind,
    SearchCountScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIResearchANNFullscanFunction() {
  duckdb::TableFunction func{
    "iresearch_ann_fullscan", {}, SearchAnnScanFunction, SereneDBScanBind,
    SearchAnnScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

duckdb::TableFunction CreateIResearchANNRangeScanFunction() {
  duckdb::TableFunction func{
    "iresearch_ann_range_scan", {}, SearchRangeScanFunction, SereneDBScanBind,
    SearchRangeScanInitGlobal,
  };
  SetCommonCallbacks(func);
  return func;
}

}  // namespace sdb::connector
