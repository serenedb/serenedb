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

#include <absl/strings/str_join.h>

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
#include "connector/duckdb_sk_full_scan.hpp"
#include "connector/duckdb_sk_point_lookup.hpp"
#include "connector/duckdb_sk_range_scan.hpp"
#include "connector/rocksdb_filter.hpp"
#include "connector/search_filter_printer.hpp"
#include "functions/search.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"

namespace sdb::connector {
namespace {

void CopyCommon(const SereneDBScanBindData& src, SereneDBScanBindData& dst) {
  dst.column_ids = src.column_ids;
  dst.column_types = src.column_types;
  dst.has_rowid = src.has_rowid;
  dst.table_entry = src.table_entry;
  dst.entry_kind = src.entry_kind;
  dst.inverted_index = src.inverted_index;
  dst.scan_source = src.scan_source->Clone();
  dst.lookup_label = src.lookup_label;
}

std::shared_ptr<search::InvertedIndexShard> ResolveInvertedIndexShard(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind) {
  if (!bind.inverted_index) {
    return nullptr;
  }
  auto& conn_ctx = GetSereneDBContext(context);
  auto cat_snapshot = conn_ctx.EnsureCatalogSnapshot();
  for (auto& s : cat_snapshot->GetIndexShardsByRelation(
         bind.inverted_index->GetRelationId())) {
    if (s->GetIndexId() == bind.inverted_index->GetId() &&
        s->GetType() == catalog::ObjectType::InvertedIndexShard) {
      return basics::downCast<search::InvertedIndexShard>(std::move(s));
    }
  }
  return nullptr;
}

duckdb::unique_ptr<duckdb::NodeStatistics> InvertedIndexCardinality(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind) {
  if (bind.scan_source && bind.scan_source->Kind() == ScanSourceKind::Search) {
    const auto& ss = bind.scan_source->Cast<SearchScan>();
    if (ss.snapshot) {
      return duckdb::make_uniq<duckdb::NodeStatistics>(
        static_cast<duckdb::idx_t>(ss.snapshot->reader.live_docs_count()));
    }
  }
  auto shard = ResolveInvertedIndexShard(context, bind);
  if (!shard) {
    return nullptr;
  }
  auto idx_snapshot = shard->GetInvertedIndexSnapshot();
  if (!idx_snapshot || !idx_snapshot->reader) {
    return nullptr;
  }
  return duckdb::make_uniq<duckdb::NodeStatistics>(
    static_cast<duckdb::idx_t>(idx_snapshot->reader->live_docs_count()));
}

}  // namespace

duckdb::unique_ptr<duckdb::FunctionData> TableScanBindData::Copy() const {
  auto copy = duckdb::make_uniq<TableScanBindData>();
  CopyCommon(*this, *copy);
  copy->table = table;
  return copy;
}

bool TableScanBindData::Equals(const duckdb::FunctionData& other) const {
  const auto& o = other.Cast<SereneDBScanBindData>();
  if (o.GetKind() != Kind::Table) {
    return false;
  }
  const auto& t = o.As<TableScanBindData>();
  return table == t.table && column_ids == t.column_ids;
}

duckdb::unique_ptr<duckdb::NodeStatistics> TableScanBindData::Cardinality(
  duckdb::ClientContext& context) const {
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto shard = snapshot->GetTableShard(table->GetId());
  if (!shard) {
    return nullptr;
  }
  auto stats = shard->GetTableStats();
  return duckdb::make_uniq<duckdb::NodeStatistics>(
    static_cast<duckdb::idx_t>(stats.num_rows));
}

ObjectId TableScanBindData::RelationId() const { return table->GetId(); }

std::string_view TableScanBindData::RelationName() const {
  return table->GetName();
}

catalog::Column::Id TableScanBindData::ColumnIdByName(
  std::string_view name) const {
  for (const auto& col : table->Columns()) {
    if (col.name == name) {
      return col.id;
    }
  }
  return kInvalidColumnId;
}

std::string_view TableScanBindData::ColumnNameById(
  catalog::Column::Id col_id) const {
  for (const auto& col : table->Columns()) {
    if (col.id == col_id) {
      return col.name;
    }
  }
  return {};
}

void TableScanBindData::IterateColumns(const ColumnVisitor& cb) const {
  for (const auto& col : table->Columns()) {
    cb(col.id, col.type);
  }
}

duckdb::unique_ptr<duckdb::FunctionData> ViewScanBindData::Copy() const {
  auto copy = duckdb::make_uniq<ViewScanBindData>();
  CopyCommon(*this, *copy);
  copy->view = view;
  return copy;
}

bool ViewScanBindData::Equals(const duckdb::FunctionData& other) const {
  const auto& o = other.Cast<SereneDBScanBindData>();
  if (o.GetKind() != Kind::View) {
    return false;
  }
  const auto& v = o.As<ViewScanBindData>();
  return view == v.view && column_ids == v.column_ids;
}

duckdb::unique_ptr<duckdb::NodeStatistics> ViewScanBindData::Cardinality(
  duckdb::ClientContext& context) const {
  return InvertedIndexCardinality(context, *this);
}

ObjectId ViewScanBindData::RelationId() const { return view->GetId(); }

std::string_view ViewScanBindData::RelationName() const {
  return view->GetName();
}

catalog::Column::Id ViewScanBindData::ColumnIdByName(
  std::string_view name) const {
  const auto& info = view->GetInfo();
  for (size_t i = 0; i < info.names.size(); ++i) {
    if (info.names[i] == name) {
      return static_cast<catalog::Column::Id>(i);
    }
  }
  return kInvalidColumnId;
}

std::string_view ViewScanBindData::ColumnNameById(
  catalog::Column::Id col_id) const {
  const auto& info = view->GetInfo();
  const auto idx = static_cast<size_t>(col_id);
  if (idx < info.names.size()) {
    return info.names[idx];
  }
  return {};
}

void ViewScanBindData::IterateColumns(const ColumnVisitor& cb) const {
  const auto& info = view->GetInfo();
  for (size_t i = 0; i < info.names.size(); ++i) {
    cb(static_cast<catalog::Column::Id>(i), info.types[i]);
  }
}

// ---------------------------------------------------------------------------

static duckdb::BindInfo SereneDBGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& data =
    const_cast<SereneDBScanBindData&>(bind_data->Cast<SereneDBScanBindData>());
  if (data.table_entry) {
    return duckdb::BindInfo(*data.table_entry);
  }
  return duckdb::BindInfo(duckdb::ScanType::TABLE);
}

duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  throw duckdb::InternalException(
    "SereneDBScanBind: should be provided via GetScanFunction");
}

static duckdb::unique_ptr<duckdb::NodeStatistics> SereneDBScanCardinality(
  duckdb::ClientContext& context, const duckdb::FunctionData* bind_data) {
  if (!bind_data) {
    return nullptr;
  }
  return bind_data->Cast<SereneDBScanBindData>().Cardinality(context);
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

// Prepared iresearch query/filter aren't duplicable; reset to a full scan.
std::unique_ptr<ScanSource> SearchScan::Clone() const {
  return std::make_unique<FullTableScan>();
}

std::unique_ptr<ScanSource> CountScan::Clone() const {
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

static std::string ColumnNameFor(const SereneDBScanBindData& bind,
                                 catalog::Column::Id col_id) {
  auto name = bind.ColumnNameById(col_id);
  if (!name.empty()) {
    return std::string{name};
  }
  return absl::StrCat("col", col_id);
}

static std::string FormatResolvedPoint(
  const ResolvedPoint& point, const SereneDBScanBindData& bind,
  std::span<const catalog::Column::Id> column_ids) {
  std::string out = "(";
  for (size_t i = 0; i < point.size(); ++i) {
    if (i) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, ColumnNameFor(bind, column_ids[i]), "=",
                    point[i].ToString());
  }
  absl::StrAppend(&out, ")");
  return out;
}

static std::string FormatResolvedRange(
  const ResolvedRange& range, const SereneDBScanBindData& bind,
  std::span<const catalog::Column::Id> column_ids) {
  std::string out = "{";
  for (size_t i = 0; i < range.prefix.size(); ++i) {
    if (i) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, ColumnNameFor(bind, column_ids[i]), "=",
                    range.prefix[i].ToString());
  }
  const auto range_col_idx = range.prefix.size();
  if (range_col_idx < column_ids.size()) {
    if (!range.prefix.empty()) {
      absl::StrAppend(&out, ", ");
    }
    absl::StrAppend(&out, ColumnNameFor(bind, column_ids[range_col_idx]), "=",
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
  if (points.empty()) {
    return;
  }
  auto cols = std::span<const catalog::Column::Id>(column_ids);
  out.insert("Filter", FormatClaimList(points, [&](const ResolvedPoint& pt) {
               return FormatResolvedPoint(pt, bind, cols);
             }));
}

void PkRangeScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (ranges.empty()) {
    return;
  }
  auto cols = std::span<const catalog::Column::Id>(column_ids);
  out.insert("Filter", FormatClaimList(ranges, [&](const ResolvedRange& rr) {
               return FormatResolvedRange(rr, bind, cols);
             }));
}

void SkPointScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!points.empty()) {
    auto cols = std::span<const catalog::Column::Id>(column_ids);
    out.insert("Filter", FormatClaimList(points, [&](const ResolvedPoint& pt) {
                 return FormatResolvedPoint(pt, bind, cols);
               }));
  }
  if (is_unique) {
    out.insert("Unique", "true");
  }
}

void SkRangeScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!ranges.empty()) {
    auto cols = std::span<const catalog::Column::Id>(column_ids);
    out.insert("Filter", FormatClaimList(ranges, [&](const ResolvedRange& rr) {
                 return FormatResolvedRange(rr, bind, cols);
               }));
  }
  if (is_unique) {
    out.insert("Unique", "true");
  }
}

namespace {

void AppendVectorSearchSummary(
  const SereneDBScanBindData& bind, const VectorSearchScan& scan,
  duckdb::InsertionOrderPreservingMap<std::string>& out) {
  out.insert("Dims", std::to_string(scan.query_vector.size()));
  if (scan.text_filter_root) {
    auto col_name = [&bind](catalog::Column::Id col_id) -> std::string_view {
      static thread_local std::string fallback;
      auto name = bind.ColumnNameById(col_id);
      if (!name.empty()) {
        return name;
      }
      fallback = absl::StrCat("col", col_id);
      return fallback;
    };
    SDB_ASSERT(scan.text_filter_root);
    out.insert("TextFilter",
               irs::ToStringDemangled(*scan.text_filter_root, col_name));
  }
  if (!scan.filter_expression) {
    return;
  }
  auto repr = scan.filter_expression->ToString();
  if (repr.empty()) {
    return;
  }
  out.insert("Filter", std::move(repr));
}

}  // namespace

void ANNScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  out.insert("TopK", std::to_string(top_k));
  AppendVectorSearchSummary(bind, *this, out);
}

void RangeSearchScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  out.insert("Radius", std::to_string(radius));
  AppendVectorSearchSummary(bind, *this, out);
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
  if (scorer) {
    out.insert("Score", scorer->ToString());
  }
  if (score_top_k) {
    std::string topk_val = std::to_string(*score_top_k);
    if (WandEnabled()) {
      absl::StrAppend(&topk_val, ", optimized");
    }
    out.insert("TopK", std::move(topk_val));
  }
  if (EmitOffsets()) {
    auto cols =
      absl::StrJoin(offsets | std::views::transform([&](const auto& off) {
                      return ColumnNameFor(bind, off.column_id);
                    }),
                    ", ");
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
  if (bind.table_entry) {
    const char* kind =
      bind.entry_kind == ScanEntryKind::BaseTable ? "Table" : "Index";
    result.insert(kind, std::string{bind.table_entry->name});
  } else {
    const char* kind = bind.IsViewBacked() ? "View" : "Table";
    result.insert(kind, std::string{bind.RelationName()});
  }
  if (!bind.lookup_label.empty()) {
    result.insert("Lookup", bind.lookup_label);
  }
  bind.scan_source->AppendSummary(bind, result);
  return result;
}

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

duckdb::TableFunction CreateIResearchANNScanFunction() {
  duckdb::TableFunction func{
    "iresearch_ann_scan",    {}, SearchAnnScanFunction, SereneDBScanBind,
    SearchAnnScanInitGlobal,
  };
  SetCommonCallbacks(func);
  func.init_local = SearchAnnScanInitLocal;
  return func;
}

duckdb::TableFunction CreateIResearchANNRangeScanFunction() {
  duckdb::TableFunction func{
    "iresearch_ann_range_scan", {}, SearchRangeScanFunction, SereneDBScanBind,
    SearchRangeScanInitGlobal,
  };
  SetCommonCallbacks(func);
  func.init_local = SearchRangeScanInitLocal;
  return func;
}

}  // namespace sdb::connector
