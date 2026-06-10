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

#include <absl/algorithm/container.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>

#include "catalog/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_pk_full_scan.hpp"
#include "connector/duckdb_pk_point_lookup.hpp"
#include "connector/duckdb_pk_range_scan.hpp"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_search_ann_scan.h"
#include "connector/duckdb_search_full_scan.hpp"
#include "connector/duckdb_sk_full_scan.hpp"
#include "connector/duckdb_sk_point_lookup.hpp"
#include "connector/duckdb_sk_range_scan.hpp"
#include "connector/duckdb_table_entry.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/optimizer/rocksdb_plan.h"
#include "connector/rocksdb_filter.hpp"
#include "connector/search_filter_printer.hpp"
#include "functions/search.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"

#define SDB_ROCKSDB_SCAN_SOURCE_KINDS  \
  case ScanSourceKind::FullTable:      \
  case ScanSourceKind::SecondaryIndex: \
  case ScanSourceKind::PkPoint:        \
  case ScanSourceKind::PkRange:        \
  case ScanSourceKind::SkPoint:        \
  case ScanSourceKind::SkRange

namespace sdb::connector {
namespace {

void CopyCommon(const SereneDBScanBindData& src, SereneDBScanBindData& dst) {
  dst.column_ids = src.column_ids;
  dst.column_types = src.column_types;
  dst.is_create_index = src.is_create_index;
  dst.table_entry = src.table_entry;
  dst.entry_kind = src.entry_kind;
  dst.inverted_index = src.inverted_index;
  dst.scan_source = src.scan_source->Clone();
  dst.lookup_label = src.lookup_label;
}

uint64_t EstimateFilterMatchCount(const irs::Filter& filter,
                                  uint64_t live_docs) {
  const auto type = filter.type();
  if (type == irs::Type<irs::All>::id()) {
    return live_docs;
  }
  if (type == irs::Type<irs::Empty>::id()) {
    return 0;
  }
  // DuckDB's RelationStatisticsHelper::DEFAULT_SELECTIVITY
  constexpr double kDefaultFilterSelectivity = 0.2;
  return std::max<uint64_t>(live_docs * kDefaultFilterSelectivity, 1U);
}

duckdb::unique_ptr<duckdb::NodeStatistics> InvertedIndexCardinality(
  const SereneDBScanBindData& bind) {
  const auto& ss = bind.scan_source->Cast<SearchScan>();
  const auto live = ss.snapshot->reader.live_docs_count();
  const auto estimate =
    ss.stored_filter ? EstimateFilterMatchCount(*ss.stored_filter, live) : live;
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate, live);
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
  const auto num_rows =
    static_cast<duckdb::idx_t>(shard->GetTableStats().num_rows);
  if (scan_source) {
    switch (scan_source->Kind()) {
      case ScanSourceKind::Search:
        return InvertedIndexCardinality(*this);
      case ScanSourceKind::PkPoint: {
        const auto& pk = scan_source->Cast<PkPointScan>();
        const auto n = std::min<duckdb::idx_t>(pk.points.size(), num_rows);
        return duckdb::make_uniq<duckdb::NodeStatistics>(n, n);
      }
      case ScanSourceKind::SkPoint: {
        const auto& sk = scan_source->Cast<SkPointScan>();
        if (sk.is_unique) {
          const auto n = std::min<duckdb::idx_t>(sk.points.size(), num_rows);
          return duckdb::make_uniq<duckdb::NodeStatistics>(n, n);
        }
      } break;
      default:
        break;
    }
  }
  return duckdb::make_uniq<duckdb::NodeStatistics>(num_rows, num_rows);
}

ObjectId TableScanBindData::RelationId() const { return table->GetId(); }

std::string_view TableScanBindData::RelationName() const {
  return table->GetName();
}

catalog::Column::Id TableScanBindData::ColumnIdByName(
  std::string_view name) const {
  for (const auto& col : table->Columns()) {
    if (col.GetName() == name) {
      return col.GetId();
    }
  }
  return kInvalidColumnId;
}

std::string_view TableScanBindData::ColumnNameById(
  catalog::Column::Id col_id) const {
  for (const auto& col : table->Columns()) {
    if (col.GetId() == col_id) {
      return col.GetName();
    }
  }
  return {};
}

duckdb::LogicalType TableScanBindData::ColumnTypeById(
  catalog::Column::Id col_id) const {
  for (const auto& col : table->Columns()) {
    if (col.GetId() == col_id) {
      return col.type;
    }
  }
  return duckdb::LogicalType::INVALID;
}

void TableScanBindData::IterateColumns(const ColumnVisitor& cb) const {
  for (const auto& col : table->Columns()) {
    cb(col.GetId(), col.type);
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
  duckdb::ClientContext& /*context*/) const {
  if (scan_source->Kind() != ScanSourceKind::Search) {
    return nullptr;
  }
  return InvertedIndexCardinality(*this);
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

duckdb::LogicalType ViewScanBindData::ColumnTypeById(
  catalog::Column::Id col_id) const {
  const auto& info = view->GetInfo();
  const auto idx = static_cast<size_t>(col_id);
  if (idx < info.types.size()) {
    return info.types[idx];
  }
  return duckdb::LogicalType::INVALID;
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

std::unique_ptr<ScanSource> SearchScan::Clone() const {
  return std::make_unique<SearchScan>(*this);
}

bool SearchScan::IsMatchAll() const noexcept { return !stored_filter; }

bool WandEnabled(const catalog::InvertedIndex* index,
                 const std::optional<catalog::ScorerOptions>& scorer) {
  if (!index) {
    return false;
  }
  const auto& topk = index->GetTopKScorer();
  return topk && topk == scorer;
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

void SearchScan::AppendSummary(
  const SereneDBScanBindData& bind,
  duckdb::InsertionOrderPreservingMap<std::string>& out) const {
  if (!vector_scorer && stored_filter && bind.inverted_index) {
    out.insert("Filter", irs::ToStringDemangled(
                           *stored_filter,
                           MakeFieldNameResolver(bind, *bind.inverted_index)));
  }
  if (count_only) {
    out.insert("Output", "row-count only");
    return;
  }
  if (text_scorer) {
    out.insert("Score", text_scorer->ToString());
  }
  if (score_top_k) {
    std::string topk_val = std::to_string(*score_top_k);
    if (WandEnabled(bind.inverted_index.get(), text_scorer)) {
      absl::StrAppend(&topk_val, ", optimized");
    }
    out.insert("Top", std::move(topk_val));
  }
  if (vector_scorer) {
    if (vector_scorer->radius != std::numeric_limits<float>::max()) {
      out.insert("Radius", std::to_string(vector_scorer->radius));
    }
    out.insert("Dims", std::to_string(vector_scorer->query_vector.size()));
    if (stored_filter && bind.inverted_index) {
      out.insert(
        "TextFilter",
        irs::ToStringDemangled(
          *stored_filter, MakeFieldNameResolver(bind, *bind.inverted_index)));
    }
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

struct ProjectionEntry {
  std::string name;
  bool from_index = false;
  bool is_virtual = false;
};

std::string ProjectionDisplayName(const SereneDBScanBindData& bind,
                                  const duckdb::ColumnIndex& column_index,
                                  const duckdb::vector<std::string>& names) {
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id < names.size()) {
    return names[col_id];
  }
  if (const auto pk_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      pk_idx != duckdb::DConstants::INVALID_INDEX) {
    if (const auto* tbd = dynamic_cast<const TableScanBindData*>(&bind)) {
      const auto& cols = tbd->table->Columns();
      if (pk_idx < cols.size()) {
        return std::string{cols[pk_idx].GetName()};
      }
    }
  }
  if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
    return "row_id";
  }
  if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_NUMBER) {
    return "row_number";
  }
  if (col_id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
    return "file_index";
  }
  if (col_id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
    return "file_row_number";
  }
  if (col_id == kColumnIdentifierTableOid) {
    return "tableoid";
  }
  if (col_id == kColumnIdentifierGeneratedPk) {
    return "generated_pk";
  }
  return absl::StrCat("column_", col_id);
}

bool ProjectionIsFromIndex(const SereneDBScanBindData& bind,
                           const duckdb::ColumnIndex& column_index) {
  if (!bind.IsInvertedIndexEntry() || !bind.inverted_index) {
    return false;
  }
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
      col_id >= duckdb::VIRTUAL_COLUMN_START ||
      col_id >= bind.column_ids.size()) {
    return false;
  }
  const auto catalog_col_id = bind.column_ids[col_id];
  if (catalog_col_id == catalog::Column::kGeneratedPKId) {
    return true;
  }
  const auto* info = bind.inverted_index->FindColumnInfo(catalog_col_id);
  return info != nullptr && info->IsStored();
}

bool ProjectionIsVirtual(const SereneDBScanBindData& bind,
                         const duckdb::ColumnIndex& column_index) {
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id >= duckdb::VIRTUAL_COLUMN_START ||
      col_id >= bind.column_ids.size()) {
    return false;
  }
  const auto catalog_col_id = bind.column_ids[col_id];
  return catalog_col_id == catalog::Column::kInvertedIndexScoreId ||
         catalog_col_id == catalog::Column::kInvertedIndexOffsetsId;
}

std::vector<ProjectionEntry> BuildProjectionEntries(
  const SereneDBScanBindData& bind,
  const duckdb::TableFunctionToStringInput& input) {
  std::vector<ProjectionEntry> entries;
  if (!input.projected_column_ids || !input.projected_names) {
    return entries;
  }
  const auto& column_ids = *input.projected_column_ids;
  const auto& names = *input.projected_names;
  const auto count =
    input.projected_filter_prune
      ? (input.projection_ids ? input.projection_ids->size() : 0)
      : column_ids.size();
  entries.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto base_index =
      input.projected_filter_prune ? (*input.projection_ids)[i] : i;
    if (base_index >= column_ids.size()) {
      continue;
    }
    const auto& column_index = column_ids[base_index];
    const auto col_id = column_index.GetPrimaryIndex();
    if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      continue;
    }
    entries.push_back({
      .name = ProjectionDisplayName(bind, column_index, names),
      .from_index = ProjectionIsFromIndex(bind, column_index),
      .is_virtual = ProjectionIsVirtual(bind, column_index),
    });
  }
  return entries;
}

std::string FormatProjections(const std::vector<ProjectionEntry>& entries,
                              bool annotate) {
  std::string out;
  for (const auto& e : entries) {
    if (!out.empty()) {
      absl::StrAppend(&out, "\n");
    }
    if (annotate && !e.is_virtual) {
      // TODO(mbkkt) Rename l (lookup) to r (relation).
      absl::StrAppend(&out, e.name, " (", e.from_index ? "i" : "l", ")");
    } else {
      absl::StrAppend(&out, e.name);
    }
  }
  return out;
}

static duckdb::InsertionOrderPreservingMap<std::string> SereneDBScanToString(
  duckdb::TableFunctionToStringInput& input) {
  duckdb::InsertionOrderPreservingMap<std::string> result;
  if (!input.bind_data) {
    return result;
  }
  auto& bind = input.bind_data->Cast<SereneDBScanBindData>();
  const auto search_tag = [&]() -> const char* {
    if (!bind.scan_source) {
      return "";
    }
    if (bind.scan_source->Kind() == ScanSourceKind::Search &&
        bind.scan_source->Cast<SearchScan>().vector_scorer) {
      const auto& ss = bind.scan_source->Cast<SearchScan>();
      if (!ss.score_top_k &&
          ss.vector_scorer->radius != std::numeric_limits<float>::max()) {
        return " (ANN range)";
      }
      return " (ANN)";
    }
    return "";
  }();
  if (bind.table_entry) {
    const char* kind =
      bind.entry_kind == ScanEntryKind::BaseTable ? "Table" : "Index";
    result.insert(kind, absl::StrCat(bind.table_entry->name, search_tag));
  } else {
    const char* kind = bind.IsViewBacked() ? "View" : "Table";
    result.insert(kind, absl::StrCat(bind.RelationName(), search_tag));
  }
  const auto entries = BuildProjectionEntries(bind, input);
  bool has_index = false;
  bool has_lookup = false;
  for (const auto& e : entries) {
    if (e.is_virtual) {
      continue;  // scan-emitted virtuals don't gate the (i)/(l) annotation
    }
    if (e.from_index) {
      has_index = true;
    } else {
      has_lookup = true;
    }
  }
  const bool count_only =
    bind.scan_source && bind.scan_source->Kind() == ScanSourceKind::Search &&
    input.projected_column_ids &&
    absl::c_all_of(
      *input.projected_column_ids, [](const duckdb::ColumnIndex& ci) {
        return ci.GetPrimaryIndex() == duckdb::COLUMN_IDENTIFIER_EMPTY;
      });
  const bool suppress_lookup =
    bind.IsInvertedIndexEntry() &&
    (count_only || (!entries.empty() && !has_lookup));
  if (!bind.lookup_label.empty() && !suppress_lookup) {
    result.insert("Lookup", bind.lookup_label);
  }
  bind.scan_source->AppendSummary(bind, result);
  if (count_only) {
    result.insert("Output", "row-count only");
  }
  if (!entries.empty()) {
    const bool annotate = has_index && has_lookup;
    result.insert("Projections", FormatProjections(entries, annotate));
  }
  return result;
}

static void SetCommonCallbacks(duckdb::TableFunction& func) {
  // TODO(mbkkt) Maybe we can use bind_replace/bind_operator to make indexes?
  func.init_local = CommonScanInitLocal;  // TODO: Use separate callbacks
  // TODO: Provide statistics
  // TODO: Better cardinality estimates
  func.cardinality = SereneDBScanCardinality;
  func.get_metrics = CommonScanGetMetrics;
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

void FullTablePushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  optimizer::IResearchPushdownComplexFilter(context, get, bind_data, filters);
  optimizer::RocksDBPushdownComplexFilter(context, get, bind_data, filters);
}

duckdb::TableFunction CreateTableFullscanFunction() {
  duckdb::TableFunction func{
    "rocksdb_table_fullscan", {}, PKFullScanFunction, SereneDBScanBind,
    PKFullScanInitGlobal,
  };
  SetCommonCallbacks(func);
  func.pushdown_complex_filter = &FullTablePushdownComplexFilter;
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
  func.pushdown_complex_filter = &optimizer::RocksDBPushdownComplexFilter;
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

namespace {

bool IsCountOnlyScan(const SereneDBScanBindData& bind_data,
                     const duckdb::TableFunctionInitInput& input) {
  return absl::c_none_of(input.column_ids, [&](auto col_id) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      return false;
    }
    if (col_id == kColumnIdentifierGeneratedPk ||
        col_id == kColumnIdentifierTableOid ||
        col_id >= duckdb::VIRTUAL_COLUMN_START) {
      return true;
    }
    return col_id < bind_data.column_ids.size();
  });
}

bool IsAnnScan(const SereneDBScanBindData& bind_data) {
  return !!bind_data.scan_source->Cast<SearchScan>().vector_scorer;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = const_cast<SereneDBScanBindData&>(
    input.bind_data->Cast<SereneDBScanBindData>());

  bind_data.scan_source->Cast<SearchScan>().count_only =
    IsCountOnlyScan(bind_data, input);

  switch (bind_data.scan_source->Kind()) {
    case ScanSourceKind::Search:
      return IsAnnScan(bind_data) ? SearchAnnScanInitGlobal(context, input)
                                  : SearchFullScanInitGlobal(context, input);
    SDB_ROCKSDB_SCAN_SOURCE_KINDS:
      SDB_UNREACHABLE();
  }
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  switch (bind_data.scan_source->Kind()) {
    case ScanSourceKind::Search:
      return IsAnnScan(bind_data)
               ? SearchAnnScanInitLocal(context, input, global_state)
               : SearchFullScanInitLocal(context, input, global_state);
    SDB_ROCKSDB_SCAN_SOURCE_KINDS:
      SDB_UNREACHABLE();
  }
}

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output) {
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  switch (bind_data.scan_source->Kind()) {
    case ScanSourceKind::Search:
      return IsAnnScan(bind_data)
               ? SearchAnnScanFunction(context, data, output)
               : SearchFullScanFunction(context, data, output);
    SDB_ROCKSDB_SCAN_SOURCE_KINDS:
      SDB_UNREACHABLE();
  }
}

}  // namespace

duckdb::TableFunction CreateIResearchScanFunction() {
  duckdb::TableFunction func{
    "iresearch_scan",        {}, IResearchScanFunction, SereneDBScanBind,
    IResearchScanInitGlobal,
  };
  SetCommonCallbacks(func);
  func.init_local = IResearchScanInitLocal;
  func.pushdown_complex_filter = &optimizer::IResearchPushdownComplexFilter;
  func.set_scan_order = &IResearchSetScanOrder;
  return func;
}

}  // namespace sdb::connector
