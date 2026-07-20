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
#include <duckdb/common/types/variant.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/optimizer/column_lifetime_analyzer.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/vector_radius_filter.hpp>
#include <iresearch/search/vector_similarity_filter.hpp>

#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_search_full_scan.hpp"
#include "connector/duckdb_table_entry.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/search_filter_printer.hpp"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {

uint32_t ReadBoundedIntSetting(duckdb::ClientContext& context,
                               std::string_view name, int32_t min_inclusive,
                               uint32_t default_value) {
  duckdb::Value v;
  if (context.TryGetCurrentSetting(std::string{name}, v) && !v.IsNull()) {
    const auto n = v.GetValue<int32_t>();
    if (n >= min_inclusive) {
      return static_cast<uint32_t>(n);
    }
  }
  return default_value;
}

namespace {

void CopyCommon(const SereneDBScanBindData& src, SereneDBScanBindData& dst) {
  dst.column_ids = src.column_ids;
  dst.column_types = src.column_types;
  dst.table_entry = src.table_entry;
  dst.entry_kind = src.entry_kind;
  dst.inverted_index = src.inverted_index;
  dst.stored_filter = src.stored_filter;
  dst.snapshot = src.snapshot;
  dst.text_scorer = src.text_scorer;
  dst.vector_scorer = src.vector_scorer;
  dst.score_top_k = src.score_top_k;
  dst.score_order = src.score_order;
  dst.score_static_floor = src.score_static_floor;
  dst.offsets = src.offsets;
  dst.ts_dicts = src.ts_dicts;
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
  // if (type == irs::Type<irs::ByVectorSimilarity>::id()) {
  //   auto& f = basics::downCast<irs::ByVectorSimilarity>(filter);
  //   if options no radius and no top, we need just return live_docs
  // }
  // DuckDB's RelationStatisticsHelper::DEFAULT_SELECTIVITY
  constexpr double kDefaultFilterSelectivity = 0.2;
  return std::max<uint64_t>(live_docs * kDefaultFilterSelectivity, 1U);
}

duckdb::unique_ptr<duckdb::NodeStatistics> TsDictEstimation(
  const SereneDBScanBindData& ss) {
  uint64_t estimate = 0;
  for (const auto& req : ss.ts_dicts) {
    const uint64_t rows = [&] -> uint64_t {
      if (req.term_uses == (TsDictTermUses::kMin | TsDictTermUses::kMax)) {
        return 2;
      }
      if (req.term_uses == TsDictTermUses::kMin ||
          req.term_uses == TsDictTermUses::kMax) {
        return 1;
      }
      uint64_t terms = 0;
      for (const auto& segment : ss.snapshot->reader) {
        if (const auto* field =
              segment.field(static_cast<irs::field_id>(req.field_id))) {
          terms += field->size();
        }
      }
      return std::max<uint64_t>(terms, 1);
    }();
    estimate += req.having_filter
                  ? EstimateFilterMatchCount(*req.having_filter, rows)
                  : rows;
  }
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate);
}

duckdb::unique_ptr<duckdb::NodeStatistics> InvertedIndexCardinality(
  const SereneDBScanBindData& bind) {
  const auto& ss = bind;
  const auto live = ss.snapshot->reader.live_docs_count();
  if (ss.TsDictMode()) {
    return TsDictEstimation(ss);
  }
  const auto* filter = ss.stored_filter.get();
  const auto estimate = filter ? EstimateFilterMatchCount(*filter, live) : live;
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
  duckdb::ClientContext&) const {
  return InvertedIndexCardinality(*this);
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
    if (info.names[i].GetIdentifierName() == name) {
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
    return info.names[idx].GetIdentifierName();
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
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INTERNAL_ERROR),
    ERR_MSG("SereneDBScanBind: should be provided via GetScanFunction"));
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

bool WandEnabled(const catalog::InvertedIndex* index,
                 const std::optional<catalog::ScorerOptions>& scorer) {
  if (!index) {
    return false;
  }
  // TODO(mbkkt) use compatibility instead
  const auto& topk = index->GetTopKScorer();
  return topk && topk == scorer;
}

std::string SereneDBScanBindData::DisplayColumnName(
  catalog::Column::Id col_id) const {
  auto name = ColumnNameById(col_id);
  if (!name.empty()) {
    return std::string{name};
  }
  if (inverted_index) {
    const auto* expr =
      inverted_index->ExpressionByFieldId(static_cast<irs::field_id>(col_id));
    if (expr && !expr->pretty_printed.empty()) {
      return expr->pretty_printed;
    }
  }
  return absl::StrCat("col", col_id);
}

static std::string ColumnNameFor(const SereneDBScanBindData& bind,
                                 catalog::Column::Id col_id) {
  auto name = bind.ColumnNameById(col_id);
  if (!name.empty()) {
    return std::string{name};
  }
  return absl::StrCat("col", col_id);
}

irs::Filter::ptr MakeVectorFilter(const VectorScorerOptions& vs,
                                  std::shared_ptr<const irs::Filter> inner,
                                  float radius) {
  if (vs.radius != std::numeric_limits<float>::max()) {
    auto f = std::make_unique<irs::ByRadius>();
    *f->mutable_field_id() = vs.field_id;
    auto* o = f->mutable_options();
    o->query = vs.query_vector;
    o->centroids_id = vs.centroids_id;
    o->postings_id = vs.postings_id;
    o->metric = vs.metric;
    o->radius = radius;
    o->inclusive = vs.radius_inclusive;
    o->inner = std::move(inner);
    return f;
  }
  auto f = std::make_unique<irs::ByVectorSimilarity>();
  *f->mutable_field_id() = vs.field_id;
  auto* o = f->mutable_options();
  o->query = vs.query_vector;
  o->centroids_id = vs.centroids_id;
  o->postings_id = vs.postings_id;
  o->metric = vs.metric;
  o->quant = vs.quant;
  o->nprobe = vs.nprobe;
  o->inner = std::move(inner);
  return f;
}

namespace {

auto MakeFieldNameResolver(const SereneDBScanBindData& bind_data,
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

auto MakeFieldKindResolver(const SereneDBScanBindData& bind_data,
                           const catalog::InvertedIndex& index) {
  return [&bind_data,
          &index](catalog::Column::Id col_id) -> catalog::term_dict::Kind {
    using catalog::term_dict::Kind;
    const auto fid = static_cast<irs::field_id>(col_id);
    const auto lookup = index.LookupField(fid);
    if (lookup.entry_field_id == catalog::term_dict::kPKFieldId) {
      return Kind::NumericI64;
    }
    if (lookup.entry) {
      const auto& entry = *lookup.entry;
      if (fid == lookup.entry_field_id) {
        const auto* expr = index.ExpressionByFieldId(fid);
        if (expr) {
          return catalog::term_dict::Classify(expr->return_type.id());
        }
        const auto column_type = bind_data.ColumnTypeById(col_id);
        if (column_type.id() != duckdb::LogicalTypeId::INVALID) {
          return catalog::term_dict::Classify(column_type.id());
        }
        return Kind::String;
      }
      if (fid == entry.null_field_id) {
        return Kind::Null;
      }
      if (fid == entry.bool_field_id) {
        return Kind::Bool;
      }
      if (fid == entry.numeric_field_id) {
        return Kind::NumericF64;
      }
    }
    const auto column_type = bind_data.ColumnTypeById(col_id);
    if (column_type.id() != duckdb::LogicalTypeId::INVALID) {
      return catalog::term_dict::Classify(column_type.id());
    }
    return Kind::Unsupported;
  };
}

}  // namespace

void SereneDBScanBindData::AppendSummary(
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) const {
  const auto& bind = *this;
  // Indexed expressions have no catalog column name and their synthetic
  // field ids come from a global allocator; display the pretty-printed
  // expression so EXPLAIN output is meaningful and deterministic.
  const auto display_field = [&](catalog::Column::Id id) -> std::string {
    if (bind.inverted_index) {
      if (const auto* expr = bind.inverted_index->ExpressionByFieldId(
            static_cast<irs::field_id>(id));
          expr && !expr->pretty_printed.empty()) {
        return expr->pretty_printed;
      }
    }
    return ColumnNameFor(bind, id);
  };
  if (bind.inverted_index) {
    const auto name_of = MakeFieldNameResolver(bind, *bind.inverted_index);
    const auto kind_of = MakeFieldKindResolver(bind, *bind.inverted_index);
    if (vector_scorer) {
      const auto display =
        MakeVectorFilter(*vector_scorer, stored_filter, vector_scorer->radius);
      out.insert("Index Filter", duckdb::ExplainValue(irs::ToExplainNode(
                                   *display, name_of, kind_of)));
    } else if (stored_filter) {
      out.insert("Index Filter", duckdb::ExplainValue(irs::ToExplainNode(
                                   *stored_filter, name_of, kind_of)));
    }
    for (const auto& req : ts_dicts) {
      if (!req.having_filter) {
        continue;
      }
      // TODO(gnusi): Maybe different name? But what?
      auto key =
        ts_dicts.size() == 1
          ? std::string{"Index Filter"}
          : absl::StrCat(
              "Index Filter(",
              display_field(static_cast<catalog::Column::Id>(req.field_id)),
              ")");
      out.insert(std::move(key), duckdb::ExplainValue(irs::ToExplainNode(
                                   *req.having_filter, name_of, kind_of)));
    }
  }
  if (text_scorer) {
    out.insert("Score", text_scorer->ToString());
  }
  if (score_top_k) {
    // TODO(mbkkt): prunnable/etc instead of optimized?
    // TODO(mbkkt): streaming top k also should be marked when wand enabled
    std::string topk_val = absl::StrCat(*score_top_k);
    if (WandEnabled(bind.inverted_index.get(), text_scorer)) {
      absl::StrAppend(&topk_val, ", optimized");
    }
    out.insert("Top", std::move(topk_val));
  }
  if (EmitOffsets()) {
    auto cols =
      absl::StrJoin(offsets | std::views::transform([&](const auto& off) {
                      return bind.DisplayColumnName(off.column_id);
                    }),
                    ", ");
    out.insert("Offsets", std::move(cols));
  }
  if (TsDictMode()) {
    auto names =
      absl::StrJoin(ts_dicts | std::views::transform([&](const auto& req) {
                      return display_field(catalog::Column::Id{req.field_id});
                    }),
                    ", ");
    out.insert("TsDict", std::move(names));
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
    if (column_index.IsPushdownExtract() && column_index.HasChildren() &&
        col_id < bind.column_types.size()) {
      std::vector<std::string_view> path{names[col_id]};
      DecodeExtractPath(column_index, bind.column_types[col_id], path);
      return absl::StrJoin(path, ".");
    }
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
         catalog_col_id == catalog::Column::kInvertedIndexOffsetsId ||
         catalog_col_id == catalog::Column::kInvertedIndexTermId ||
         catalog_col_id == catalog::Column::kInvertedIndexTermRawId ||
         catalog_col_id == catalog::Column::kInvertedIndexTermCountId ||
         catalog_col_id == catalog::Column::kInvertedIndexTermFreqId ||
         catalog_col_id == catalog::Column::kInvertedIndexTermScoreId;
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

static duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>
SereneDBScanToValue(duckdb::TableFunctionToStringInput& input) {
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> result;
  if (!input.bind_data) {
    return result;
  }
  auto& bind = input.bind_data->Cast<SereneDBScanBindData>();
  if (bind.table_entry) {
    const char* kind =
      bind.entry_kind == ScanEntryKind::BaseTable ? "Table" : "Index";
    result.insert(kind,
                  std::string{bind.table_entry->name.GetIdentifierName()});
  } else {
    const char* kind = bind.IsViewBacked() ? "View" : "Table";
    result.insert(kind, std::string{bind.RelationName()});
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
  // Count-only = every OUTPUT column is the empty virtual column; filter-only
  // columns (in projected_column_ids but excluded by projection_ids) are read
  // for their filters without being emitted, so they don't disqualify.
  bool count_only = input.projected_column_ids != nullptr;
  if (count_only) {
    const auto& column_ids = *input.projected_column_ids;
    // an empty projection_ids means no pruning: the output is every column
    const bool use_projection = input.projected_filter_prune &&
                                input.projection_ids &&
                                !input.projection_ids->empty();
    const auto count =
      use_projection ? input.projection_ids->size() : column_ids.size();
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const auto base_index = use_projection ? (*input.projection_ids)[i] : i;
      if (base_index < column_ids.size() &&
          column_ids[base_index].GetPrimaryIndex() !=
            duckdb::COLUMN_IDENTIFIER_EMPTY) {
        count_only = false;
        break;
      }
    }
  }
  // A filter on a lookup (source-only) column keeps the lookup load-bearing
  // even when the scan's output is count-only.
  bool has_lookup_filter = false;
  if (input.filters && input.projected_column_ids &&
      bind.IsInvertedIndexEntry() && bind.inverted_index) {
    const auto& column_ids = *input.projected_column_ids;
    for (const auto& entry : *input.filters) {
      const auto proj = static_cast<duckdb::idx_t>(entry.GetIndex());
      if (proj >= column_ids.size() || column_ids[proj].IsVirtualColumn()) {
        continue;
      }
      const auto bind_idx = column_ids[proj].GetPrimaryIndex();
      if (bind_idx >= bind.column_ids.size()) {
        continue;
      }
      const auto col_id = bind.column_ids[bind_idx];
      if (col_id == catalog::Column::kInvertedIndexScoreId) {
        continue;
      }
      const auto* info = bind.inverted_index->FindColumnInfo(col_id);
      if (!info || !info->IsStored()) {
        has_lookup_filter = true;
        break;
      }
    }
  }
  const bool suppress_lookup =
    bind.IsInvertedIndexEntry() && !has_lookup_filter &&
    (count_only || (!entries.empty() && !has_lookup));
  if (!bind.lookup_label.empty() && !suppress_lookup) {
    result.insert("Lookup", bind.lookup_label);
  }
  bind.AppendSummary(result);
  // Top-k enforces the floor via the collectors for any scorer; streaming
  // only puts it to work as the text WAND threshold.
  if (bind.score_static_floor > std::numeric_limits<float>::lowest() &&
      (bind.score_top_k || bind.text_scorer)) {
    result.insert("Min Score", absl::StrCat(bind.score_static_floor));
  }
  if (count_only) {
    result.insert("Output", "row-count only");
  }
  if (!entries.empty()) {
    const bool annotate = has_index && has_lookup;
    result.insert("Projections", FormatProjections(entries, annotate));
  }
  return result;
}

static double IResearchScanProgress(
  duckdb::ClientContext&, const duckdb::FunctionData*,
  const duckdb::GlobalTableFunctionState* gstate_p) {
  const auto& gstate = gstate_p->Cast<IResearchScanGlobalState>();
  if (gstate.total_segments == 0) {
    return -1;
  }
  const auto claimed = std::min<uint64_t>(
    gstate.next_segment.load(std::memory_order_relaxed), gstate.total_segments);
  return 100.0 * static_cast<double>(claimed) /
         static_cast<double>(gstate.total_segments);
}

namespace {

bool IResearchSupportsPushdownExtract(const duckdb::FunctionData& bind_data_p,
                                      const duckdb::LogicalIndex& col_idx) {
  const auto& bind = bind_data_p.Cast<SereneDBScanBindData>();
  if (!bind.IsInvertedIndexEntry() || !bind.inverted_index) {
    return false;
  }
  const auto bind_col = col_idx.index;
  if (bind_col >= bind.column_ids.size()) {
    return false;
  }
  const auto type_id = bind.column_types[bind_col].id();
  if (type_id != duckdb::LogicalTypeId::VARIANT &&
      type_id != duckdb::LogicalTypeId::STRUCT) {
    return false;
  }
  const auto* info =
    bind.inverted_index->FindColumnInfo(bind.column_ids[bind_col]);
  return info != nullptr && info->store_values;
}

// A user-space score lower bound is a raw-space collector floor only where
// the user-facing score IS the raw score: text scorers, and Identity-emit
// vector scorers whose collector holds exact (unquantized) scores. The
// decreasing emits (l2/cosine distance, ...) invert the bound -- their
// near-keeping bounds are consumed as the vector query radius instead -- and
// a quantized collector's approximate scores must not be cut by an exact
// floor.
bool ScoreFloorApplies(const SereneDBScanBindData& bind) {
  if (bind.text_scorer) {
    return true;
  }
  return bind.vector_scorer &&
         bind.vector_scorer->score_emit == ScoreEmit::Identity &&
         bind.vector_scorer->quant == irs::VectorQuantization::None;
}

// Score-column filter policy. The filter may be one predicate or an AND
// combination (a TableFilterSet holds one filter per column). Static lower
// bounds (`score > c` / `>= c`) are recorded as `score_static_floor`: it
// seeds the streaming WAND threshold, the top-k collectors and the Min Score
// display. On the top-k collector path the scan enforces score bounds itself,
// so those conjuncts are stripped from the filter: the dynamic TOP_N boundary
// (the collector maintains its own) and static lower bounds where the floor
// is the collector's raw space (ScoreFloorApplies) -- the collector starts at
// the floor. An empty residue drops the filter from the plan entirely.
// Everything else stays pushed and evaluated per row: on the streaming path
// the WAND threshold only skips blocks, it enforces nothing.
duckdb::TableFilterPushdown HandleScoreFilter(SereneDBScanBindData& bind,
                                              duckdb::TableFilter& filter) {
  auto& expr_filter =
    duckdb::ExpressionFilter::GetExpressionFilter(filter, "HandleScoreFilter");
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> conjuncts;
  if (expr_filter.expr->GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_CONJUNCTION &&
      expr_filter.expr->GetExpressionType() ==
        duckdb::ExpressionType::CONJUNCTION_AND) {
    conjuncts =
      std::move(expr_filter.expr->Cast<duckdb::BoundConjunctionExpression>()
                  .GetChildrenMutable());
  } else {
    conjuncts.push_back(std::move(expr_filter.expr));
  }
  const bool collector_enforces = bind.score_top_k.has_value();
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> residue;
  for (auto& conjunct : conjuncts) {
    if (duckdb::ExpressionFilter::GetOptionalDynamicFilterData(*conjunct)) {
      if (!collector_enforces) {
        residue.push_back(std::move(conjunct));
      }
      continue;
    }
    if (ScoreFloorApplies(bind)) {
      bool exact = false;
      const auto floor = StaticScoreFloor(*conjunct, exact);
      bind.score_static_floor = std::max(bind.score_static_floor, floor);
      if (exact && collector_enforces) {
        continue;
      }
    }
    residue.push_back(std::move(conjunct));
  }
  if (residue.empty()) {
    return duckdb::TableFilterPushdown::Drop;
  }
  if (residue.size() == 1) {
    expr_filter.expr = std::move(residue.front());
  } else {
    auto conj = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
      duckdb::ExpressionType::CONJUNCTION_AND);
    conj->GetChildrenMutable() = std::move(residue);
    expr_filter.expr = std::move(conj);
  }
  return duckdb::TableFilterPushdown::BeforeLimit;
}

// Per-filter pushdown decision (see TableFilterPushdown), finer than the
// per-column supports_pushdown_type. BeforeLimit: covered `.col` INCLUDE
// filters, and score-column filters (or what remains of them after
// HandleScoreFilter strips the collector-enforced conjuncts) -- applied
// in-scan before any limit/lookup, so a pushed top-k stays valid and the
// lookup only fetches survivors. Drop: a score filter the scan enforces
// entirely by itself (HandleScoreFilter), removed from the plan. AfterLimit:
// lookup-source filters (parquet/duckdb apply them during the source scan,
// after the doc-id selection) -- forces the scan unlimited. Reject: other
// virtuals, search-table columns, csv/json lookups -- stay a Filter node
// above the scan (which, being a LogicalFilter, also forces streaming, so
// top-k stays correct).
duckdb::TableFilterPushdown IResearchSupportsPushdownFilter(
  duckdb::FunctionData& bind_data_p, duckdb::idx_t col_idx,
  duckdb::TableFilter& filter) {
  auto& bind = bind_data_p.Cast<SereneDBScanBindData>();
  if (col_idx >= bind.column_ids.size()) {
    return duckdb::TableFilterPushdown::Reject;
  }
  const auto col_id = bind.column_ids[col_idx];
  if (col_id == catalog::Column::kInvertedIndexScoreId) {
    return HandleScoreFilter(bind, filter);
  }
  if (col_id.id() > catalog::Column::kMaxRealIdValue) {
    return duckdb::TableFilterPushdown::Reject;
  }
  if (bind.IsSearchTableEntry()) {
    return duckdb::TableFilterPushdown::BeforeLimit;
  }
  if (bind.IsInvertedIndexEntry() && bind.inverted_index) {
    const auto* info = bind.inverted_index->FindColumnInfo(col_id);
    if (info && info->IsStored()) {
      return duckdb::TableFilterPushdown::BeforeLimit;
    }
    if (bind.lookup_supports_filters) {
      // Vector (ANN) search is approximate: keep the pushed top-k (BeforeLimit)
      // so the collector runs the Top: path -- it over-fetches a pool by score,
      // this lookup filter is applied during materialization, and the TOP_N
      // above trims to the exact k. A text/exact scan keeps AfterLimit: the
      // lookup filter runs after doc selection, forcing the unlimited streaming
      // scan (a pushed top-k there could drop true matches).
      return bind.vector_scorer ? duckdb::TableFilterPushdown::BeforeLimit
                                : duckdb::TableFilterPushdown::AfterLimit;
    }
  }
  return duckdb::TableFilterPushdown::Reject;
}

// Accept single-column generic expressions (IS NULL, arithmetic, extracts,
// function predicates) as pushed ExpressionFilters, like the native table
// scan: the chain evaluates the whole expression against the decoded column.
// Only for covered `.col` columns (and the computed score) -- expressions on
// merely-indexed text columns, vector columns and lookup columns keep their
// Filter node above the scan, where the dedicated ts_dict / vector-radius /
// lookup handling already deals with them.
bool IResearchPushdownExpression(duckdb::ClientContext&,
                                 const duckdb::LogicalGet& get,
                                 duckdb::Expression& expr) {
  const auto& bind = get.bind_data->Cast<SereneDBScanBindData>();
  duckdb::vector<duckdb::ColumnBinding> bindings;
  duckdb::ColumnLifetimeAnalyzer::ExtractColumnBindings(expr, bindings);
  if (bindings.empty()) {
    return false;
  }
  const auto& column_ids = get.GetColumnIds();
  if (bindings[0].column_index >= column_ids.size()) {
    return false;
  }
  const auto col_idx = column_ids[bindings[0].column_index].GetPrimaryIndex();
  if (col_idx >= bind.column_ids.size()) {
    return false;
  }
  const auto col_id = bind.column_ids[col_idx];
  if (col_id == catalog::Column::kInvertedIndexScoreId) {
    return true;
  }
  if (col_id.id() > catalog::Column::kMaxRealIdValue) {
    return false;
  }
  if (bind.IsSearchTableEntry()) {
    return true;
  }
  if (bind.IsInvertedIndexEntry() && bind.inverted_index) {
    const auto* info = bind.inverted_index->FindColumnInfo(col_id);
    return info != nullptr && info->IsStored();
  }
  return false;
}

duckdb::unique_ptr<duckdb::BaseStatistics> IResearchScanStatistics(
  duckdb::ClientContext&, duckdb::TableFunctionGetStatisticsInput& input) {
  if (!input.bind_data) {
    return nullptr;
  }
  const auto& bind = input.bind_data->Cast<SereneDBScanBindData>();
  if (!input.column_index.HasPrimaryIndex()) {
    return nullptr;
  }
  const auto column_index = input.column_index.GetPrimaryIndex();
  if (column_index >= bind.column_ids.size()) {
    return nullptr;
  }
  const auto col_id = bind.column_ids[column_index];
  if (bind.IsInvertedIndexEntry() && bind.inverted_index) {
    const auto* info = bind.inverted_index->FindColumnInfo(col_id);
    if (!info || !info->store_values) {
      return nullptr;
    }
  } else if (bind.IsSearchTableEntry()) {
    if (col_id.id() > catalog::Column::kMaxRealIdValue) {
      return nullptr;
    }
  } else {
    return nullptr;
  }
  if (!bind.snapshot) {
    return nullptr;
  }
  const auto* stats = bind.snapshot->reader.GetColumnStats(col_id);
  if (stats == nullptr) {
    return nullptr;
  }
  if (!input.column_index.HasChildren()) {
    SDB_ASSERT(stats->GetType() == bind.column_types[column_index]);
    return stats->ToUnique();
  }
  const duckdb::BaseStatistics* leaf = stats;
  const duckdb::ColumnIndex* node = &input.column_index;
  if (bind.column_types[column_index].id() == duckdb::LogicalTypeId::VARIANT) {
    if (!duckdb::VariantStats::IsShredded(*stats)) {
      return nullptr;
    }
    leaf = &duckdb::VariantStats::GetShreddedStats(*stats);
    while (node->HasChildren()) {
      node = &node->GetChildIndex(0);
      if (node->HasPrimaryIndex()) {
        return nullptr;
      }
      const duckdb::VariantPathComponent comp{node->GetFieldName()};
      const auto child =
        duckdb::VariantShreddedStats::FindChildStats(*leaf, comp);
      if (!child) {
        return nullptr;
      }
      leaf = child.get();
    }
  } else {
    while (node->HasChildren()) {
      node = &node->GetChildIndex(0);
      if (!node->HasPrimaryIndex() ||
          leaf->GetType().id() != duckdb::LogicalTypeId::STRUCT) {
        return nullptr;
      }
      const auto field = node->GetPrimaryIndex();
      if (field >= duckdb::StructType::GetChildCount(leaf->GetType())) {
        return nullptr;
      }
      leaf = &duckdb::StructStats::GetChildStats(*leaf, field);
    }
  }
  if (leaf->GetType().IsNested() || !input.column_index.HasType()) {
    return nullptr;
  }
  const auto& want = input.column_index.GetScanType();
  if (leaf->GetType() == want) {
    return leaf->ToUnique();
  }
  if (leaf->GetType().IsNumeric() && want.IsNumeric() &&
      duckdb::NumericStats::HasMinMax(*leaf)) {
    duckdb::Value cmin;
    duckdb::Value cmax;
    if (duckdb::NumericStats::Min(*leaf).DefaultTryCastAs(want, cmin,
                                                          nullptr) &&
        duckdb::NumericStats::Max(*leaf).DefaultTryCastAs(want, cmax,
                                                          nullptr)) {
      auto casted = duckdb::NumericStats::CreateEmpty(want);
      duckdb::NumericStats::SetMin(casted, cmin);
      duckdb::NumericStats::SetMax(casted, cmax);
      return casted.ToUnique();
    }
  }
  return nullptr;
}

}  // namespace

duckdb::TableFunction CreateIResearchScanFunction() {
  duckdb::TableFunction func{
    "iresearch_scan",        {}, IResearchScanFunction, SereneDBScanBind,
    IResearchScanInitGlobal,
  };
  func.init_local = IResearchScanInitLocal;
  // TODO(mbkkt) Maybe we can use bind_replace/bind_operator to make indexes?
  // TODO: Better cardinality estimates
  func.cardinality = SereneDBScanCardinality;
  func.get_metrics = IResearchScanGetMetrics;
  func.to_string_value = SereneDBScanToValue;
  func.table_scan_progress = IResearchScanProgress;
  // TODO: Implement dynamic_to_string
  // TODO: Use get_partition_data for partition pruning of partitioned
  // tables/indexes
  func.get_bind_info = SereneDBGetBindInfo;
  // TODO: Is get_multi_file_reader helpful for us? Will it allow us
  // faster/simpler implementation of multi-threaded scanning?
  // TODO: Implement get_partition_info and get_partition_stats for partitioned
  // tables/indexes
  func.get_virtual_columns = SereneDBScanGetVirtualColumns;
  func.get_row_id_columns = SereneDBScanGetRowIdColumns;
  func.pushdown_complex_filter = &optimizer::IResearchPushdownComplexFilter;
  func.pushdown_expression = &IResearchPushdownExpression;
  func.set_scan_order = &IResearchSetScanOrder;
  func.supports_pushdown_extract = &IResearchSupportsPushdownExtract;
  func.supports_pushdown_filter = &IResearchSupportsPushdownFilter;
  func.statistics_extended = &IResearchScanStatistics;
  func.verify_serialization = false;
  func.projection_pushdown = true;
  func.filter_pushdown = true;
  func.filter_prune = true;
  // TODO: Use late_materialization instead of our materialization approach for
  // indexes/primary keys
  // TODO: Better order preservation types for different scan strategies, e.g.,
  // PK scans preserve insertion order, SK scans don't guarantee any order, but
  // could be made to preserve index order if we implement set_scan_order
  func.order_preservation_type = duckdb::OrderPreservationType::NO_ORDER;
  return func;
}

}  // namespace sdb::connector
