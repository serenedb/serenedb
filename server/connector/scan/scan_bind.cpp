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

#include "connector/scan/scan_bind.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/vector_radius_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "connector/scan/scan_function.h"
#include "pg/connection_context.h"
#include "search/search_table.h"
#include "search/search_table_transaction.h"

namespace sdb::connector {
namespace {

uint64_t EstimateFilterMatchCount(const irs::Filter& filter,
                                  uint64_t live_docs) {
  const auto type = filter.type();
  if (type == irs::Type<irs::All>::id()) {
    return live_docs;
  }
  if (type == irs::Type<irs::Empty>::id()) {
    return 0;
  }
  constexpr double kDefaultFilterSelectivity = 0.2;
  return std::max<uint64_t>(live_docs * kDefaultFilterSelectivity, 1U);
}

duckdb::unique_ptr<duckdb::NodeStatistics> TsDictEstimation(
  const ScanBindData& bind) {
  uint64_t estimate = 0;
  for (const auto& req : bind.ts_dict.requests) {
    const uint64_t rows = [&] -> uint64_t {
      if (req.term_uses == (TsDictTermUses::Min | TsDictTermUses::Max)) {
        return 2;
      }
      if (req.term_uses == TsDictTermUses::Min ||
          req.term_uses == TsDictTermUses::Max) {
        return 1;
      }
      uint64_t terms = 0;
      for (const auto& segment : bind.search.snapshot->reader) {
        if (const auto* field = segment.field(req.field_id)) {
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

const duckdb::ColumnDefinition* FindColumnById(
  const duckdb::TableCatalogEntry& entry, ColumnId col_id) {
  for (const auto& column : entry.GetColumns().Logical()) {
    if (ColumnId{column.Oid()} == col_id) {
      return &column;
    }
  }
  return nullptr;
}

irs::DirectoryReader PinnedSearchReader(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& table) {
  const auto& store = table.Storage();
  auto* conn_ctx = GetSereneDBContextPtr(context);
  if (!conn_ctx) {
    return store->GetDirectoryReader();
  }
  return irs::DirectoryReader{*conn_ctx->SearchTxn().EnsureSearchTableReader(
    table.oid, [&] { return store->GetDirectoryReader(); })};
}

duckdb::unique_ptr<ScanBindData> MakeTableScanBindData(
  duckdb::TableCatalogEntry& table, ScanEntryKind kind,
  std::string lookup_label, search::InvertedIndexSnapshotPtr snapshot) {
  auto data = duckdb::make_uniq<ScanBindData>();
  data->relation.table_entry = &table;
  data->relation.kind = kind;
  data->lookup.label = std::move(lookup_label);
  data->search.snapshot = std::move(snapshot);
  for (const auto& column : table.GetColumns().Logical()) {
    data->columns.ids.push_back(ColumnId{column.Oid()});
    data->columns.types.push_back(column.Type());
  }
  return data;
}

duckdb::unique_ptr<ScanBindData> MakeViewScanBindData(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view,
  const duckdb::case_insensitive_map_t<duckdb::Value>& index_options,
  search::InvertedIndexSnapshotPtr snapshot) {
  auto view_info = view.GetInfo();
  const auto& view_base = view_info->Cast<duckdb::CreateViewInfo>();
  auto data = duckdb::make_uniq<ScanBindData>();
  auto& spec = data->view.emplace();
  spec.id = view.oid;
  spec.name = view.name.GetIdentifierName();
  data->relation.kind = ScanEntryKind::InvertedIndex;
  data->search.snapshot = std::move(snapshot);
  spec.fast_path = ResolveViewFastPath(
    context, view_base, catalog::ParseKeyColumns(index_options));
  data->lookup.supports_filters = false;
  if (spec.fast_path) {
    data->lookup.label = FormatLookupLabel(*spec.fast_path);
    data->lookup.supports_filters = spec.fast_path->supports_filters;
  } else {
    data->lookup.label = "view";
  }
  for (duckdb::idx_t i = 0; i < view_base.names.size(); ++i) {
    data->columns.ids.push_back(ColumnId{i});
    data->columns.types.push_back(view_base.types[i]);
    spec.column_names.push_back(view_base.names[i].GetIdentifierName());
  }
  return data;
}

}  // namespace

TsDictRequest& TsDictSpec::For(irs::field_id field_id) {
  const auto it = absl::c_find_if(
    requests,
    [field_id](const TsDictRequest& req) { return req.field_id == field_id; });
  if (it != requests.end()) {
    return *it;
  }
  return requests.emplace_back(
    TsDictRequest{.field_id = field_id, .display_id = field_id});
}

bool ScanBindData::Equals(const duckdb::FunctionData& other) const {
  const auto& o = other.Cast<ScanBindData>();
  if (view.has_value() != o.view.has_value()) {
    return false;
  }
  if (columns.ids != o.columns.ids) {
    return false;
  }
  if (view) {
    return view->id == o.view->id;
  }
  return relation.table_entry.get() == o.relation.table_entry.get();
}

std::string_view ScanBindData::ColumnNameById(ColumnId col_id) const {
  if (view) {
    const auto idx = static_cast<size_t>(col_id);
    const auto& names = view->column_names;
    return idx < names.size() ? std::string_view{names[idx]}
                              : std::string_view{};
  }
  const auto* column = FindColumnById(*relation.table_entry, col_id);
  return column ? column->Name().GetIdentifierName() : std::string_view{};
}

duckdb::LogicalType ScanBindData::ColumnTypeById(ColumnId col_id) const {
  if (view) {
    const auto idx = static_cast<size_t>(col_id);
    return idx < columns.types.size() ? columns.types[idx]
                                      : duckdb::LogicalType::INVALID;
  }
  const auto* column = FindColumnById(*relation.table_entry, col_id);
  return column ? column->Type() : duckdb::LogicalType::INVALID;
}

std::string ScanBindData::DisplayColumnName(ColumnId col_id) const {
  auto name = ColumnNameById(col_id);
  if (name.empty()) {
    name = ColumnNameById(relation.inverted_config->ColumnOf(col_id));
  }
  if (!name.empty()) {
    return std::string{name};
  }
  if (auto expr = relation.inverted_config->ExpressionText(col_id);
      !expr.empty()) {
    return expr;
  }
  return absl::StrCat("col", col_id);
}

bool ScanBindData::IsColumnNotNull(ColumnId col_id) const {
  if (view) {
    return false;
  }
  const auto* column = FindColumnById(*relation.table_entry, col_id);
  if (!column) {
    return false;
  }
  const auto index = column->Logical();
  for (const auto& constraint : relation.table_entry->GetConstraints()) {
    if (constraint->type == duckdb::ConstraintType::NOT_NULL &&
        constraint->Cast<duckdb::NotNullConstraint>().index == index) {
      return true;
    }
  }
  return false;
}

void ScanBindData::IterateColumns(const ColumnVisitor& cb) const {
  if (view) {
    const auto& names = view->column_names;
    for (size_t i = 0; i < names.size(); ++i) {
      cb(static_cast<ColumnId>(i), columns.types[i]);
    }
    return;
  }
  for (const auto& column : relation.table_entry->GetColumns().Logical()) {
    cb(ColumnId{column.Oid()}, column.Type());
  }
}

bool ScanBindData::IsHnswScored() const noexcept {
  if (!score.vector) {
    return false;
  }
  const auto info =
    relation.inverted_config->GetColumnOptions(score.vector->field_id).ann_info;
  return info && info->kind == irs::AnnKind::Hnsw;
}

std::string_view ScanBindData::RelationName() const {
  if (relation.IsInvertedIndex()) {
    return relation.inverted_index->name.GetIdentifierName();
  }
  return view ? std::string_view{view->name}
              : relation.table_entry->name.GetIdentifierName();
}

duckdb::unique_ptr<duckdb::NodeStatistics> ScanBindData::Cardinality(
  duckdb::ClientContext&) const {
  if (ts_dict.Active()) {
    return TsDictEstimation(*this);
  }
  const auto live = search.snapshot->reader.live_docs_count();
  const auto* filter = search.filter.get();
  const auto estimate = filter ? EstimateFilterMatchCount(*filter, live) : live;
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate, live);
}

duckdb::unique_ptr<duckdb::FunctionData> ScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  const duckdb::QualifiedName qualified{
    duckdb::Identifier{input.inputs[0].GetValue<std::string>()},
    duckdb::Identifier{input.inputs[1].GetValue<std::string>()},
    duckdb::Identifier{input.inputs[2].GetValue<std::string>()}};
  auto index = duckdb::Catalog::GetEntry<duckdb::IndexCatalogEntry>(
    context, qualified, duckdb::OnEntryNotFound::RETURN_NULL);
  if (!index || !IsInvertedIndex(*index)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", qualified.Name().GetIdentifierName(),
                            "\" does not exist"));
  }
  const auto& entry = irs::utils::downCast<catalog::InvertedIndexEntry>(*index);
  auto& relation = duckdb::Catalog::GetEntry(
    context,
    duckdb::EntryLookupInfo{
      duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName{qualified.Catalog(), index->GetSchemaName(),
                            index->GetTableName()}});
  const auto* search_table =
    dynamic_cast<const catalog::SearchTableEntry*>(&relation);
  search::InvertedIndexSnapshotPtr snapshot;
  if (search_table) {
    snapshot = std::make_shared<search::InvertedIndexSnapshot>(
      PinnedSearchReader(context, *search_table), nullptr);
  } else {
    snapshot = GetSereneDBContext(context).EnsureSearchSnapshot(
      entry.oid, entry.Storage());
  }
  auto data =
    relation.type == duckdb::CatalogType::VIEW_ENTRY
      ? MakeViewScanBindData(context, relation.Cast<duckdb::ViewCatalogEntry>(),
                             entry.options, std::move(snapshot))
      : MakeTableScanBindData(relation.Cast<duckdb::TableCatalogEntry>(),
                              search_table ? ScanEntryKind::SearchTableIndex
                                           : ScanEntryKind::InvertedIndex,
                              search_table ? "search" : "table",
                              std::move(snapshot));
  data->relation.inverted_index = &entry;
  data->relation.inverted_config = entry.Config();
  data->relation.row_group_size =
    search_table ? search_table->Storage()->Config()->row_group_size
                 : entry.Config()->row_group_size;
  data->score.prune = search_table ? search_table->Storage()->TopKScorer()
                                   : entry.Config()->top_k_scorer;
  data->IterateColumns([&](ColumnId id, const duckdb::LogicalType& type) {
    return_types.push_back(type);
    names.push_back(std::string{data->ColumnNameById(id)});
  });
  return data;
}

duckdb::TableFunction BindSearchTableScan(
  duckdb::ClientContext& context, catalog::SearchTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  const auto& store = entry.Storage();
  auto data =
    MakeTableScanBindData(entry, ScanEntryKind::SearchTable, "search",
                          std::make_shared<search::InvertedIndexSnapshot>(
                            PinnedSearchReader(context, entry), nullptr));
  data->score.prune = store->TopKScorer();
  data->relation.inverted_config = store->Config();
  data->relation.row_group_size =
    data->relation.inverted_config->row_group_size;
  bind_data = std::move(data);
  return CreateIResearchScanFunction();
}

std::optional<duckdb::LogicalType> GeneratedPkTypeOf(const ScanBindData& bind) {
  if (bind.relation.IsSearchTable()) {
    return duckdb::LogicalType::ROW_TYPE;
  }
  return std::nullopt;
}

std::optional<PkSpec> ViewPkSpecOf(const ScanBindData& bind) {
  if (bind.view &&
      bind.relation.inverted_config->pk.column == PkColumnKind::Has) {
    if (const auto& fp = bind.view->fast_path) {
      return fp->pk_spec;
    }
  }
  return std::nullopt;
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
    o->quant = vs.quant;
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
  o->max_search_fanout = vs.max_search_fanout;
  o->ef_search = vs.ef_search;
  o->min_ef = vs.min_ef;
  o->inner = std::move(inner);
  return f;
}

}  // namespace sdb::connector
