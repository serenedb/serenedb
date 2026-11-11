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

#include <cmath>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/index/segment_reader.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/boolean_rules.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/filters/vector_exact_filter.hpp>
#include <iresearch/search/filters/vector_radius_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ranges>

#include "catalog/base_catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/scan/deferred_check.h"
#include "connector/scan/scan_function.h"
#include "connector/search_filter_builder.hpp"
#include "pg/connection_context.h"
#include "query/config.h"
#include "search/scorer_options.h"
#include "search/store.h"
#include "search/writes.h"

namespace sdb::connector {
namespace {

using FieldSet = irs::containers::FlatHashSet<irs::field_id>;

FieldSet UniqueTermFields(const ScanBindData& bind) {
  FieldSet fields;
  if (!bind.relation.IsSearchTable() || !bind.relation.inverted_config) {
    return fields;
  }
  const auto& config = bind.relation.ScannedIndex();
  for (const auto& spec :
       bind.relation.table_entry->Cast<catalog::SearchTableEntry>()
         .KeySpecs()) {
    if (spec.kind != search::KeyKind::Unique || spec.key.columns.size() != 1 ||
        (!spec.key.expressions.empty() &&
         !spec.key.expressions.front().empty())) {
      continue;
    }
    fields.insert(spec.key.field);
    const auto column = spec.key.columns.front();
    const auto type = bind.ColumnTypeById(column).id();
    const auto term = config.TermField(column);
    const auto* entry = config.LookupField(term).entry;
    if (entry && entry->IsTermDict() && !entry->HasJsonLeafFields() &&
        (!entry->HasTextDictionary() || entry->is_keyword) &&
        type != duckdb::LogicalTypeId::LIST &&
        type != duckdb::LogicalTypeId::ARRAY) {
      fields.insert(term);
    }
  }
  return fields;
}

std::optional<uint64_t> UniqueMatches(const irs::Filter& filter,
                                      const FieldSet& unique) {
  if (filter.type() == irs::Type<irs::ByTerm>::id()) {
    if (unique.contains(irs::utils::downCast<irs::ByTerm>(filter).field_id())) {
      return 1;
    }
    return std::nullopt;
  }
  if (filter.type() != irs::Type<irs::BooleanFilter>::id()) {
    return std::nullopt;
  }
  const auto& node = irs::utils::downCast<irs::BooleanFilter>(filter);
  std::optional<uint64_t> bound;
  const auto tighten = [&](uint64_t rows) {
    bound = bound ? std::min(*bound, rows) : rows;
  };
  for (const auto& term : node.Terms(irs::Occur::Must)) {
    if (unique.contains(term.field)) {
      tighten(1);
    }
  }
  for (const auto& child : node.Filters(irs::Occur::Must)) {
    if (const auto rows = UniqueMatches(*child, unique)) {
      tighten(*rows);
    }
  }
  if (bound || node.Size(irs::Occur::Must) != 0 ||
      node.Size(irs::Occur::Should) == 0 || node.MinShouldMatch() == 0) {
    return bound;
  }
  uint64_t rows = 0;
  for (const auto& term : node.Terms(irs::Occur::Should)) {
    if (!unique.contains(term.field)) {
      return std::nullopt;
    }
    ++rows;
  }
  for (const auto& child : node.Filters(irs::Occur::Should)) {
    const auto child_rows = UniqueMatches(*child, unique);
    if (!child_rows) {
      return std::nullopt;
    }
    rows += *child_rows;
  }
  return rows;
}

uint64_t EstimateFilterMatchCount(const irs::Filter& filter, uint64_t live_docs,
                                  const FieldSet& unique) {
  const auto type = filter.type();
  if (type == irs::Type<irs::All>::id()) {
    return live_docs;
  }
  if (type == irs::Type<irs::Empty>::id()) {
    return 0;
  }
  if (const auto rows = UniqueMatches(filter, unique)) {
    return std::max<uint64_t>(std::min(*rows, live_docs), 1U);
  }
  constexpr double kDefaultFilterSelectivity = 0.2;
  return std::max<uint64_t>(
    static_cast<uint64_t>(live_docs * kDefaultFilterSelectivity), 1U);
}

duckdb::unique_ptr<duckdb::NodeStatistics> TsDictEstimation(
  duckdb::ClientContext& context, const ScanBindData& bind) {
  const auto& reader = *bind.Read(context).reader;
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
      for (const auto& segment : reader) {
        if (const auto* field = segment.field(
              req.field_id,
              {.layout = req.layout, .member = irs::DictMember::Strings})) {
          terms += field->size();
        }
      }
      return std::max<uint64_t>(terms, 1);
    }();
    estimate += req.having_filter
                  ? EstimateFilterMatchCount(*req.having_filter, rows, {})
                  : rows;
  }
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate);
}

const duckdb::ColumnDefinition* FindColumnById(
  const duckdb::TableCatalogEntry& entry, ColumnId col_id) {
  for (const auto& column : entry.GetColumns().Logical()) {
    if (TableColumnId(column) == col_id) {
      return &column;
    }
  }
  return nullptr;
}

duckdb::unique_ptr<ScanBindData> MakeTableScanBindData(
  duckdb::TableCatalogEntry& table, ScanEntryKind kind,
  std::string lookup_label) {
  auto data = duckdb::make_uniq<ScanBindData>();
  data->relation.table_entry = &table;
  data->relation.kind = kind;
  data->lookup.label = std::move(lookup_label);
  for (const auto& column : table.GetColumns().Logical()) {
    data->columns.ids.emplace_back(TableColumnId(column));
    data->columns.types.emplace_back(column.Type());
  }
  return data;
}

duckdb::unique_ptr<ScanBindData> MakeViewScanBindData(
  duckdb::ClientContext& context, duckdb::ViewCatalogEntry& view,
  const duckdb::case_insensitive_map_t<duckdb::Value>& index_options) {
  auto view_info = view.GetInfo();
  const auto& view_base = view_info->Cast<duckdb::CreateViewInfo>();
  auto data = duckdb::make_uniq<ScanBindData>();
  auto& spec = data->view.emplace();
  spec.id = view.oid;
  spec.name = view.name.GetIdentifierName();
  data->relation.kind = ScanEntryKind::InvertedIndex;
  spec.fast_path = ResolveViewFastPath(context, view.ParentCatalog(), view_base,
                                       catalog::ParseKeyColumns(index_options));
  data->lookup.supports_filters = false;
  if (spec.fast_path) {
    data->lookup.label = FormatLookupLabel(*spec.fast_path);
    data->lookup.supports_filters = spec.fast_path->supports_filters;
  } else {
    data->lookup.label = "view";
  }
  for (duckdb::idx_t i = 0; i < view_base.names.size(); ++i) {
    data->columns.ids.emplace_back(i);
    data->columns.types.emplace_back(view_base.types[i]);
    spec.column_names.emplace_back(view_base.names[i].GetIdentifierName());
  }
  return data;
}

float AutoRescoreWidth(irs::VectorQuantization quant, uint32_t nb_bits,
                       irs::AnnKind kind) noexcept {
  const bool ivf = kind == irs::AnnKind::Ivf;
  switch (quant) {
    case irs::VectorQuantization::None:
    case irs::VectorQuantization::FP16:
    case irs::VectorQuantization::BF16:
      return 0.f;
    case irs::VectorQuantization::SQ8:
      return 2.f;
    case irs::VectorQuantization::RaBitQ:
      return nb_bits > 1 ? 3.f : 2.f;
    case irs::VectorQuantization::OSQ:
      if (nb_bits > 2) {
        return 7.f;
      }
      return ivf ? 3.f : 2.f;
    case irs::VectorQuantization::TQ:
      return 3.f;
    case irs::VectorQuantization::BQ:
      return ivf ? 1.f : 3.f;
    case irs::VectorQuantization::USQ8:
      return 4.f;
    case irs::VectorQuantization::PQ:
      return ivf ? 2.f : 3.f;
    case irs::VectorQuantization::SQ4:
      return 5.f;
    case irs::VectorQuantization::USQ4:
      return 6.f;
  }
  return 0.f;
}

float AnnRescoreWidth(duckdb::ClientContext& context,
                      const VectorScorerOptions& vs) {
  if (vs.quant == irs::VectorQuantization::None) {
    return 0.f;
  }
  const auto z = GetDoubleSetting(context, "sdb_ann_rescore_width");
  return z < 0.0 ? AutoRescoreWidth(vs.quant, vs.nb_bits, vs.kind)
                 : static_cast<float>(z);
}

template<typename AnnFilter>
std::unique_ptr<AnnFilter> MakeAnnFilter(
  const VectorScorerOptions& vs, std::shared_ptr<const irs::Filter> inner) {
  auto f = std::make_unique<AnnFilter>();
  *f->mutable_field_id() = vs.field_id;
  auto* o = f->mutable_options();
  o->query = vs.query_vector;
  o->column_id = vs.field_id;
  o->ann_id = vs.ann_id;
  o->metric = vs.metric;
  o->quant = vs.quant;
  o->inner = std::move(inner);
  return f;
}

duckdb::unique_ptr<ScanBindData> BindIndexScan(
  duckdb::ClientContext& context, const catalog::InvertedIndexEntry& entry) {
  auto host = entry.GetRelation(entry.catalog.GetCatalogTransaction(context));
  if (!host) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG("relation \"", entry.GetTableName().GetIdentifierName(),
              "\" does not exist"));
  }
  auto& relation = *host;
  const auto* search_table =
    dynamic_cast<const catalog::SearchTableEntry*>(&relation);
  auto data =
    relation.type == duckdb::CatalogType::VIEW_ENTRY
      ? MakeViewScanBindData(context, relation.Cast<duckdb::ViewCatalogEntry>(),
                             entry.options)
      : MakeTableScanBindData(relation.Cast<duckdb::TableCatalogEntry>(),
                              search_table ? ScanEntryKind::SearchTableIndex
                                           : ScanEntryKind::InvertedIndex,
                              search_table ? "search" : "table");
  data->relation.inverted_index = &entry;
  data->relation.inverted_config = entry.Config();
  if (search_table) {
    const auto config =
      ConnectionContext::Get(context).Transaction().ScanConfig(
        context, *search_table->Storage());
    if (!absl::c_all_of(entry.Config()->fields, [&](const auto& field) {
          return config->fields.contains(field.first);
        })) {
      search::ThrowConcurrentUpdate();
    }
  }
  data->relation.row_group_size =
    search_table ? search_table->Storage()->Config()->row_group_size
                 : entry.Config()->row_group_size;
  data->score.prune = search_table ? search_table->Storage()->TopKScorer()
                                   : entry.Config()->top_k_scorer;
  return data;
}

duckdb::unique_ptr<ScanBindData> BindSearchTable(
  duckdb::ClientContext& context, catalog::SearchTableEntry& entry) {
  auto data =
    MakeTableScanBindData(entry, ScanEntryKind::SearchTable, "search");
  data->score.prune = entry.Storage()->TopKScorer();
  data->relation.inverted_config =
    ConnectionContext::Get(context).Transaction().ScanConfig(context,
                                                             *entry.Storage());
  data->relation.row_group_size =
    data->relation.inverted_config->row_group_size;
  return data;
}

}  // namespace

double SegmentBeamShare(double k, size_t segments) noexcept {
  if (segments <= 1) {
    return k;
  }
  const double n = static_cast<double>(segments);
  const double mean = k / n;
  const double sd = std::sqrt(mean * (1.0 - 1.0 / n));
  return std::min(k, std::max(16.0, std::ceil(mean + 3.0 * sd)));
}

uint32_t ClampCandidates(double pool, double k, uint64_t live) noexcept {
  return static_cast<uint32_t>(
    std::min({pool, std::max(k, static_cast<double>(live)),
              static_cast<double>(std::numeric_limits<uint32_t>::max())}));
}

void RefreshVectorKnobs(VectorScorerOptions& vs,
                        duckdb::ClientContext& context) {
  const auto count = [&](std::string_view name) {
    const auto n = GetSignedIntSetting(context, name);
    return n < 0 ? 0 : static_cast<uint32_t>(n);
  };
  const auto mode = [&]<typename E>(std::string_view name, E fallback) {
    static constexpr auto kNames = magic_enum::enum_names<E>();
    const auto i = GetEnumSetting(context, name, kNames);
    return i < kNames.size() ? static_cast<E>(i) : fallback;
  };
  vs.nprobe = count("sdb_ivf_search_nprobe");
  vs.min_search_fanout = count("sdb_ivf_min_search_fanout");
  vs.max_search_fanout = count("sdb_ivf_max_search_fanout");
  vs.ef_search = count("sdb_hnsw_ef_search");
  vs.hnsw_filter_mode = mode("sdb_hnsw_filter_mode", irs::HnswFilterMode::Auto);
  vs.hnsw_column_filter =
    mode("sdb_hnsw_column_filter", irs::HnswColumnFilter::Auto);
  vs.exact = GetBoolSetting(context, "sdb_ann_force_exact");
  vs.rescore_width = AnnRescoreWidth(context, vs);
}

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

bool ScanBindData::HasPushdowns() const noexcept {
  return search.filter || search.index_only || !search.duals.empty() ||
         !search.deferred.empty() || !search.filter_scorers.empty() ||
         !search.row_filters.empty() || score.text || score.vector ||
         score.HasTopK() || score.top_offset_expr || score.top_offset != 0 ||
         score.top_n_consumed || score.order ||
         score.static_floor != std::numeric_limits<float>::lowest() ||
         scan_order || offsets.Active() || ts_dict.Active() ||
         plan_cache.deferred;
}

bool ScanBindData::ReadsParametersAtExecution() const {
  return !plan_cache.declined_parameter && !ts_dict.Active() &&
         !offsets.Active();
}

search::ReadView ScanBindData::Read(duckdb::ClientContext& context) const {
  auto& transaction = ConnectionContext::Get(context).Transaction();
  if (relation.kind == ScanEntryKind::InvertedIndex) {
    return transaction.SearchTxn().Read(context,
                                        *relation.inverted_index->Storage());
  }
  return transaction.SearchTxn().Read(
    context,
    *relation.table_entry->Cast<catalog::SearchTableEntry>().Storage());
}

duckdb::idx_t ScanColumnOfStorage(const ScanBindData& bind,
                                  duckdb::idx_t storage_index) {
  const auto table = bind.relation.table_entry;
  if (!table || storage_index >= table->GetColumns().PhysicalColumnCount()) {
    return storage_index;
  }
  return table->GetColumns()
    .PhysicalToLogical(duckdb::PhysicalIndex{storage_index})
    .index;
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
  if (!column && relation.inverted_config) {
    column = FindColumnById(*relation.table_entry,
                            relation.inverted_config->ColumnOf(col_id));
  }
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
    cb(TableColumnId(column), column.Type());
  }
}

std::string_view ScanBindData::RelationName() const {
  if (relation.IsInvertedIndex()) {
    return relation.inverted_index->name.GetIdentifierName();
  }
  return view ? std::string_view{view->name}
              : relation.table_entry->name.GetIdentifierName();
}

duckdb::unique_ptr<duckdb::NodeStatistics> ScanBindData::Cardinality(
  duckdb::ClientContext& context) const {
  if (ts_dict.Active()) {
    return TsDictEstimation(context, *this);
  }
  const auto live = Read(context).reader->live_docs_count();
  const auto* filter = search.filter.get();
  const auto estimate =
    filter ? EstimateFilterMatchCount(*filter, live, UniqueTermFields(*this))
           : live;
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate, live);
}

duckdb::unique_ptr<duckdb::FunctionData> ScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  SDB_IF_FAILURE("connector::ScanBind") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  duckdb::vector<duckdb::Identifier> path;
  for (const auto& part : duckdb::ListValue::GetChildren(input.inputs[0])) {
    path.emplace_back(part.GetValue<std::string>());
  }
  const auto qualified = duckdb::QualifiedName::FromPath(std::move(path));
  auto index = duckdb::Catalog::GetEntry<duckdb::IndexCatalogEntry>(
    context, qualified, duckdb::OnEntryNotFound::RETURN_NULL);
  if (!index || !catalog::IsInvertedIndex(*index)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", qualified.Name().GetIdentifierName(),
                            "\" does not exist"));
  }
  if (input.binder) {
    auto& properties = input.binder->GetStatementProperties();
    properties.RegisterDBRead(index->ParentCatalog(), context);
    properties.RegisterDataRead(*index, context);
  }
  auto data = BindIndexScan(
    context, irs::utils::downCast<catalog::InvertedIndexEntry>(*index));
  data->IterateColumns([&](ColumnId id, const duckdb::LogicalType& type) {
    return_types.emplace_back(type);
    names.emplace_back(data->ColumnNameById(id));
  });
  return data;
}

duckdb::unique_ptr<ScanBindData> RebindScan(duckdb::ClientContext& context,
                                            const duckdb::Identifier& catalog,
                                            duckdb::idx_t oid,
                                            ScanEntryKind kind) {
  auto& base =
    duckdb::Catalog::GetCatalog(context, catalog).Cast<catalog::BaseCatalog>();
  auto entry = base.LookupOid(base.GetCatalogTransaction(context), oid);
  if (!entry) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation with OID ", oid, " does not exist"));
  }
  if (kind == ScanEntryKind::SearchTable) {
    return BindSearchTable(context, entry->Cast<catalog::SearchTableEntry>());
  }
  return BindIndexScan(
    context, irs::utils::downCast<catalog::InvertedIndexEntry>(*entry));
}

duckdb::TableFunction BindSearchTableScan(
  duckdb::ClientContext& context, catalog::SearchTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {
  SDB_IF_FAILURE("connector::ScanBind") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  bind_data = BindSearchTable(context, entry);
  return CreateIResearchScanFunction();
}

bool LooksUpByPk(const ScanBindData& bind) {
  return bind.relation.kind == ScanEntryKind::InvertedIndex &&
         bind.relation.ScannedIndex().pk == catalog::PkColumnKind::Has &&
         (!bind.view || bind.view->fast_path);
}

std::optional<duckdb::LogicalType> GeneratedPkTypeOf(const ScanBindData& bind) {
  if (!LooksUpByPk(bind)) {
    return std::nullopt;
  }
  if (bind.view) {
    return bind.view->fast_path->GeneratedPkType();
  }
  return duckdb::LogicalType::ROW_TYPE;
}

std::optional<PkSpec> ViewPkSpecOf(const ScanBindData& bind) {
  if (bind.view &&
      bind.relation.inverted_config->pk == catalog::PkColumnKind::Has) {
    if (const auto& fp = bind.view->fast_path) {
      return fp->pk_spec;
    }
  }
  return std::nullopt;
}

bool RowsOnlySegment(const irs::SubReader& segment) {
  return segment.field_ids().empty();
}

std::optional<uint64_t> UniqueMatchBound(const ScanBindData& scan,
                                         const irs::Filter& filter) {
  return UniqueMatches(filter, UniqueTermFields(scan));
}

DeferredBuild BuildDeferredFilter(duckdb::ClientContext& context,
                                  const ScanBindData& scan,
                                  const irs::DirectoryReader& reader) {
  SDB_ASSERT(scan.plan_cache.deferred);
  const auto& claim = *scan.plan_cache.deferred;
  const auto bound = [&](const std::shared_ptr<const duckdb::Expression>& e) {
    return optimizer::NormalizeClaimShape(
      context,
      optimizer::SubstituteParameters(e->Copy(), /*with_values=*/true));
  };
  DeferredBuild out;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> conjuncts;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> index_only;
  conjuncts.reserve(claim.conjuncts.size() + claim.ranges.size());
  const bool keeps_columns =
    scan.KeepsColumnForms() || scan.ReadsRowsOnlySegments();
  const auto keep_column_form = [&](const duckdb::Expression& conjunct) {
    auto form = optimizer::RowFormOf(conjunct);
    if (!form) {
      return false;
    }
    DeferredRowFilter row;
    for (const auto* ref : form->columns) {
      const auto it = claim.columns->find(MakeBindingKey(ref->Binding()));
      if (it == claim.columns->end() || !it->second.column_stored) {
        return false;
      }
      row.columns.push_back(it->second.column);
      row.types.push_back(ref->GetReturnType());
    }
    row.filter =
      duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(form->expr));
    out.dual_filters.push_back(std::move(row));
    return true;
  };
  const auto to_index = [&](duckdb::unique_ptr<duckdb::Expression> conjunct) {
    if (keeps_columns && !keep_column_form(*conjunct)) {
      index_only.push_back(conjunct->Copy());
    }
    conjuncts.push_back(std::move(conjunct));
  };
  for (const auto& e : claim.conjuncts) {
    to_index(bound(e));
  }
  const ColumnGetter getter = [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<SearchColumnInfo> {
    const auto it = claim.columns->find(MakeBindingKey(ref.Binding()));
    if (it == claim.columns->end()) {
      return std::nullopt;
    }
    auto info = optimizer::ResolveSearchColumnById(
      context, scan, it->second.column, it->second.column_stored);
    if (info && info->column_stored) {
      info->stats = reader.GetColumnStats(it->second.column);
    }
    return info;
  };
  const ExpressionGetter expr_getter =
    [](const duckdb::Expression&) -> std::optional<SearchColumnInfo> {
    return std::nullopt;
  };
  struct WideColumn {
    duckdb::LogicalType type;
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> parts;
  };
  std::map<ColumnId, WideColumn> wide;
  const auto to_column = [&](duckdb::unique_ptr<duckdb::Expression> range) {
    const duckdb::BoundColumnRefExpression* ref = nullptr;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundColumnRefExpression>(
      *range, [&](const duckdb::BoundColumnRefExpression& r) { ref = &r; });
    SDB_ENSURE(ref, "a deferred range reads one column");
    const auto it = claim.columns->find(MakeBindingKey(ref->Binding()));
    SDB_ENSURE(it != claim.columns->end(),
               "a deferred range's column was resolved at plan time");
    auto& [type, parts] = wide[it->second.column];
    type = ref->GetReturnType();
    if (range->GetExpressionType() == duckdb::ExpressionType::COMPARE_BETWEEN) {
      using duckdb::BoundBetweenExpression;
      const auto& between = range->Cast<duckdb::BoundFunctionExpression>();
      parts.push_back(duckdb::BoundComparisonExpression::Create(
        BoundBetweenExpression::LowerComparisonType(between),
        BoundBetweenExpression::Input(between).Copy(),
        BoundBetweenExpression::LowerBound(between).Copy()));
      parts.push_back(duckdb::BoundComparisonExpression::Create(
        BoundBetweenExpression::UpperComparisonType(between),
        BoundBetweenExpression::Input(between).Copy(),
        BoundBetweenExpression::UpperBound(between).Copy()));
    } else {
      parts.push_back(std::move(range));
    }
  };
  const auto probe_wide =
    [&](const duckdb::unique_ptr<duckdb::Expression>& range) {
      auto probe = std::make_unique<irs::BooleanFilter>();
      std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&range, 1};
      FilterScorers probe_scorers;
      return !MakeSearchFilter(*probe, single, getter, context, expr_getter,
                               &probe_scorers, WideRanges::DeclineWide)
                .ok();
    };
  ColumnIntervals<duckdb::unique_ptr<duckdb::Expression>> intervals;
  for (const auto& e : claim.ranges) {
    auto range = bound(e);
    const auto bounds = ColumnRangeOf(*range);
    if (!bounds) {
      if (probe_wide(range)) {
        to_column(std::move(range));
      } else {
        to_index(std::move(range));
      }
      continue;
    }
    AddColumnRange(intervals, *bounds, getter, std::move(range));
  }
  for (auto& [key, interval] : intervals) {
    const auto fits = IntervalFitsIndex(interval);
    for (auto& range : interval.members) {
      if (fits ? !*fits : probe_wide(range)) {
        to_column(std::move(range));
      } else {
        to_index(std::move(range));
      }
    }
  }
  for (auto& [column, typed] : wide) {
    auto& [type, parts] = typed;
    for (auto& part : parts) {
      part = optimizer::ColumnFormOf(std::move(part));
    }
    duckdb::unique_ptr<duckdb::Expression> expr;
    if (parts.size() == 1) {
      expr = std::move(parts[0]);
    } else {
      auto conjunction = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
        duckdb::ExpressionType::CONJUNCTION_AND);
      for (auto& part : parts) {
        conjunction->GetChildrenMutable().push_back(std::move(part));
      }
      expr = std::move(conjunction);
    }
    out.column_filters.push_back(
      {.column = column,
       .type = std::move(type),
       .filter = duckdb::make_uniq<duckdb::ExpressionFilter>(std::move(expr))});
  }
  if (conjuncts.empty()) {
    return out;
  }
  auto root = std::make_unique<irs::BooleanFilter>();
  FilterScorers scorers;
  const auto status =
    MakeSearchFilter(*root, conjuncts, getter, context, expr_getter, &scorers);
  if (!status.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(status.message()));
  }
  irs::Filter::ptr filter = std::move(root);
  EnsureIncludeSides(*filter);
  const auto scorer =
    scan.score.text ? search::MakeScorer(*scan.score.text) : nullptr;
  irs::OptimizeContext ctx{.scored = static_cast<bool>(scan.score.text),
                           .scorer = scorer.get()};
  ctx.analyzed_fields.insert(claim.analyzed_fields.begin(),
                             claim.analyzed_fields.end());
  irs::Deferral deferral{.reader = reader};
  if (scan.DefersChecks()) {
    ctx.deferral = &deferral;
  }
  irs::Optimize(filter, ctx);
  out.filter = std::shared_ptr<const irs::Filter>{std::move(filter)};
  out.scorers = std::move(scorers);
  out.checks = MakeChecks(std::move(deferral));
  if (!out.dual_filters.empty() && !index_only.empty()) {
    auto rest = std::make_unique<irs::BooleanFilter>();
    FilterScorers rest_scorers;
    const auto rest_status = MakeSearchFilter(
      *rest, index_only, getter, context, expr_getter, &rest_scorers);
    SDB_ENSURE(rest_status.ok(), rest_status.message());
    irs::Filter::ptr rest_filter = std::move(rest);
    EnsureIncludeSides(*rest_filter);
    irs::Optimize(rest_filter,
                  {.scored = false, .analyzed_fields = ctx.analyzed_fields});
    out.index_only = std::move(rest_filter);
  }
  return out;
}

irs::Filter::ptr MakeVectorFilter(const VectorScorerOptions& vs,
                                  std::shared_ptr<const irs::Filter> inner,
                                  float radius,
                                  std::shared_ptr<const irs::Filter> index_only,
                                  bool dual) {
  if (vs.exact) {
    auto f = std::make_unique<irs::ByVectorExact>();
    *f->mutable_field_id() = vs.field_id;
    auto* o = f->mutable_options();
    o->query = vs.query_vector;
    o->ann_id = vs.ann_id;
    o->metric = vs.metric;
    o->radius = radius;
    o->inclusive = vs.radius_inclusive;
    o->inner = std::move(inner);
    return f;
  }
  if (vs.radius != std::numeric_limits<float>::max()) {
    auto f = MakeAnnFilter<irs::ByRadius>(vs, std::move(inner));
    auto* o = f->mutable_options();
    o->radius = radius;
    o->inclusive = vs.radius_inclusive;
    return f;
  }
  auto f = MakeAnnFilter<irs::ByVectorSimilarity>(vs, std::move(inner));
  auto* o = f->mutable_options();
  o->nprobe = vs.nprobe;
  o->min_search_fanout = vs.min_search_fanout;
  o->max_search_fanout = vs.max_search_fanout;
  o->ef_search = vs.ef_search;
  o->min_ef = vs.min_ef;
  o->top_k = vs.top_k;
  o->posting_size = vs.posting_size;
  o->rescore_width = vs.rescore_all || vs.top_k == 0 ? 0.f : vs.rescore_width;
  o->hnsw_filter_mode = vs.hnsw_filter_mode;
  o->hnsw_column_filter = vs.hnsw_column_filter;
  o->index_only = std::move(index_only);
  o->dual = dual;
  return f;
}

}  // namespace sdb::connector
