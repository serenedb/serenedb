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
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/boolean_rules.hpp>
#include <iresearch/search/filters/vector_exact_filter.hpp>
#include <iresearch/search/filters/vector_radius_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ranges>

#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/scan/scan_function.h"
#include "connector/search_filter_builder.hpp"
#include "pg/connection_context.h"
#include "query/config.h"
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
    if (TableColumnId(column) == col_id) {
      return &column;
    }
  }
  return nullptr;
}

irs::DirectoryReader PinnedSearchReader(
  duckdb::ClientContext& context, duckdb::idx_t oid,
  const std::shared_ptr<search::SearchTable>& store) {
  auto* conn_ctx = GetSereneDBContextPtr(context);
  if (!conn_ctx) {
    return store->GetDirectoryReader();
  }
  return irs::DirectoryReader{*conn_ctx->SearchTxn().EnsureSearchTableReader(
    oid, [&] { return store->GetDirectoryReader(); })};
}

irs::DirectoryReader PinnedSearchReader(
  duckdb::ClientContext& context, const catalog::SearchTableEntry& table) {
  return PinnedSearchReader(context, table.oid, table.Storage());
}

decltype(PlanCacheSpec::reacquire_snapshot) SearchTableReacquire(
  const catalog::SearchTableEntry& table) {
  return [oid = table.oid, store = table.Storage()](
           duckdb::ClientContext& context) -> search::InvertedIndexSnapshotPtr {
    return std::make_shared<search::InvertedIndexSnapshot>(
      PinnedSearchReader(context, oid, store), nullptr);
  };
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
    data->columns.ids.emplace_back(TableColumnId(column));
    data->columns.types.emplace_back(column.Type());
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

// Auto is 0 or 1 and never more: a default decides *whether* to re-score, not
// how much to spend on it. 1 gives every segment its own pool, re-scored
// exactly, with only real scores crossing a segment boundary -- Qdrant's
// property. 0 lets quantized scores merge across segments directly.
//
// Which side a quantizer falls on is measured rather than read off its bit
// count. On 120k x 64, eight segments, k=10, moving from 0 to 1 gains:
//
//     sq8   +0.018 hnsw  +0.021 ivf     sq4     +0.298  +0.318
//     usq8  +0.020       +0.025         usq4    +0.347  +0.363
//                                       pq          --  +0.442
//                                       rabitq3 +0.265  +0.281
//                                       tq3     +0.360  +0.418
//
// Eight-bit codes rank well enough alone that re-scoring buys two points of
// recall for the reads it costs, which is not a trade to make for everyone by
// default. Every narrower code is unusable without it.
double AutoOversample(const VectorScorerOptions& vs) noexcept {
  switch (vs.quant) {
    case irs::VectorQuantization::None:
      return 0.0;
    case irs::VectorQuantization::SQ8:
    case irs::VectorQuantization::USQ8:
    case irs::VectorQuantization::SQ4:
    case irs::VectorQuantization::USQ4:
    case irs::VectorQuantization::PQ:
    case irs::VectorQuantization::RaBitQ:
    case irs::VectorQuantization::TQ:
      return 1.0;
  }
  return 0.0;
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

double AnnOversample(duckdb::ClientContext& context,
                     const VectorScorerOptions& vs, bool& chosen_here) {
  static constinit SettingRef gOversample{"sdb_ann_oversample"};
  auto factor = gOversample.Double(context);
  chosen_here = factor < 0.0;
  return chosen_here ? AutoOversample(vs) : factor;
}

void RefreshVectorKnobs(VectorScorerOptions& vs,
                        duckdb::ClientContext& context) {
  static constinit SettingRef gNprobe{"sdb_ivf_search_nprobe"};
  static constinit SettingRef gMinFanout{"sdb_ivf_min_search_fanout"};
  static constinit SettingRef gMaxFanout{"sdb_ivf_max_search_fanout"};
  static constinit SettingRef gEfSearch{"sdb_hnsw_ef_search"};
  const auto nprobe = gNprobe.SignedInt(context);
  vs.nprobe = nprobe < 0 ? 0 : static_cast<uint32_t>(nprobe);
  const auto min_fanout = gMinFanout.SignedInt(context);
  vs.min_search_fanout = min_fanout < 0 ? 0 : static_cast<uint32_t>(min_fanout);
  const auto max_fanout = gMaxFanout.SignedInt(context);
  vs.max_search_fanout = max_fanout < 0 ? 0 : static_cast<uint32_t>(max_fanout);
  const auto ef = gEfSearch.SignedInt(context);
  vs.ef_search = ef < 0 ? 0 : static_cast<uint32_t>(ef);
  vs.hnsw_filter_mode = ReadHnswFilterMode(context);
  vs.hnsw_column_filter = ReadHnswColumnFilter(context);
  vs.exact = ReadAnnExact(context);
}

irs::HnswFilterMode ReadHnswFilterMode(duckdb::ClientContext& context) {
  static constexpr auto kModes = magic_enum::enum_names<irs::HnswFilterMode>();
  static constinit SettingRef gFilterMode{"sdb_hnsw_filter_mode"};
  const auto mode = gFilterMode.Enum(context, kModes);
  if (mode >= kModes.size()) {
    return irs::HnswFilterMode::Auto;
  }
  return static_cast<irs::HnswFilterMode>(mode);
}

irs::HnswColumnFilter ReadHnswColumnFilter(duckdb::ClientContext& context) {
  static constexpr auto kModes =
    magic_enum::enum_names<irs::HnswColumnFilter>();
  static constinit SettingRef gColumnFilter{"sdb_hnsw_column_filter"};
  const auto mode = gColumnFilter.Enum(context, kModes);
  if (mode >= kModes.size()) {
    return irs::HnswColumnFilter::Auto;
  }
  return static_cast<irs::HnswColumnFilter>(mode);
}

bool ReadAnnExact(duckdb::ClientContext& context) {
  static constinit SettingRef gExact{"sdb_ann_force_exact"};
  return gExact.Bool(context);
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
    cb(TableColumnId(column), column.Type());
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
  if (!search.snapshot) {
    return nullptr;
  }
  const auto live = search.snapshot->reader.live_docs_count();
  const auto* filter = search.filter.get();
  const auto estimate = filter ? EstimateFilterMatchCount(*filter, live) : live;
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate, live);
}

duckdb::unique_ptr<duckdb::FunctionData> ScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
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
  auto host = index->GetRelation(index->catalog.GetCatalogTransaction(context));
  if (!host) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG("relation \"", index->GetTableName().GetIdentifierName(),
              "\" does not exist"));
  }
  auto& relation = *host;
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
  if (search_table) {
    data->plan_cache.reacquire_snapshot = SearchTableReacquire(*search_table);
  } else {
    data->plan_cache.reacquire_snapshot =
      [oid = entry.oid, storage = entry.Storage()](
        duckdb::ClientContext& ctx) -> search::InvertedIndexSnapshotPtr {
      return GetSereneDBContext(ctx).EnsureSearchSnapshot(oid, storage);
    };
  }
  data->relation.inverted_config = entry.Config();
  data->relation.row_group_size =
    search_table ? search_table->Storage()->Config()->row_group_size
                 : entry.Config()->row_group_size;
  data->score.prune = search_table ? search_table->Storage()->TopKScorer()
                                   : entry.Config()->top_k_scorer;
  data->IterateColumns([&](ColumnId id, const duckdb::LogicalType& type) {
    return_types.emplace_back(type);
    names.emplace_back(data->ColumnNameById(id));
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
  data->plan_cache.reacquire_snapshot = SearchTableReacquire(entry);
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

DeferredBuild BuildDeferredFilter(duckdb::ClientContext& context,
                                  const ScanBindData& scan) {
  SDB_ASSERT(scan.plan_cache.deferred);
  const auto& claim = *scan.plan_cache.deferred;
  const auto bound = [&](const std::shared_ptr<const duckdb::Expression>& e) {
    return optimizer::NormalizeClaimShape(
      context,
      optimizer::SubstituteParameters(e->Copy(), /*with_values=*/true));
  };
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> conjuncts;
  conjuncts.reserve(claim.conjuncts.size() + claim.ranges.size());
  for (const auto& e : claim.conjuncts) {
    conjuncts.push_back(bound(e));
  }
  const ColumnGetter getter = [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<SearchColumnInfo> {
    const auto it = claim.columns->find(
      {ref.Binding().table_index.index, ref.Binding().column_index.GetIndex()});
    if (it == claim.columns->end()) {
      return std::nullopt;
    }
    return optimizer::ResolveSearchColumnById(context, scan, it->second.column,
                                              it->second.column_stored);
  };
  const ExpressionGetter expr_getter =
    [](const duckdb::Expression&) -> std::optional<SearchColumnInfo> {
    return std::nullopt;
  };
  DeferredBuild out;
  std::map<ColumnId,
           std::pair<duckdb::LogicalType,
                     duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>>>
    wide;
  for (const auto& e : claim.ranges) {
    auto range = bound(e);
    auto probe = std::make_unique<irs::BooleanFilter>();
    std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&range, 1};
    FilterScorers probe_scorers;
    if (MakeSearchFilter(*probe, single, getter, context, expr_getter,
                         &probe_scorers, WideRanges::DeclineWide)
          .ok()) {
      conjuncts.push_back(std::move(range));
      continue;
    }
    const duckdb::BoundColumnRefExpression* ref = nullptr;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundColumnRefExpression>(
      *range, [&](const duckdb::BoundColumnRefExpression& r) { ref = &r; });
    SDB_ENSURE(ref != nullptr, "a deferred range reads one column");
    const auto it =
      claim.columns->find({ref->Binding().table_index.index,
                           ref->Binding().column_index.GetIndex()});
    SDB_ENSURE(it != claim.columns->end(),
               "a deferred range's column was resolved at plan time");
    auto& [type, parts] = wide[it->second.column];
    type = ref->GetReturnType();
    if (range->GetExpressionType() == duckdb::ExpressionType::COMPARE_BETWEEN) {
      using duckdb::BoundBetweenExpression;
      using duckdb::ExpressionType;
      const auto& between = range->Cast<duckdb::BoundFunctionExpression>();
      parts.push_back(duckdb::BoundComparisonExpression::Create(
        BoundBetweenExpression::LowerInclusive(between)
          ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
          : ExpressionType::COMPARE_GREATERTHAN,
        BoundBetweenExpression::Input(between).Copy(),
        BoundBetweenExpression::LowerBound(between).Copy()));
      parts.push_back(duckdb::BoundComparisonExpression::Create(
        BoundBetweenExpression::UpperInclusive(between)
          ? ExpressionType::COMPARE_LESSTHANOREQUALTO
          : ExpressionType::COMPARE_LESSTHAN,
        BoundBetweenExpression::Input(between).Copy(),
        BoundBetweenExpression::UpperBound(between).Copy()));
    } else {
      parts.push_back(std::move(range));
    }
  }
  for (auto& [column, typed] : wide) {
    auto& [type, parts] = typed;
    for (auto& part : parts) {
      duckdb::ExpressionIterator::VisitExpressionMutable<
        duckdb::BoundColumnRefExpression>(
        part, [](duckdb::BoundColumnRefExpression& r,
                 duckdb::unique_ptr<duckdb::Expression>& child) {
          child = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
            r.GetAlias(), r.GetReturnType(), 0ULL);
        });
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
                    ERR_MSG("cannot build the search filter from the "
                            "statement's parameters: ",
                            status.message()));
  }
  irs::Filter::ptr filter = std::move(root);
  EnsureIncludeSides(*filter);
  irs::OptimizeContext ctx;
  ctx.analyzed_fields.insert(claim.analyzed_fields.begin(),
                             claim.analyzed_fields.end());
  irs::containers::FlatHashMap<irs::field_id, irs::field_id> null_markers;
  for (const auto& [marker, field] : claim.null_markers) {
    null_markers[marker] = field;
  }
  ctx.null_markers = &null_markers;
  irs::Optimize(filter, ctx);
  out.filter = std::shared_ptr<const irs::Filter>{std::move(filter)};
  return out;
}

irs::Filter::ptr MakeVectorFilter(const VectorScorerOptions& vs,
                                  std::shared_ptr<const irs::Filter> inner,
                                  float radius) {
  if (vs.exact && vs.radius == std::numeric_limits<float>::max()) {
    auto f = std::make_unique<irs::ByVectorExact>();
    *f->mutable_field_id() = vs.field_id;
    auto* o = f->mutable_options();
    o->query = vs.query_vector;
    o->metric = vs.metric;
    o->inner = std::move(inner);
    return f;
  }
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
  o->min_search_fanout = vs.min_search_fanout;
  o->max_search_fanout = vs.max_search_fanout;
  o->ef_search = vs.ef_search;
  o->min_ef = vs.min_ef;
  o->top_k = vs.top_k;
  o->posting_size = vs.posting_size;
  o->hnsw_filter_mode = vs.hnsw_filter_mode;
  o->hnsw_column_filter = vs.hnsw_column_filter;
  o->inner = std::move(inner);
  return f;
}

}  // namespace sdb::connector
