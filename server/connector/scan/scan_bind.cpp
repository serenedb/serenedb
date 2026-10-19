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
#include <duckdb/planner/binder.hpp>
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
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ranges>

#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/scan/deferred_check.h"
#include "connector/scan/scan_function.h"
#include "connector/search_filter_builder.hpp"
#include "pg/connection_context.h"
#include "query/config.h"
#include "search/scorer_options.h"
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
  duckdb::ClientContext& context, const ScanBindData& bind) {
  const auto snapshot = bind.Snapshot(context);
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
      for (const auto& segment : snapshot->reader) {
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
  duckdb::ClientContext& context, duckdb::idx_t database_oid, duckdb::idx_t oid,
  const std::shared_ptr<search::SearchTable>& store) {
  auto* conn_ctx = GetSereneDBContextPtr(context);
  if (!conn_ctx) {
    return store->GetDirectoryReader();
  }
  return irs::DirectoryReader{*conn_ctx->SearchTxn().EnsureSearchTableReader(
    database_oid, oid, [&] { return store->GetDirectoryReader(); })};
}

decltype(SearchSpec::acquire_snapshot) SearchTableSnapshot(
  const catalog::SearchTableEntry& table) {
  return [database_oid = table.catalog.GetOid(), oid = table.oid,
          store = table.Storage()](
           duckdb::ClientContext& context) -> search::InvertedIndexSnapshotPtr {
    return std::make_shared<search::InvertedIndexSnapshot>(
      PinnedSearchReader(context, database_oid, oid, store), nullptr);
  };
}

void AcquireSnapshot(ScanBindData& data, duckdb::ClientContext& context,
                     decltype(SearchSpec::acquire_snapshot) acquire) {
  data.search.planned_reader = acquire(context)->reader.GetImpl();
  data.search.acquire_snapshot = std::move(acquire);
}

class SnapshotVersion final : public duckdb::PlanDataVersion {
 public:
  SnapshotVersion(std::weak_ptr<const irs::DirectoryReaderImpl> planned,
                  decltype(SearchSpec::acquire_snapshot) acquire)
    : _planned{std::move(planned)}, _acquire{std::move(acquire)} {}

  bool IsCurrent(duckdb::ClientContext& context) const final {
    const auto snapshot = _acquire(context);
    const auto& current = snapshot->reader.GetImpl();
    return !_planned.owner_before(current) && !current.owner_before(_planned);
  }

 private:
  std::weak_ptr<const irs::DirectoryReaderImpl> _planned;
  decltype(SearchSpec::acquire_snapshot) _acquire;
};

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

double AutoOversample(const VectorScorerOptions& vs) noexcept {
  switch (vs.quant) {
    case irs::VectorQuantization::None:
    case irs::VectorQuantization::FP16:
    case irs::VectorQuantization::BF16:
      return 0.0;
    case irs::VectorQuantization::SQ8:
    case irs::VectorQuantization::USQ8:
    case irs::VectorQuantization::SQ4:
    case irs::VectorQuantization::USQ4:
    case irs::VectorQuantization::PQ:
    case irs::VectorQuantization::RaBitQ:
    case irs::VectorQuantization::TQ:
    case irs::VectorQuantization::BQ:
    case irs::VectorQuantization::OSQ:
      return 1.0;
  }
  return 0.0;
}

template<typename AnnFilter>
std::unique_ptr<AnnFilter> MakeAnnFilter(
  const VectorScorerOptions& vs, std::shared_ptr<const irs::Filter> inner) {
  auto f = std::make_unique<AnnFilter>();
  *f->mutable_field_id() = vs.field_id;
  auto* o = f->mutable_options();
  o->query = vs.query_vector;
  o->centroids_id = vs.centroids_id;
  o->postings_id = vs.postings_id;
  o->metric = vs.metric;
  o->quant = vs.quant;
  o->inner = std::move(inner);
  return f;
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
  auto factor = GetDoubleSetting(context, "sdb_ann_oversample");
  chosen_here = factor < 0.0;
  return chosen_here ? AutoOversample(vs) : factor;
}

double OversamplePool(double factor, double count) noexcept {
  return factor > 0.0 ? std::max(count, std::ceil(factor * count)) : 0.0;
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

bool ScanBindData::ReadsParametersAtExecution() const {
  return !plan_cache.declined_parameter && !ts_dict.Active() &&
         !offsets.Active();
}

duckdb::shared_ptr<duckdb::PlanDataVersion> ScanBindData::GetPlanDataVersion()
  const {
  return duckdb::make_shared_ptr<SnapshotVersion>(search.planned_reader,
                                                  search.acquire_snapshot);
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
  const auto live = Snapshot(context)->reader.live_docs_count();
  const auto* filter = search.filter.get();
  const auto estimate = filter ? EstimateFilterMatchCount(*filter, live) : live;
  return duckdb::make_uniq<duckdb::NodeStatistics>(estimate, live);
}

duckdb::unique_ptr<duckdb::FunctionData> ScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  SDB_IF_FAILURE("connector::ScanBind") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
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
  if (input.binder) {
    input.binder->GetStatementProperties().RegisterDBRead(
      index->ParentCatalog(), context);
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
  auto data =
    relation.type == duckdb::CatalogType::VIEW_ENTRY
      ? MakeViewScanBindData(context, relation.Cast<duckdb::ViewCatalogEntry>(),
                             entry.options)
      : MakeTableScanBindData(relation.Cast<duckdb::TableCatalogEntry>(),
                              search_table ? ScanEntryKind::SearchTableIndex
                                           : ScanEntryKind::InvertedIndex,
                              search_table ? "search" : "table");
  data->relation.inverted_index = &entry;
  if (search_table) {
    AcquireSnapshot(*data, context, SearchTableSnapshot(*search_table));
  } else {
    AcquireSnapshot(
      *data, context,
      [oid = entry.oid, storage = entry.Storage()](
        duckdb::ClientContext& ctx) -> search::InvertedIndexSnapshotPtr {
        return GetSereneDBContext(ctx).EnsureSearchSnapshot(oid, storage);
      });
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
  SDB_IF_FAILURE("connector::ScanBind") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  const auto& store = entry.Storage();
  auto data =
    MakeTableScanBindData(entry, ScanEntryKind::SearchTable, "search");
  AcquireSnapshot(*data, context, SearchTableSnapshot(entry));
  data->score.prune = store->TopKScorer();
  data->relation.inverted_config = store->ScanConfig();
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

DeferredBuild BuildDeferredFilter(
  duckdb::ClientContext& context, const ScanBindData& scan,
  const search::InvertedIndexSnapshot& snapshot) {
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
    const auto it = claim.columns->find(MakeBindingKey(ref.Binding()));
    if (it == claim.columns->end()) {
      return std::nullopt;
    }
    auto info = optimizer::ResolveSearchColumnById(
      context, scan, it->second.column, it->second.column_stored);
    if (info && info->column_stored) {
      info->stats = snapshot.reader.GetColumnStats(it->second.column);
    }
    return info;
  };
  const ExpressionGetter expr_getter =
    [](const duckdb::Expression&) -> std::optional<SearchColumnInfo> {
    return std::nullopt;
  };
  DeferredBuild out;
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
        conjuncts.push_back(std::move(range));
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
        conjuncts.push_back(std::move(range));
      }
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
  const auto scorer =
    scan.score.text ? search::MakeScorer(*scan.score.text) : nullptr;
  irs::OptimizeContext ctx{.scored = static_cast<bool>(scan.score.text),
                           .scorer = scorer.get()};
  ctx.analyzed_fields.insert(claim.analyzed_fields.begin(),
                             claim.analyzed_fields.end());
  irs::containers::FlatHashMap<irs::field_id, irs::field_id> null_markers;
  for (const auto& [marker, field] : claim.null_markers) {
    null_markers[marker] = field;
  }
  ctx.null_markers = &null_markers;
  irs::Deferral deferral{.reader = snapshot.reader};
  if (scan.DefersChecks()) {
    ctx.deferral = &deferral;
  }
  irs::Optimize(filter, ctx);
  out.filter = std::shared_ptr<const irs::Filter>{std::move(filter)};
  out.scorers = std::move(scorers);
  out.checks = MakeChecks(std::move(deferral));
  return out;
}

irs::Filter::ptr MakeVectorFilter(const VectorScorerOptions& vs,
                                  std::shared_ptr<const irs::Filter> inner,
                                  float radius) {
  if (vs.exact) {
    auto f = std::make_unique<irs::ByVectorExact>();
    *f->mutable_field_id() = vs.field_id;
    auto* o = f->mutable_options();
    o->query = vs.query_vector;
    o->centroids_id = vs.centroids_id;
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
  o->hnsw_filter_mode = vs.hnsw_filter_mode;
  o->hnsw_column_filter = vs.hnsw_column_filter;
  return f;
}

}  // namespace sdb::connector
