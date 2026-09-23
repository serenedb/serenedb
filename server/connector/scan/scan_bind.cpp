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

#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/boolean_rules.hpp>
#include <iresearch/search/filters/vector_exact_filter.hpp>
#include <iresearch/search/filters/vector_radius_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <cmath>
#include <magic_enum/magic_enum.hpp>
#include <ranges>

#include "catalog/entry/duckdb_table_entry.h"
#include "connector/optimizer/iresearch_plan.h"
#include "connector/search_filter_builder.hpp"
#include "query/config.h"

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

}  // namespace

std::vector<const catalog::InvertedIndex*> RelationSpec::InvertedIndexes()
  const {
  return indexes | std::views::transform([](const auto& index) {
           return &catalog::InvertedInfo(*index);
         }) |
         std::ranges::to<std::vector>();
}

double SegmentBeamShare(double k, size_t segments) noexcept {
  if (segments <= 1) {
    return k;
  }
  const double n = static_cast<double>(segments);
  const double mean = k / n;
  const double sd = std::sqrt(mean * (1.0 - 1.0 / n));
  return std::min(k, std::max(16.0, std::ceil(mean + 3.0 * sd)));
}

namespace {

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
    case irs::VectorQuantization::SQ8:
    case irs::VectorQuantization::USQ8:
      return 0.0;
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
  static constinit SettingRef gFanout{"sdb_ivf_max_search_fanout"};
  static constinit SettingRef gEfSearch{"sdb_hnsw_ef_search"};
  const auto nprobe = gNprobe.SignedInt(context);
  vs.nprobe = nprobe < 0 ? 0 : static_cast<uint32_t>(nprobe);
  vs.max_search_fanout = gFanout.Int(context);
  const auto ef = gEfSearch.SignedInt(context);
  vs.ef_search = ef < 0 ? 0 : static_cast<uint32_t>(ef);
  vs.hnsw_filter_mode = ReadHnswFilterMode(context);
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

bool ReadAnnExact(duckdb::ClientContext& context) {
  static constinit SettingRef gExact{"sdb_ann_exact"};
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

duckdb::unique_ptr<duckdb::FunctionData> ScanBindData::Copy() const {
  return duckdb::make_uniq<ScanBindData>(*this);
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

catalog::ColumnId ScanBindData::ColumnIdByName(std::string_view name) const {
  if (view) {
    const auto& names = view->column_names;
    for (size_t i = 0; i < names.size(); ++i) {
      if (names[i] == name) {
        return static_cast<catalog::ColumnId>(i);
      }
    }
    return catalog::kInvalidColumnId;
  }
  const auto& entry_columns = relation.table_entry->GetColumns();
  const duckdb::Identifier key{name};
  return entry_columns.ColumnExists(key)
           ? catalog::ColumnId{entry_columns.GetColumn(key).CatalogOid()}
           : catalog::kInvalidColumnId;
}

std::string_view ScanBindData::ColumnNameById(catalog::ColumnId col_id) const {
  if (view) {
    const auto idx = static_cast<size_t>(col_id);
    const auto& names = view->column_names;
    return idx < names.size() ? std::string_view{names[idx]}
                              : std::string_view{};
  }
  const auto* column = catalog::TableEntryColumn(*relation.table_entry, col_id);
  return column ? column->Name().GetIdentifierName() : std::string_view{};
}

duckdb::LogicalType ScanBindData::ColumnTypeById(
  catalog::ColumnId col_id) const {
  if (view) {
    const auto idx = static_cast<size_t>(col_id);
    return idx < columns.types.size() ? columns.types[idx]
                                      : duckdb::LogicalType::INVALID;
  }
  const auto* column = catalog::TableEntryColumn(*relation.table_entry, col_id);
  return column ? column->Type() : duckdb::LogicalType::INVALID;
}

std::string ScanBindData::DisplayColumnName(catalog::ColumnId col_id) const {
  auto name = ColumnNameById(col_id);
  if (!name.empty()) {
    return std::string{name};
  }
  if (relation.IsIndexRelation()) {
    const auto* expr = relation.ScannedIndex().ExpressionByFieldId(
      static_cast<irs::field_id>(col_id));
    if (expr && !expr->pretty_printed.empty()) {
      return expr->pretty_printed;
    }
  }
  return absl::StrCat("col", col_id);
}

bool ScanBindData::IsColumnNotNull(catalog::ColumnId col_id) const {
  if (view) {
    return false;
  }
  return catalog::TableEntryColumnNotNull(*relation.table_entry, col_id);
}

void ScanBindData::IterateColumns(const ColumnVisitor& cb) const {
  if (view) {
    const auto& names = view->column_names;
    for (size_t i = 0; i < names.size(); ++i) {
      cb(static_cast<catalog::ColumnId>(i), columns.types[i]);
    }
    return;
  }
  for (const auto& column : relation.table_entry->GetColumns().Logical()) {
    cb(catalog::ColumnId{column.CatalogOid()}, column.Type());
  }
}

bool ScanBindData::IsHnswScored() const noexcept {
  if (!score.vector) {
    return false;
  }
  for (const auto& index : relation.indexes) {
    const auto info =
      catalog::InvertedInfo(*index).GetAnnInfo(score.vector->field_id);
    if (info) {
      return info->kind == irs::AnnKind::Hnsw;
    }
  }
  return false;
}

ObjectId ScanBindData::RelationId() const {
  return view ? view->id : catalog::ScanRelationId(*relation.table_entry);
}

std::string_view ScanBindData::RelationName() const {
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
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                  ERR_MSG("ScanBind: should be provided via GetScanFunction"));
}

std::optional<duckdb::LogicalType> GeneratedPkTypeOf(const ScanBindData& bind) {
  if (bind.relation.IsSearchTable()) {
    return duckdb::LogicalType::ROW_TYPE;
  }
  return std::nullopt;
}

std::optional<catalog::PkSpec> ViewPkSpecOf(const ScanBindData& bind) {
  if (bind.view && bind.relation.IsIndexRelation() &&
      bind.relation.ScannedIndex().GetOptions().pk_column ==
        catalog::PkColumnKind::Has) {
    if (const auto& fp = bind.view->fast_path) {
      return fp->pk_spec;
    }
  }
  return std::nullopt;
}

const irs::Scorer* ResolvePruneScorer(
  const std::optional<catalog::ScorerOptions>& topk,
  const irs::Scorer* scorer) {
  return topk && scorer && scorer->Compatible(*topk) ? scorer : nullptr;
}

std::shared_ptr<const irs::Filter> BuildDeferredFilter(
  duckdb::ClientContext& context, const ScanBindData& scan) {
  SDB_ASSERT(scan.plan_cache.deferred);
  const auto& claim = *scan.plan_cache.deferred;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> conjuncts;
  conjuncts.reserve(claim.conjuncts.size());
  for (const auto& e : claim.conjuncts) {
    conjuncts.push_back(optimizer::NormalizeClaimShape(
      context,
      optimizer::SubstituteParameters(e->Copy(), /*with_values=*/true)));
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
  return std::shared_ptr<const irs::Filter>{std::move(filter)};
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
  o->max_search_fanout = vs.max_search_fanout;
  o->ef_search = vs.ef_search;
  o->min_ef = vs.min_ef;
  o->top_k = vs.top_k;
  o->posting_size = vs.posting_size;
  o->hnsw_filter_mode = vs.hnsw_filter_mode;
  o->inner = std::move(inner);
  return f;
}

}  // namespace sdb::connector
