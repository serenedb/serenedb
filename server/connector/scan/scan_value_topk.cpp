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

#include <absl/algorithm/container.h>
#include <absl/synchronization/mutex.h>

#include <cstring>
#include <iresearch/formats/term_reader.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/detail/term_iterator.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <utility>

#include "connector/optimizer/iresearch_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

struct ValueLess {
  bool operator()(const ValueHit& l, const ValueHit& r) const noexcept {
    if (const auto c = std::memcmp(l.key.data(), r.key.data(), l.key.size());
        c != 0) {
      return c < 0;
    }
    return std::pair{l.seg, l.doc} < std::pair{r.seg, r.doc};
  }
};

void Intersect(ValueOrderSpec& spec, irs::bstring from, irs::bstring to) {
  if (!from.empty() && (spec.from.empty() || from > spec.from)) {
    spec.from = std::move(from);
  }
  if (!to.empty() && (spec.to.empty() || to < spec.to)) {
    spec.to = std::move(to);
  }
  spec.nulls = false;
}

bool Equal(ValueOrderSpec& spec, irs::field_id field, irs::DictRef dict,
           irs::bytes_view term) {
  if (field != spec.field || dict != irs::NumericLayout::kNumbers) {
    return false;
  }
  Intersect(spec, irs::bstring{term}, irs::AfterKey(term));
  return true;
}

bool Narrow(ValueOrderSpec& spec, const irs::Filter& filter) {
  const auto type = filter.type();
  if (type == irs::Type<irs::All>::id()) {
    return true;
  }
  if (type == irs::Type<irs::ByGranularRange>::id()) {
    const auto& range = irs::utils::downCast<irs::ByGranularRange>(filter);
    if (range.field_id() != spec.field ||
        range.dict() != irs::NumericLayout::kNumbers) {
      return false;
    }
    const auto& bounds = range.options().range;
    Intersect(spec, irs::RangeLower(bounds.min, bounds.min_type),
              irs::RangeUpper(bounds.max, bounds.max_type));
    return true;
  }
  if (type != irs::Type<irs::BooleanFilter>::id()) {
    return false;
  }
  const auto& node = irs::utils::downCast<irs::BooleanFilter>(filter);
  if (node.Size(irs::Occur::Must) == 0 || node.Size(irs::Occur::Should) != 0 ||
      !node.Filters(irs::Occur::MustNot).empty()) {
    return false;
  }
  for (const auto& clause : node.Terms(irs::Occur::MustNot)) {
    if (clause.field != spec.field ||
        clause.dict != irs::NumericLayout::kNulls || !clause.term.empty()) {
      return false;
    }
    spec.nulls = false;
  }
  for (const auto& clause : node.Terms(irs::Occur::Must)) {
    if (!Equal(spec, clause.field, clause.dict, clause.term)) {
      return false;
    }
  }
  return absl::c_all_of(node.Filters(irs::Occur::Must),
                        [&](const irs::Filter::ptr& child) {
                          return child && Narrow(spec, *child);
                        });
}

ValueHit ValueOf(const ValueOrderSpec& spec, irs::bytes_view term, uint32_t seg,
                 irs::doc_id_t doc) {
  SDB_ASSERT(term.size() < ValueHit{}.key.size());
  ValueHit hit{.seg = seg, .doc = doc};
  hit.key[0] = spec.nulls_first ? 1 : 0;
  const irs::byte_type flip = spec.descending ? 0xFF : 0;
  for (size_t i = 0; i != term.size(); ++i) {
    hit.key[1 + i] = term[i] ^ flip;
  }
  return hit;
}

ValueHit NullOf(const ValueOrderSpec& spec, uint32_t seg, irs::doc_id_t doc) {
  ValueHit hit{.seg = seg, .doc = doc};
  hit.key[0] = spec.nulls_first ? 0 : 1;
  return hit;
}

void MergeHits(std::vector<ValueHit>& best, const std::vector<ValueHit>& hits,
               std::vector<ValueHit>& merged, size_t limit) {
  merged.clear();
  auto a = best.begin();
  auto b = hits.begin();
  while (merged.size() < limit && (a != best.end() || b != hits.end())) {
    if (b == hits.end() || (a != best.end() && ValueLess{}(*a, *b))) {
      merged.push_back(*a++);
    } else {
      merged.push_back(*b++);
    }
  }
  best.swap(merged);
}

void RefreshBound(ScanGlobalState& g, ValueTopKLocalState& l) {
  auto& shared = g.value;
  if (shared.bound_version.load(std::memory_order_acquire) != l.bound_version) {
    absl::ReaderMutexLock lock{&shared.mutex};
    l.bound = shared.bound;
    l.bound_version = shared.bound_version.load(std::memory_order_relaxed);
  }
}

void Tighten(ScanGlobalState::ValueTopKState& shared, const ValueHit& bound)
  ABSL_EXCLUSIVE_LOCKS_REQUIRED(shared.mutex) {
  if (shared.bound_version.load(std::memory_order_relaxed) == 0 ||
      ValueLess{}(bound, shared.bound)) {
    shared.bound = bound;
    shared.bound_version.fetch_add(1, std::memory_order_release);
  }
}

void CollectSegment(ScanGlobalState& g, ValueTopKLocalState& l) {
  const auto& spec = *g.Bind().value_order;
  const auto seg_idx = l.unit.seg;
  const auto& seg = (*g.reader)[seg_idx];
  const auto mask = seg.MaskedDocs();
  const auto limit = spec.limit;
  l.hits.clear();
  bool going = true;
  const auto take = [&](const ValueHit& hit) {
    if (l.bound_version != 0 && !ValueLess{}(hit, l.bound)) {
      going = false;
      return false;
    }
    if (!mask.Contains(hit.doc)) {
      l.hits.push_back(hit);
    }
    going = l.hits.size() < limit;
    return going;
  };
  const auto* nulls =
    spec.nulls ? seg.field(spec.field, irs::NumericLayout::kNulls) : nullptr;
  const auto read_nulls = [&] {
    if (nulls && going) {
      RefreshBound(g, l);
      nulls->ReadDocs(irs::kSlotTerm, [&](irs::doc_id_t doc) {
        return take(NullOf(spec, seg_idx, doc));
      });
    }
  };
  if (spec.nulls_first) {
    read_nulls();
  }
  const auto* values = seg.field(spec.field, irs::NumericLayout::kNumbers);
  if (values && going) {
    irs::TermPostings::ptr docs;
    values->VisitRange(
      spec.from, spec.to, spec.descending,
      [&](irs::bytes_view term, const irs::PostingMeta& meta) {
        if (term.size() >= ValueHit{}.key.size()) [[unlikely]] {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INDEX_CORRUPTED),
            ERR_MSG("inverted index: numeric term of ", term.size(), " bytes"));
        }
        RefreshBound(g, l);
        irs::ReadPostingDocs(
          *values, meta,
          [&](irs::doc_id_t doc) {
            return take(ValueOf(spec, term, seg_idx, doc));
          },
          docs);
        return going;
      });
  }
  if (!spec.nulls_first) {
    read_nulls();
  }
}

void KeepSegment(ScanGlobalState& g, ValueTopKLocalState& l) {
  const auto limit = g.Bind().value_order->limit;
  if (l.hits.empty()) {
    return;
  }
  MergeHits(l.best, l.hits, l.merged, limit);
  if (l.best.size() != limit ||
      (l.bound_version != 0 && !ValueLess{}(l.best.back(), l.bound))) {
    return;
  }
  auto& shared = g.value;
  absl::MutexLock lock{&shared.mutex};
  Tighten(shared, l.best.back());
  l.bound = shared.bound;
  l.bound_version = shared.bound_version.load(std::memory_order_relaxed);
}

void PublishBest(ScanGlobalState& g, ValueTopKLocalState& l) {
  if (l.best.empty()) {
    return;
  }
  const auto limit = g.Bind().value_order->limit;
  auto& shared = g.value;
  absl::MutexLock lock{&shared.mutex};
  MergeHits(shared.best, l.best, l.merged, limit);
  if (shared.best.size() == limit) {
    Tighten(shared, shared.best.back());
  }
}

void Merge(ScanGlobalState& g) {
  auto& t = g.topk;
  auto& shared = g.value;
  absl::MutexLock lock{&shared.mutex};
  t.answer.clear();
  for (size_t rank = g.top_offset; rank < shared.best.size(); ++rank) {
    const auto& hit = shared.best[rank];
    t.answer.push_back({.score = -static_cast<irs::score_t>(rank),
                        .doc = hit.doc,
                        .segment_idx = hit.seg});
  }
  PlanFetch(g);
}

}  // namespace

std::optional<ValueOrderSpec> ValueOrderOf(
  duckdb::ClientContext& context, const ScanBindData& bind, ColumnId column,
  const duckdb::RowGroupOrderOptions& options) {
  constexpr uint64_t kExactRanks = uint64_t{1}
                                   << std::numeric_limits<irs::score_t>::digits;
  const bool descending = options.order_type == duckdb::OrderType::DESCENDING;
  const bool nulls_first =
    options.null_order == duckdb::OrderByNullType::NULLS_FIRST;
  if (!options.row_limit.IsValid() || options.row_limit.GetIndex() == 0 ||
      options.row_limit.GetIndex() > kExactRanks ||
      options.column_type != duckdb::OrderByColumnType::NUMERIC ||
      (!descending && options.order_type != duckdb::OrderType::ASCENDING) ||
      (!nulls_first &&
       options.null_order != duckdb::OrderByNullType::NULLS_LAST) ||
      column > kMaxRealColumnIdValue) {
    return std::nullopt;
  }
  const auto& search = bind.search;
  if (bind.score.text || bind.score.vector || bind.score.order ||
      bind.score.HasTopK() || bind.offsets.Active() || bind.ts_dict.Active() ||
      bind.plan_cache.deferred || search.index_only ||
      !search.deferred.empty() || !search.duals.empty() ||
      !search.row_filters.empty()) {
    return std::nullopt;
  }
  const auto info =
    optimizer::ResolveSearchColumnById(context, bind, column, false);
  if (!info || info->layout != irs::DictLayout::Numeric) {
    return std::nullopt;
  }
  ValueOrderSpec spec{
    .column = column,
    .field = info->field_id,
    .limit = options.row_limit.GetIndex(),
    .descending = descending,
    .nulls_first = nulls_first,
  };
  if (search.filter && !Narrow(spec, *search.filter)) {
    return std::nullopt;
  }
  return spec;
}

void InitValueTopKGlobal(ScanGlobalState& g) {
  auto& t = g.topk;
  t.limit = *g.top_k - std::min(*g.top_k, g.top_offset);
  t.shared_fetch = t.limit > STANDARD_VECTOR_SIZE;
}

void RunValueTopKScan(duckdb::ClientContext& ctx,
                      duckdb::TableFunctionInput& input, ScanGlobalState& g,
                      ValueTopKLocalState& l, duckdb::DataChunk& output) {
  if (!l.published) {
    while (NextLiveUnit(g, l)) {
      CollectSegment(g, l);
      KeepSegment(g, l);
      FinishUnit(g, l);
    }
    PublishBest(g, l);
    l.published = true;
    g.topk.published.fetch_add(1, std::memory_order_acq_rel);
  }
  RunAnswerStages(ctx, input, g, l, output, [&] { Merge(g); });
}

}  // namespace sdb::connector
