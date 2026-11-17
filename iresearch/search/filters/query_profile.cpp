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

#include "iresearch/search/filters/query_profile.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <limits>
#include <typeinfo>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

uint64_t MeasureTimerTicks() noexcept {
  auto best = std::numeric_limits<int64_t>::max();
  for (int i = 0; i != 1000; ++i) {
    const auto start = duckdb::TimePoint::Now();
    best = std::min(best, duckdb::TimePoint::Now() - start);
  }
  return static_cast<uint64_t>(std::max<int64_t>(best, 0));
}

uint64_t Minus(uint64_t total, uint64_t part) noexcept {
  return total > part ? total - part : 0;
}

class ProfiledDocs : public QueryProfile::Profiled<docs::Root> {
 public:
  using Profiled::Profiled;

  uint32_t Run(doc_id_t min, doc_id_t max, doc_id_t* out) final {
    const auto count =
      _counter.Time([&] { return _inner->Run(min, max, out); });
    _counter.Docs(count);
    return count;
  }
};

class ProfiledLead : public QueryProfile::Profiled<lead::Node> {
 public:
  using Profiled::Profiled;

  doc_id_t Next() final {
    const auto doc = _counter.Sample([&] { return _inner->Next(); });
    _counter.Docs(!doc_limits::eof(doc));
    return doc;
  }

  doc_id_t Seek(doc_id_t target) final {
    const auto doc = _counter.Sample([&] { return _inner->Seek(target); });
    _counter.Docs(!doc_limits::eof(doc));
    return doc;
  }

  void FetchScoreArgs(uint32_t n) final { _inner->FetchScoreArgs(n); }

  ScoreFunction PrepareScore() final { return _inner->PrepareScore(); }
};

class ProfiledProbe : public QueryProfile::Profiled<probe::Node> {
 public:
  using Profiled::Profiled;

  doc_id_t Probe(doc_id_t target) final {
    const auto doc = _counter.Sample([&] { return _inner->Probe(target); });
    _counter.Docs(doc == target);
    return doc;
  }

  void FetchScoreArgs(uint32_t n) final { _inner->FetchScoreArgs(n); }

  ScoreFunction PrepareScore() final { return _inner->PrepareScore(); }
};

class ProfiledFill : public QueryProfile::Profiled<fill::Node> {
 public:
  using Profiled::Profiled;

  doc_id_t FillOr(doc_id_t min, doc_id_t max,
                  uint64_t* IRS_RESTRICT mask) final {
    const auto before = QueryProfile::WindowDocs(mask, min, max);
    const auto next =
      _counter.Time([&] { return _inner->FillOr(min, max, mask); });
    _counter.Docs(Minus(QueryProfile::WindowDocs(mask, min, max), before));
    return next;
  }

  doc_id_t FillAndNot(doc_id_t min, doc_id_t max,
                      uint64_t* IRS_RESTRICT mask) final {
    const auto before = QueryProfile::WindowDocs(mask, min, max);
    const auto next =
      _counter.Time([&] { return _inner->FillAndNot(min, max, mask); });
    _counter.Docs(Minus(before, QueryProfile::WindowDocs(mask, min, max)));
    return next;
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT scores) final {
    const auto before = QueryProfile::WindowDocs(mask, min, max);
    const auto next =
      _counter.Time([&] { return _inner->Fill(min, max, mask, scores); });
    _counter.Docs(Minus(QueryProfile::WindowDocs(mask, min, max), before));
    return next;
  }

  uint32_t Keep(const doc_id_t* docs, uint32_t count, doc_id_t* out) final {
    const auto kept =
      _counter.Time([&] { return _inner->Keep(docs, count, out); });
    _counter.Docs(kept);
    return kept;
  }

  uint32_t Drop(const doc_id_t* docs, uint32_t count, doc_id_t* out) final {
    const auto kept =
      _counter.Time([&] { return _inner->Drop(docs, count, out); });
    _counter.Docs(count - kept);
    return kept;
  }

  bool FillCostAbove(uint64_t cost, doc_id_t min, doc_id_t max) final {
    return _inner->FillCostAbove(cost, min, max);
  }

  detail::BitsetStorage* Folded() noexcept final { return _inner->Folded(); }
};

template<typename Wrapper, typename Ptr>
Ptr WrapPart(QueryProfile& profile, Ptr node, uint32_t id) {
  if (typeid(*node) == typeid(memory::OnHeap<Wrapper>)) {
    node = utils::downCast<Wrapper>(*node).Release();
  }
  return profile.WrapAs<Wrapper>(std::move(node), id);
}

}  // namespace

QueryProfile::QueryProfile(std::span<const Filter* const> filters, bool nested)
  : _nodes{std::make_unique<Prepared[]>(filters.size())},
    _size{static_cast<uint32_t>(filters.size())} {
  _ids.reserve(filters.size());
  for (uint32_t i = 0; i != _size; ++i) {
    _ids.try_emplace(filters[i], i);
  }
  if (nested) {
    _executed = std::make_unique<std::list<Executed>[]>(_size);
    _timer_ticks = MeasureTimerTicks();
  }
}

QueryBuilder::ptr QueryProfile::Prepare(const Filter& filter,
                                        const SubReader& segment,
                                        const PrepareContext& ctx) {
  const auto id = _ids.find(&filter);
  if (id == _ids.end()) {
    return filter.PrepareSegment(segment, ctx);
  }
  const auto start = duckdb::TimePoint::Now();
  auto query = filter.PrepareSegment(segment, ctx);
  auto& node = _nodes[id->second];
  node.ticks.fetch_add(static_cast<uint64_t>(duckdb::TimePoint::Now() - start),
                       std::memory_order_relaxed);
  node.segments.fetch_add(1, std::memory_order_relaxed);
  SDB_ASSERT(query);
  if (!QueryBuilder::IsEmpty(*query)) {
    node.terms.fetch_add(query->Leaves(), std::memory_order_relaxed);
    node.postings.fetch_add(query->Postings(), std::memory_order_relaxed);
    node.matches.fetch_add(query->EstimateMatches(), std::memory_order_relaxed);
    node.kind.store(query->Kind(), std::memory_order_relaxed);
    if (_executed) {
      query->SetProfile(*this, id->second);
    }
  }
  return query;
}

QueryProfile::Executed& QueryProfile::Plan(uint32_t node,
                                           const std::type_info& type) {
  SDB_ASSERT(_executed && node < _size);
  const absl::MutexLock lock{&_mutex};
  auto& entries = _executed[node];
  auto entry = absl::c_find_if(
    entries, [&](const Executed& executed) { return *executed.type == type; });
  if (entry == entries.end()) {
    entry = entries.emplace(entries.end());
    entry->type = &type;
  }
  entry->plans.fetch_add(1, std::memory_order_relaxed);
  return *entry;
}

std::vector<QueryProfile::ExecutedNode> QueryProfile::Slots() const {
  std::vector<ExecutedNode> slots;
  if (!_executed) {
    return slots;
  }
  const absl::MutexLock lock{&_mutex};
  for (uint32_t node = 0; node != _size; ++node) {
    for (const auto& executed : _executed[node]) {
      if (executed.plans.load(std::memory_order_relaxed) != 0) {
        slots.push_back({.node = node, .executed = &executed});
      }
    }
  }
  return slots;
}

uint64_t QueryProfile::WindowDocs(const uint64_t* mask, doc_id_t min,
                                  doc_id_t max) noexcept {
  return max > min ? detail::CountBitRange(mask, 0, max - min) : 0;
}

docs::Root::ptr QueryProfile::WrapInner(docs::Root::ptr node, uint32_t id) {
  return WrapAs<ProfiledDocs>(std::move(node), id);
}

lead::Node::ptr QueryProfile::Wrap(lead::Node::ptr node, uint32_t id) {
  return WrapPart<ProfiledLead>(*this, std::move(node), id);
}

probe::Node::ptr QueryProfile::Wrap(probe::Node::ptr node, uint32_t id) {
  return WrapPart<ProfiledProbe>(*this, std::move(node), id);
}

fill::Node::ptr QueryProfile::Wrap(fill::Node::ptr node, uint32_t id) {
  return WrapPart<ProfiledFill>(*this, std::move(node), id);
}

}  // namespace irs
