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

#pragma once

#include <algorithm>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/phrase_variadic_pos.hpp"
#include "iresearch/search/phrase_iterator.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename Matcher, typename Leaf, bool HasBoost = false>
class PhraseVariadicSlots {
 public:
  using Slot = PhraseVariadicPos<Leaf, HasBoost>;

  struct Entry {
    Entry(Leaf* begin, uint32_t count, const score_t* boosts,
          TermInterval interval)
      : leaf{begin, count, boosts}, interval{interval} {}

    Slot leaf;
    TermInterval interval;
  };

  template<typename Init, typename... Args>
  PhraseVariadicSlots(size_t size, Init&& init,
                      std::span<const uint32_t> widths,
                      std::span<const score_t> boosts,
                      std::span<const TermInterval> intervals, Args&&... args)
    : _terms{size, std::forward<Init>(init)},
      _slots{widths.size(), std::piecewise_construct,
             [&](size_t i) {
               const auto offset = Offset(widths, i);
               return std::tuple<Leaf*, uint32_t, const score_t*, TermInterval>{
                 _terms.data() + offset, widths[i],
                 boosts.empty() ? nullptr : boosts.data() + offset,
                 intervals[i]};
             }},
      _probes{
        _slots.size(),
        [this](Slot*& slot, size_t i) noexcept { slot = &_slots[i].leaf; }},
      _matcher{_slots.size(), std::forward<Args>(args)...} {
    for (size_t i = 0, count = _slots.size(); i != count; ++i) {
      _matcher.Position(i) = {&_slots[i].leaf.Positions(), _slots[i].interval};
    }
    _matcher.Finish();
    SDB_ASSERT(widths.size() == intervals.size());
    SDB_ASSERT(boosts.empty() == !HasBoost);
    SDB_ASSERT(boosts.empty() || boosts.size() == size);
    SDB_ASSERT(!widths.empty());
    SDB_ASSERT(widths.size() > 1 || _terms.size() > 1);
    absl::c_sort(_probes, [](const Slot* lhs, const Slot* rhs) noexcept {
      return lhs->Estimate() < rhs->Estimate();
    });
  }

  PhraseVariadicSlots(PhraseVariadicSlots&&) = delete;
  PhraseVariadicSlots& operator=(PhraseVariadicSlots&&) = delete;

  size_t Size() const noexcept { return _slots.size(); }

  Slot& Nth(size_t i) noexcept { return *_probes[i]; }

  const Slot& Nth(size_t i) const noexcept { return *_probes[i]; }

  doc_id_t Seek(doc_id_t from) { return Agree(Nth(0).Seek(from)); }

  doc_id_t Next(doc_id_t) { return Agree(Nth(0).Advance()); }

  IRS_FORCE_INLINE doc_id_t ProbeRest(doc_id_t target) {
    for (auto it = _probes.begin() + 1, end = _probes.end(); it != end; ++it) {
      const auto probe = (*it)->Probe(target);
      if (probe != target) {
        return probe;
      }
    }
    return target;
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    for (auto* slot : _probes) {
      const auto probe = slot->Probe(target);
      if (probe != target) {
        return probe;
      }
    }
    return target;
  }

  bool Match(doc_id_t) {
    for (auto* slot : _probes) {
      slot->OnMatch();
    }
    if constexpr (requires { _matcher.template Match<true>(); }) {
      return _matcher.template Match<true>();
    } else {
      return _matcher.Match();
    }
  }

  uint32_t Freq() const noexcept { return _matcher.GetFreq(); }

  static constexpr bool kOffsets = Leaf::kOffsets;
  static_assert(kOffsets == Matcher::kOffsets);

  std::pair<uint32_t, uint32_t> Offsets() const noexcept
    requires(kOffsets)
  {
    return _matcher.Offsets();
  }

  bool NextAlignment()
    requires(kOffsets)
  {
    return _matcher.NextAlignment();
  }

  static constexpr bool kHasBoost = Matcher::kHasBoost;

  score_t Boost() const noexcept
    requires(kHasBoost)
  {
    return _matcher.GetBoost();
  }

 private:
  doc_id_t Agree(doc_id_t doc) {
    while (!doc_limits::eof(doc)) {
      const auto probe = ProbeRest(doc);
      if (probe == doc) {
        return doc;
      }
      doc = Nth(0).Seek(probe);
    }
    return doc;
  }
  static uint32_t Offset(std::span<const uint32_t> widths, size_t i) noexcept {
    uint32_t offset = 0;
    for (size_t j = 0; j != i; ++j) {
      offset += widths[j];
    }
    return offset;
  }

  search::FixedArray<Leaf> _terms;
  search::FixedArray<Entry> _slots;
  search::FixedArray<Slot*> _probes;
  Matcher _matcher;
};

}  // namespace irs::search
