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

#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/detail/conjunction_leaves.hpp"
#include "iresearch/search/detail/phrase_matcher.hpp"
#include "iresearch/search/detail/phrase_variadic_pos.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

struct PhraseTerms {
  struct Repeat {
    uint32_t slot;
    uint32_t first;
    uint32_t rank;
  };

  PhraseTerms(std::span<const PostingMeta> metas,
              std::span<const TermInterval> intervals, uint64_t docs)
    : metas{metas}, docs{docs} {
    const bool distinct = absl::c_none_of(
      intervals.subspan(1),
      [](const TermInterval& interval) { return interval.offs_min == 0; });
    for (uint32_t slot = 0; slot != metas.size(); ++slot) {
      const auto first = static_cast<uint32_t>(
        std::find(metas.begin(), metas.begin() + slot, metas[slot]) -
        metas.begin());
      if (first == slot) {
        leaves.push_back(slot);
      } else {
        const auto rank = distinct ? static_cast<uint32_t>(std::count(
                                       metas.begin() + first + 1,
                                       metas.begin() + slot + 1, metas[slot]))
                                   : 0;
        repeats.push_back({slot, first, rank});
      }
    }
  }

  bool Defers(uint64_t lead, uint64_t term) const noexcept {
    return lead >= kDeferLead && term >= kDeferRatio * lead &&
           term * kDeferShare < docs;
  }

  bool Defers() const noexcept {
    uint64_t lead = std::numeric_limits<uint64_t>::max();
    for (const auto j : leaves) {
      lead = std::min<uint64_t>(lead, metas[j].docs_count);
    }
    return absl::c_any_of(
      leaves, [&](uint32_t j) { return Defers(lead, metas[j].docs_count); });
  }

  static constexpr uint64_t kDeferLead = 4 * doc_limits::kBlockSize;
  static constexpr uint64_t kDeferRatio = 16;
  static constexpr uint64_t kDeferShare = 4;

  std::span<const PostingMeta> metas;
  uint64_t docs;
  absl::InlinedVector<uint32_t, 8> leaves;
  absl::InlinedVector<Repeat, 4> repeats;
};

template<typename Matcher, typename Leaf, size_t N = 0, typename Slot = Leaf>
class PhraseSlots {
  static constexpr bool kVariadic = !std::is_same_v<Slot, Leaf>;
  static constexpr bool kRepeats = !kVariadic && N == 0;
  static constexpr bool kDeferrable =
    !kVariadic && requires(Leaf& leaf) { leaf.Defer(); };

 public:
  template<typename... Args>
  PhraseSlots(const PhraseTerms& terms, std::span<const TermInterval> intervals,
              const IndexInput& doc_in, IndexFeatures layout,
              const IndexInput& pos_in, const IndexInput* pay_in,
              Args&&... args)
    : _matcher{terms.metas.size(), std::forward<Args>(args)...},
      _repeats{terms.repeats.size()},
      _leaves{terms.leaves,
              [&](uint32_t j) { return terms.metas[j].docs_count; },
              [&](uint32_t j) {
                return std::forward_as_tuple(terms.metas[j], doc_in, layout,
                                             pos_in, pay_in);
              },
              [&](uint32_t j, Leaf& leaf) {
                _matcher.Position(j) = {&leaf.Positions(), intervals[j]};
                if constexpr (kRepeats) {
                  for (size_t i = 0; i != terms.repeats.size(); ++i) {
                    const auto [slot, first, rank] = terms.repeats[i];
                    if (first != j) {
                      continue;
                    }
                    auto& repeat = _repeats[i];
                    leaf.PreparePositions(repeat.pos, terms.metas[slot], pos_in,
                                          pay_in);
                    repeat.lead = &leaf.Positions();
                    repeat.rank = rank;
                    _matcher.Position(slot) = {&repeat.pos, intervals[slot]};
                  }
                }
              }} {
    static_assert(!kVariadic);
    SDB_ASSERT(terms.metas.size() == intervals.size());
    SDB_ASSERT(terms.metas.size() > 1);
    SDB_ASSERT(kRepeats || terms.repeats.empty());
    _matcher.Finish();
    if constexpr (kDeferrable) {
      const uint64_t lead = _leaves.Lead().Estimate();
      for (size_t i = _leaves.Size(); i-- != 1;) {
        auto& leaf = _leaves.begin()[i];
        if (terms.Defers(lead, leaf.Estimate())) {
          leaf.Defer();
          _leaves.DeferFrom(i);
        }
      }
    }
  }

  template<typename... Args>
  PhraseSlots(std::span<const PostingMeta> metas,
              std::span<const uint32_t> offsets,
              std::span<const score_t> boosts,
              std::span<const TermInterval> intervals, const IndexInput& doc_in,
              IndexFeatures layout, const IndexInput& pos_in,
              const IndexInput* pay_in, Args&&... args)
    : _terms{metas.size(), std::piecewise_construct,
             [&](size_t i) {
               return std::forward_as_tuple(metas[i], doc_in, layout, pos_in,
                                            pay_in);
             }},
      _matcher{intervals.size(), std::forward<Args>(args)...},
      _leaves{intervals.size(),
              [&](uint32_t j) {
                uint64_t estimate = 0;
                for (auto i = offsets[j]; i != offsets[j + 1]; ++i) {
                  estimate += metas[i].docs_count;
                }
                return estimate;
              },
              [&](uint32_t j) {
                const auto offset = offsets[j];
                return std::tuple<Leaf*, uint32_t, const score_t*>{
                  _terms.data() + offset, offsets[j + 1] - offset,
                  boosts.empty() ? nullptr : boosts.data() + offset};
              },
              [&](uint32_t j, Slot& slot) {
                _matcher.Position(j) = {&slot.Positions(), intervals[j]};
              }} {
    static_assert(kVariadic);
    SDB_ASSERT(offsets.size() == intervals.size() + 1);
    SDB_ASSERT(offsets.back() == metas.size());
    SDB_ASSERT(boosts.empty() == !Slot::Position::kHasBoost);
    SDB_ASSERT(boosts.empty() || boosts.size() == metas.size());
    SDB_ASSERT(intervals.size() > 1 || metas.size() > 1);
    _matcher.Finish();
    for (auto& slot : _leaves) {
      if (auto* leaf = slot.Single()) {
        _bound = leaf;
        break;
      }
    }
  }

  PhraseSlots(PhraseSlots&&) = delete;
  PhraseSlots& operator=(PhraseSlots&&) = delete;

  doc_id_t Seek(doc_id_t from) { return _leaves.Seek(from); }

  doc_id_t Next(doc_id_t) { return _leaves.Next(); }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaves.Probe(target);
  }

  bool Match(doc_id_t) { return Collect() && _matcher.Match(); }

  static constexpr bool kHasFreqBound =
    requires(Matcher& m) { m.DocFreqBound(); };

  uint32_t FreqBound()
    requires(kHasFreqBound)
  {
    return Collect() ? _matcher.DocFreqBound() : 0;
  }

  template<typename Gates>
  void Gate(Gates&& gates) {
    const auto lead = EstimateOf(LeavesOf(_leaves.Lead()));
    for (size_t i = 1; i != _leaves.Size(); ++i) {
      const auto leaves = LeavesOf(_leaves.begin()[i]);
      if (!gates(lead, leaves)) {
        continue;
      }
      const auto docs = EstimateOf(leaves);
      _gates[i].share = static_cast<double>(lead) * doc_limits::kBlockSize /
                        static_cast<double>(std::max<uint64_t>(docs, 1));
      _gating = true;
    }
  }

  bool Gating() const noexcept { return _gating; }

  template<typename Admit>
  doc_id_t Next(doc_id_t, doc_id_t end, Admit&& admit)
    requires(kHasFreqBound)
  {
    return _leaves.Next(end, [&](doc_id_t candidate, Slot* cold) {
      auto& gate = _gates[static_cast<size_t>(cold - _leaves.begin())];
      if (gate.share == 0) {
        return true;
      }
      if (gate.skip != 0) {
        --gate.skip;
        return true;
      }
      auto* slot = _leaves.begin();
      auto freq = DocFreqOf(*slot);
      while (++slot != cold) {
        freq = std::min(freq, DocFreqOf(*slot));
      }
      const bool admitted = admit(candidate, _matcher.FreqBoundOf(freq));
      gate.Observe(admitted);
      return admitted;
    });
  }

  bool MatchOrdered(doc_id_t)
    requires(kHasFreqBound)
  {
    return _matcher.template Match<true>();
  }

  uint32_t FreqBoundOf(uint32_t freq) const noexcept
    requires(kHasFreqBound)
  {
    return _matcher.FreqBoundOf(freq);
  }

  Leaf* BoundLeaf() noexcept {
    if constexpr (kVariadic) {
      return _bound;
    } else {
      return &_leaves.Lead();
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

  score_t Scale() const noexcept
    requires(Matcher::kHasScale)
  {
    return _matcher.GetScale();
  }

 private:
  using Leaves = ConjunctionLeaves<Slot, N>;

  static constexpr uint32_t kGateWindow = 64;

  struct SlotGate {
    void Observe(bool admitted) noexcept {
      admits += static_cast<uint32_t>(admitted);
      if (++checks != kGateWindow) {
        return;
      }
      const auto passes = admits * share;
      const bool read = passes >= checks;
      if (read && read_before) {
        skip = static_cast<uint32_t>(passes);
      }
      read_before = read;
      checks = 0;
      admits = 0;
    }

    double share = 0;
    uint32_t checks = 0;
    uint32_t admits = 0;
    uint32_t skip = 0;
    bool read_before = false;
  };

  static std::span<Leaf> LeavesOf(Slot& slot) noexcept {
    if constexpr (kVariadic) {
      return slot.Leaves();
    } else {
      return {&slot, 1};
    }
  }

  static uint64_t EstimateOf(std::span<Leaf> leaves) {
    uint64_t docs = 0;
    for (const auto& leaf : leaves) {
      docs += leaf.Estimate();
    }
    return docs;
  }

  static uint32_t DocFreqOf(Slot& slot) {
    if constexpr (kVariadic) {
      slot.OnMatch();
    } else if constexpr (kDeferrable) {
      slot.Sync();
    }
    return slot.Positions().DocFreq();
  }

  bool Collect() {
    if constexpr (kVariadic) {
      for (auto& slot : _leaves) {
        slot.OnMatch();
      }
    } else {
      if constexpr (kDeferrable) {
        for (auto* leaf = _leaves.Deferred(); leaf != _leaves.end(); ++leaf) {
          leaf->Sync();
        }
      }
      if constexpr (kRepeats) {
        if (!_repeats.empty()) {
          return FollowRepeats();
        }
      }
    }
    return true;
  }

  IRS_NO_INLINE bool FollowRepeats() noexcept {
    bool enough = true;
    for (auto& repeat : _repeats) {
      repeat.pos.Follow(*repeat.lead);
      enough &= repeat.lead->DocFreq() > repeat.rank;
    }
    return enough;
  }

  struct Repeat {
    typename Leaf::Position pos;
    const typename Leaf::Position* lead = nullptr;
    uint32_t rank = 0;
  };

  [[no_unique_address]] utils::Need<kVariadic, containers::Fixed<Leaf>> _terms;
  Matcher _matcher;
  [[no_unique_address]] utils::Need<kRepeats, containers::Fixed<Repeat>>
    _repeats;
  Leaves _leaves;
  [[no_unique_address]] utils::Need<kVariadic, Leaf*> _bound{};
  containers::Fixed<SlotGate, N> _gates{_leaves.Size()};
  bool _gating = false;
};

}  // namespace irs::detail
