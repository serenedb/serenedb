#include "automaton_combinators.hpp"

#include "iresearch/utils/automaton_utils.hpp"

// clang-format off
#include <fst/arcsort.h>
#include <fst/union.h>
#include "fstext/determinize-star.h"
// clang-format on

#include <algorithm>
#include <vector>

namespace irs {
namespace {

std::vector<uint16_t> CollectBoundaries(const automaton& a) {
  using StateId = automaton::StateId;
  using Arc = automaton::Arc;

  std::vector<uint16_t> b;
  b.push_back(0x00);

  for (StateId s = 0; s < a.NumStates(); ++s) {
    fst::ArcIteratorData<Arc> arcs;
    a.InitArcIterator(s, &arcs);
    for (size_t i = 0; i < arcs.narcs; ++i) {
      const RangeLabel r{arcs.arcs[i].ilabel};
      b.push_back(r.min);
      if (r.max < 0xFF) {
        b.push_back(static_cast<uint16_t>(r.max + 1));
      }
    }
  }

  b.push_back(0x100);
  std::sort(b.begin(), b.end());
  b.erase(std::unique(b.begin(), b.end()), b.end());
  return b;
}

automaton RefineAutomaton(const automaton& src,
                          const std::vector<uint16_t>& boundaries) {
  SDB_ASSERT(!boundaries.empty());
  SDB_ASSERT(boundaries.front() == 0x00);
  SDB_ASSERT(boundaries.back() == 0x100);

  using StateId = automaton::StateId;
  using Arc = automaton::Arc;

  automaton result;
  for (StateId s = 0; s < src.NumStates(); ++s) {
    result.AddState();
    result.SetFinal(s, src.Final(s));
  }
  if (src.Start() != fst::kNoStateId) {
    result.SetStart(src.Start());
  }

  for (StateId s = 0; s < src.NumStates(); ++s) {
    fst::ArcIteratorData<Arc> arcs;
    src.InitArcIterator(s, &arcs);

    for (size_t i = 0; i < arcs.narcs; ++i) {
      const RangeLabel r{arcs.arcs[i].ilabel};
      const StateId target = arcs.arcs[i].nextstate;
      auto it = std::lower_bound(boundaries.begin(), boundaries.end(), r.min);

      for (; it != boundaries.end() - 1; ++it) {
        if (static_cast<uint32_t>(*(it + 1)) >
            static_cast<uint32_t>(r.max) + 1u) {
          break;
        }
        result.EmplaceArc(
          s, RangeLabel::From(*it, static_cast<uint16_t>(*(it + 1) - 1)),
          target);
      }
    }
  }

  fst::ArcSort(&result, fst::ILabelCompare<Arc>());
  return result;
}

automaton ComplementAutomaton(const automaton& a) {
  using StateId = automaton::StateId;
  using Arc = automaton::Arc;

  automaton result;
  result.ReserveStates(a.NumStates() + 1);

  for (StateId s = 0; s < a.NumStates(); ++s) {
    result.AddState();
    const auto w = a.Final(s);
    result.SetFinal(s,
                    w ? automaton::Weight::Zero() : automaton::Weight::One());
  }

  const StateId dead = result.AddState();
  result.SetFinal(dead, automaton::Weight::One());
  result.EmplaceArc(dead, RangeLabel::From(0x00, 0xFF), dead);

  if (a.Start() != fst::kNoStateId) {
    result.SetStart(a.Start());
  }

  for (StateId s = 0; s < a.NumStates(); ++s) {
    fst::ArcIteratorData<Arc> arcs;
    a.InitArcIterator(s, &arcs);

    uint32_t next_min = 0x00;
    for (size_t i = 0; i < arcs.narcs; ++i) {
      const RangeLabel r{arcs.arcs[i].ilabel};
      if (next_min < static_cast<uint32_t>(r.min)) {
        result.EmplaceArc(s,
                          RangeLabel::From(static_cast<uint16_t>(next_min),
                                           static_cast<uint16_t>(r.min - 1)),
                          dead);
      }
      result.EmplaceArc(s, r, arcs.arcs[i].nextstate);
      next_min = static_cast<uint32_t>(r.max) + 1u;
    }
    if (next_min <= 0xFF) {
      result.EmplaceArc(
        s, RangeLabel::From(static_cast<uint16_t>(next_min), 0xFF), dead);
    }
  }

  fst::ArcSort(&result, fst::ILabelCompare<Arc>());
  return result;
}

}  // namespace

automaton IntersectAutomatons(const automaton& a1, const automaton& a2) {
  using StateId = automaton::StateId;
  using Arc = automaton::Arc;

  automaton result;

  if (a1.Start() == fst::kNoStateId || a2.Start() == fst::kNoStateId) {
    return result;
  }

  absl::flat_hash_map<std::pair<StateId, StateId>, StateId> state_map;
  std::vector<std::pair<StateId, StateId>> worklist;

  auto get_or_create = [&](StateId q1, StateId q2) -> StateId {
    auto [it, inserted] =
      state_map.try_emplace({q1, q2}, static_cast<StateId>(result.NumStates()));
    if (inserted) {
      result.AddState();
      const bool both_final = a1.Final(q1) && a2.Final(q2);
      result.SetFinal(it->second, both_final ? automaton::Weight::One()
                                             : automaton::Weight::Zero());
      worklist.push_back({q1, q2});
    }
    return it->second;
  };

  result.SetStart(get_or_create(a1.Start(), a2.Start()));

  while (!worklist.empty()) {
    auto [q1, q2] = worklist.back();
    worklist.pop_back();
    const StateId src = state_map.at({q1, q2});

    fst::ArcIteratorData<Arc> arcs1, arcs2;
    a1.InitArcIterator(q1, &arcs1);
    a2.InitArcIterator(q2, &arcs2);

    for (size_t i = 0; i < arcs1.narcs; ++i) {
      const RangeLabel r1{arcs1.arcs[i].ilabel};
      for (size_t j = 0; j < arcs2.narcs; ++j) {
        const RangeLabel r2{arcs2.arcs[j].ilabel};
        const uint16_t lo = std::max(r1.min, r2.min);
        const uint16_t hi = std::min(r1.max, r2.max);
        if (lo > hi) {
          continue;
        }
        const StateId dst =
          get_or_create(arcs1.arcs[i].nextstate, arcs2.arcs[j].nextstate);
        result.EmplaceArc(src, RangeLabel::From(lo, hi), dst);
      }
    }
  }

  fst::ArcSort(&result, fst::ILabelCompare<Arc>());
  return result;
}

automaton UnionAutomatons(const automaton& a1, const automaton& a2) {
  auto b1 = CollectBoundaries(a1);
  auto b2 = CollectBoundaries(a2);

  std::vector<uint16_t> boundaries;
  boundaries.reserve(b1.size() + b2.size());
  std::merge(b1.begin(), b1.end(), b2.begin(), b2.end(),
             std::back_inserter(boundaries));
  boundaries.erase(std::unique(boundaries.begin(), boundaries.end()),
                   boundaries.end());

  auto refined1 = RefineAutomaton(a1, boundaries);
  auto refined2 = RefineAutomaton(a2, boundaries);

  fst::Union(&refined1, refined2);

  automaton dfa;
  if (fst::DeterminizeStar(refined1, &dfa)) {
    return {};
  }

  fst::ArcSort(&dfa, fst::ILabelCompare<automaton::Arc>());
  return dfa;
}

automaton UnionAutomatonsDeMorgan(const automaton& a1, const automaton& a2) {
  const auto not_a1 = ComplementAutomaton(a1);
  const auto not_a2 = ComplementAutomaton(a2);
  return ComplementAutomaton(IntersectAutomatons(not_a1, not_a2));
}

}  // namespace irs
