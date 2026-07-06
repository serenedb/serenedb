////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/utils/automaton_utils.hpp"

#include "fst/arcsort.h"
#include "fst/union.h"
#include "fstext/determinize-star.h"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

void Utf8EmplaceArc(automaton& a, automaton::StateId from, bytes_view label,
                    automaton::StateId to) {
  switch (label.size()) {
    case 1: {
      a.EmplaceArc(from, RangeLabel::From(label[0]), to);
      return;
    }
    case 2: {
      const auto s0 = a.AddState();
      a.EmplaceArc(from, RangeLabel::From(label[0]), s0);
      a.EmplaceArc(s0, RangeLabel::From(label[1]), to);
      return;
    }
    case 3: {
      const auto s0 = a.AddState();
      const auto s1 = a.AddState();
      a.EmplaceArc(from, RangeLabel::From(label[0]), s0);
      a.EmplaceArc(s0, RangeLabel::From(label[1]), s1);
      a.EmplaceArc(s1, RangeLabel::From(label[2]), to);
      return;
    }
    case 4: {
      const auto s0 = a.NumStates();
      const auto s1 = s0 + 1;
      const auto s2 = s0 + 2;
      a.AddStates(3);
      a.EmplaceArc(from, RangeLabel::From(label[0]), s0);
      a.EmplaceArc(s0, RangeLabel::From(label[1]), s1);
      a.EmplaceArc(s1, RangeLabel::From(label[2]), s2);
      a.EmplaceArc(s2, RangeLabel::From(label[3]), to);
      return;
    }
  }
}

void Utf8EmplaceRhoArc(automaton& a, automaton::StateId from,
                       automaton::StateId to) {
  const auto id = a.NumStates();  // stated ids are sequential
  a.AddStates(3);

  // add rho transitions
  a.ReserveArcs(from, 4);
  a.EmplaceArc(from, kUTF8Byte1, to);
  a.EmplaceArc(from, kUTF8Byte2, id);
  a.EmplaceArc(from, kUTF8Byte3, id + 1);
  a.EmplaceArc(from, kUTF8Byte4, id + 2);

  // connect intermediate states of default multi-byte UTF8 sequence
  a.EmplaceArc(id, kRhoLabel, to);
  a.EmplaceArc(id + 1, kRhoLabel, id);
  a.EmplaceArc(id + 2, kRhoLabel, id + 1);
}

void Utf8TransitionsBuilder::Minimize(automaton& a, size_t prefix) {
  SDB_ASSERT(prefix > 0);

  for (size_t i = _last.size(); i >= prefix; --i) {
    auto& s = _states[i];
    auto& p = _states[i - 1];
    SDB_ASSERT(!p.arcs.empty());

    if (s.id == fst::kNoStateId) {
      // here we deal with rho transition only for
      // intermediate states, i.e. char range is [128;191]
      const size_t rho_idx = _last.size() - i - 1;
      SDB_ASSERT(rho_idx < std::size(_rho_states));
      s.AddRhoArc(kRhoLabel.min, kRhoLabel.max + 1, _rho_states[rho_idx]);
    }

    p.arcs.back().id = _states_map.insert(s, a);  // finalize state

    s.Clear();
  }
}

void Utf8TransitionsBuilder::Insert(automaton& a, const byte_type* label,
                                    size_t size, automaton::StateId to) {
  SDB_ASSERT(label);
  SDB_ASSERT(size < 5);

  const size_t prefix = 1 + CommonPrefixLength(_last, {label, size});
  Minimize(a, prefix);  // minimize suffix

  // add current word suffix
  for (size_t i = prefix; i <= size; ++i) {
    const auto ch = label[i - 1];
    auto& p = _states[i - 1];
    // root state is already a part of automaton
    SDB_ASSERT(i == 1 || p.id == fst::kNoStateId);

    if (p.id == fst::kNoStateId) {
      // here we deal with rho transition only for
      // intermediate states, i.e. char range is [128;191]
      p.AddRhoArc(128, ch, _rho_states[size - i]);
    }

    p.arcs.emplace_back(RangeLabel::From(ch), &_states[i]);
  }

  const bool is_final = _last.size() != size || prefix != (size + 1);

  if (is_final) {
    _states[size].id = to;
  }
}

void Utf8TransitionsBuilder::Finish(automaton& a, automaton::StateId from) {
#ifdef SDB_DEV
  Finally ensure_empty = [&]() noexcept {
    // ensure everything is cleaned up
    SDB_ASSERT(std::all_of(std::begin(_states), std::end(_states),
                           [](const State& s) noexcept {
                             return s.arcs.empty() && s.id == fst::kNoStateId;
                           }));
  };
#endif

  auto& root = _states[0];
  Minimize(a, 1);

  if (fst::kNoStateId == _rho_states[0]) {
    // no default state: just add transitions from the
    // root node to its successors
    for (const auto& arc : root.arcs) {
      a.EmplaceArc(from, arc.ilabel, arc.id);
    }

    root.Clear();

    return;
  }

  // in presence of default state we have to add some extra
  // transitions from root to properly handle multi-byte sequences
  // and preserve correctness of arcs order

  // reserve some memory to store all outbound transitions
  a.ReserveArcs(from, root.arcs.size());

  auto add_arcs = [&a, from, arc = root.arcs.begin(), end = root.arcs.end()](
                    uint32_t min, uint32_t max,
                    automaton::StateId rho_state) mutable {
    SDB_ASSERT(min < max);

    for (; arc != end && arc->max <= max; ++arc) {
      // ensure arcs are sorted
      SDB_ASSERT(min <= arc->min);
      // ensure every arc denotes a single char, otherwise
      // we have to use "arc->min" below which is a bit more
      // expensive to access
      SDB_ASSERT(arc->min == arc->max);

      if (min < arc->max) {
        a.EmplaceArc(from, RangeLabel::From(min, arc->max - 1), rho_state);
      }

      a.EmplaceArc(from, arc->ilabel, arc->id);
      min = arc->max + 1;
    }

    if (min < max) {
      a.EmplaceArc(from, RangeLabel::From(min, max), rho_state);
    }
  };

  add_arcs(kUTF8Byte1.min, kUTF8Byte1.max, _rho_states[0]);
  add_arcs(kUTF8Byte2.min, kUTF8Byte2.max, _rho_states[1]);
  add_arcs(kUTF8Byte3.min, kUTF8Byte3.max, _rho_states[2]);
  add_arcs(kUTF8Byte4.min, kUTF8Byte4.max, _rho_states[3]);

  root.Clear();

  // connect intermediate states of default multi-byte UTF8 sequence
  a.EmplaceArc(_rho_states[1], kRhoLabel, _rho_states[0]);
  a.EmplaceArc(_rho_states[2], kRhoLabel, _rho_states[1]);
  a.EmplaceArc(_rho_states[3], kRhoLabel, _rho_states[2]);
}

QueryBuilder::ptr PrepareAutomatonSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field,
  const automaton_table_matcher& matcher, score_t boost) {
  if (fst::kError == matcher.Properties(0)) {
    SDB_ERROR(
      IRESEARCH,
      absl::StrCat("Expected deterministic, epsilon-free acceptor, got the "
                   "following properties ",
                   matcher.GetFst().Properties(
                     automaton_table_matcher::kFstProperties, false)));

    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, ScoreMergeType::Sum,
    size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto* collector =
    ctx.collector
      ? &sdb::basics::downCast<LimitedTermsCollector>(*ctx.collector)
      : nullptr;
  if (collector) {
    collector->Field().Collect(*reader);
  }
  SampledMultiTermVisitor mtv{collector ? &collector->Limited() : nullptr,
                              query->State()};
  Visit(segment, *reader, matcher, mtv);
  return query;
}

std::optional<automaton> IntersectAcceptors(const automaton& lhs,
                                            const automaton& rhs,
                                            size_t state_budget) {
  if (fst::kNoStateId == lhs.Start() || fst::kNoStateId == rhs.Start()) {
    return std::nullopt;
  }
  automaton out;
  absl::flat_hash_map<uint64_t, automaton::StateId> ids;
  std::vector<std::pair<automaton::StateId, automaton::StateId>> pending;
  const auto key = [](automaton::StateId l, automaton::StateId r) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(l)) << 32) |
           static_cast<uint32_t>(r);
  };
  const auto product_state = [&](automaton::StateId l, automaton::StateId r) {
    const auto [it, added] = ids.try_emplace(key(l, r), 0);
    if (added) {
      it->second = out.AddState();
      if (lhs.Final(l) && rhs.Final(r)) {
        out.SetFinal(it->second, true);
      }
      pending.emplace_back(l, r);
    }
    return it->second;
  };
  out.SetStart(product_state(lhs.Start(), rhs.Start()));
  while (!pending.empty()) {
    const auto [l, r] = pending.back();
    pending.pop_back();
    const auto from = ids.at(key(l, r));
    for (fst::ArcIterator lit{lhs, l}; !lit.Done(); lit.Next()) {
      const auto& la = lit.Value();
      if (la.ilabel == 0) {
        return std::nullopt;
      }
      for (fst::ArcIterator rit{rhs, r}; !rit.Done(); rit.Next()) {
        const auto& ra = rit.Value();
        if (ra.ilabel == 0) {
          return std::nullopt;
        }
        const auto lo = std::max(la.min, ra.min);
        const auto hi = std::min(la.max, ra.max);
        if (lo > hi) {
          continue;
        }
        out.EmplaceArc(from, RangeLabel::From(lo, hi),
                       product_state(la.nextstate, ra.nextstate));
        if (ids.size() > state_budget) {
          return std::nullopt;
        }
      }
    }
  }
  return out;
}

automaton MakeRangeAcceptor(bytes_view min, bytes_view max, bool min_inclusive,
                            bool max_inclusive) {
  const bool has_min = !IsNull(min);
  const bool has_max = !IsNull(max);

  const auto empty = [] {
    automaton a;
    a.SetStart(a.AddState());
    return a;
  };
  const auto all = [] {
    automaton a;
    const auto s = a.AddState();
    a.SetStart(s);
    a.SetFinal(s, true);
    a.EmplaceArc(s, RangeLabel::From(0, 255), s);
    return a;
  };

  const auto greater_or_equal = [&](bytes_view bound, bool inclusive) {
    automaton a;
    const auto free = a.AddState();
    a.SetFinal(free, true);
    a.EmplaceArc(free, RangeLabel::From(0, 255), free);
    auto state = a.AddState();
    a.SetStart(state);
    for (const auto byte : bound) {
      const auto next = a.AddState();
      a.EmplaceArc(state, RangeLabel::From(byte), next);
      if (byte != 255) {
        a.EmplaceArc(state, RangeLabel::From(byte + 1, 255), free);
      }
      state = next;
    }
    a.SetFinal(state, inclusive);
    a.EmplaceArc(state, RangeLabel::From(0, 255), free);
    return a;
  };
  const auto less_or_equal = [&](bytes_view bound, bool inclusive) {
    automaton a;
    const auto free = a.AddState();
    a.SetFinal(free, true);
    a.EmplaceArc(free, RangeLabel::From(0, 255), free);
    auto state = a.AddState();
    a.SetStart(state);
    for (const auto byte : bound) {
      a.SetFinal(state, true);
      const auto next = a.AddState();
      if (byte != 0) {
        a.EmplaceArc(state, RangeLabel::From(0, byte - 1), free);
      }
      a.EmplaceArc(state, RangeLabel::From(byte), next);
      state = next;
    }
    a.SetFinal(state, inclusive);
    return a;
  };

  if (!has_min && !has_max) {
    return all();
  }
  if (!has_min) {
    return less_or_equal(max, max_inclusive);
  }
  if (!has_max) {
    return greater_or_equal(min, min_inclusive);
  }
  auto product = IntersectAcceptors(greater_or_equal(min, min_inclusive),
                                    less_or_equal(max, max_inclusive),
                                    2 * (min.size() + max.size()) + 8);
  return product ? std::move(*product) : empty();
}

}  // namespace irs
