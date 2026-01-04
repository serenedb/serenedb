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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/limited_sample_collector.hpp"
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

Filter::Query::ptr PrepareAutomatonFilter(const PrepareContext& ctx,
                                          std::string_view field,
                                          const automaton& acceptor,
                                          size_t scored_terms_limit) {
  auto matcher = MakeAutomatonMatcher(acceptor);

  if (fst::kError == matcher.Properties(0)) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Expected deterministic, epsilon-free acceptor, got the "
                   "following properties ",
                   matcher.GetFst().Properties(
                     automaton_table_matcher::kFstProperties, false)));

    return Filter::Query::empty();
  }

  // object for collecting order stats
  LimitedSampleCollector<TermFrequency> collector(
    ctx.scorers.empty() ? 0 : scored_terms_limit);
  MultiTermQuery::States states{ctx.memory, ctx.index.size()};
  MultiTermVisitor mtv{collector, states};

  for (const auto& segment : ctx.index) {
    if (const auto* reader = segment.field(field); reader) {
      Visit(segment, *reader, matcher, mtv);
    }
  }

  MultiTermQuery::Stats stats{{ctx.memory}};
  collector.score(ctx.index, ctx.scorers, stats);

  return memory::make_tracked<MultiTermQuery>(ctx.memory, std::move(states),
                                              std::move(stats), ctx.boost,
                                              ScoreMergeType::Sum, size_t{1});
}

}  // namespace irs
