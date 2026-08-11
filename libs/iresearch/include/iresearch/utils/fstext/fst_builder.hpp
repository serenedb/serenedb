////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <fst/vector-fst.h>

#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/fstext/fst_states_map.hpp"
#include "iresearch/utils/fstext/fst_string_weight.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct FstStats {
  size_t num_states{};  // total number of states
  size_t num_arcs{};    // total number of arcs

  template<typename Weight>
  void operator()(const Weight&) noexcept {}
};

/// helper class for building minimal acyclic subsequential transducers
/// algorithm is described there:
/// http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
template<typename Char, typename Fst, typename Stats = FstStats>
class FstBuilder : util::Noncopyable {
 public:
  typedef Fst fst_t;
  typedef Char char_t;
  typedef irs::basic_string_view<char_t> key_t;
  typedef Stats stats_t;
  typedef typename fst_t::Weight weight_t;
  typedef typename fst_t::Arc arc_t;
  typedef typename fst_t::StateId stateid_t;
  typedef typename arc_t::Label label_t;

  static constexpr stateid_t kFinal = 0;

  explicit FstBuilder(fst_t& fst)
    : _states_map(16, StateEmplace(_stats)), _fst(fst) {
    reset();
  }

  void add(const key_t& in, const weight_t& out) {
    // inputs should be sorted
    SDB_ASSERT(_last.empty() || _last < in);

    if (in.empty()) {
      _start_out = fst::Times(_start_out, out);
      return;
    }

    const auto size = in.size();

    // determine common prefix
    const size_t pref = 1 + CommonPrefixLength(_last, in);

    // add states for current input
    add_states(size);

    // minimize last word suffix
    minimize(pref);

    // add current word suffix
    for (size_t i = pref; i <= size; ++i) {
      _states[i - 1].arcs.emplace_back(in[i - 1], &_states[i]);
    }

    const bool is_final = _last.size() != size || pref != (size + 1);

    decltype(fst::DivideLeft(out, out)) output = out;

    for (size_t i = 1; i < pref; ++i) {
      State& s = _states[i];
      State& p = _states[i - 1];

      SDB_ASSERT(!p.arcs.empty() && p.arcs.back().label == in[i - 1]);

      auto& last_out = p.arcs.back().out;

      if (last_out != weight_t::One()) {
        auto prefix = fst::Plus(last_out, output);
        const auto suffix = fst::DivideLeft(last_out, prefix);
        output = fst::DivideLeft(output, prefix);

        for (Arc& a : s.arcs) {
          a.out = fst::Times(suffix, a.out);
        }

        if (s.final) {
          s.out = fst::Times(suffix, s.out);
        }

        if constexpr (std::is_same_v<decltype(prefix), irs::bytes_view>) {
          last_out.Resize(prefix.size());
        } else {
          last_out = std::move(prefix);
        }
      }
    }

    if (is_final) {
      // set final state
      {
        State& s = _states[size];
        s.final = true;
      }

      // set output
      {
        State& s = _states[pref - 1];
        SDB_ASSERT(!s.arcs.empty() && s.arcs.back().label == in[pref - 1]);
        s.arcs.back().out = std::move(output);
      }
    } else {
      State& s = _states[size];
      SDB_ASSERT(s.arcs.size());
      SDB_ASSERT(s.arcs.back().label == in[pref - 1]);
      s.arcs.back().out = fst::Times(s.arcs.back().out, output);
    }

    _last = in;
  }

  stats_t finish() {
    stateid_t start = FstBuilder::kFinal;

    if (!_states.empty()) {
      // minimize last word suffix
      minimize(1);

      auto& root = _states[0];

      if (!root.arcs.empty() || !root.final) {
        start = _states_map.insert(root, _fst);
      }
    }

    // set the start state
    _fst.SetStart(start);
    _fst.SetFinal(start, _start_out);

    // count start state
    _stats(_start_out);

    return _stats;
  }

  void reset() {
    // remove states
    _fst.DeleteStates();

    // initialize final state
    _fst.AddState();
    _fst.SetFinal(kFinal, weight_t::One());

    // reset stats
    _stats = {};
    _stats.num_states = 1;
    _stats.num_arcs = 0;
    _stats(weight_t::One());

    _states.clear();
    _states_map.reset();
    _last = {};
    _start_out = weight_t{};
  }

 private:
  struct State;

  struct Arc : private util::Noncopyable {
    Arc(label_t label, State* target) : target(target), label(label) {}

    Arc(Arc&& rhs) noexcept
      : target(rhs.target), label(rhs.label), out(std::move(rhs.out)) {}

    bool operator==(const arc_t& rhs) const noexcept {
      return label == rhs.ilabel && id == rhs.nextstate && out == rhs.weight;
    }

    union {
      State* target;
      stateid_t id;
    };
    label_t label;
    weight_t out{weight_t::One()};
  };

  struct State : private util::Noncopyable {
    explicit State(bool final = false) : final(final) {}

    State(State&& rhs) noexcept
      : arcs(std::move(rhs.arcs)), out(std::move(rhs.out)), final(rhs.final) {}

    void clear() noexcept {
      arcs.clear();
      out = weight_t::One();
      final = false;
    }

    template<typename H>
    friend H AbslHashValue(H h, const State& state) {
      for (auto& arc : state.arcs) {
        h = H::combine(std::move(h), arc.label, arc.id, arc.out.Impl());
      }
      return std::move(h);
    }

    std::vector<Arc> arcs;
    weight_t out{weight_t::One()};
    bool final{false};
  };

  static_assert(std::is_nothrow_move_constructible_v<State>);

  struct StateEqual {
    bool operator()(const State& lhs, stateid_t rhs, const fst_t& fst) const {
      if (lhs.arcs.size() != fst.NumArcs(rhs)) {
        return false;
      }

      fst::ArcIterator<fst_t> rhs_arc(fst, rhs);

      for (auto& lhs_arc : lhs.arcs) {
        if (lhs_arc != rhs_arc.Value()) {
          return false;
        }

        rhs_arc.Next();
      }

      SDB_ASSERT(rhs_arc.Done());
      return true;
    }
  };

  struct StateHash {
    size_t operator()(const State& s, const fst_t& /*fst*/) const noexcept {
      return absl::HashOf(s);
    }

    struct Impl {
      const fst_t& fst;
      stateid_t id;

      template<typename H>
      friend H AbslHashValue(H h, const Impl& impl) {
        for (fst::ArcIterator<fst_t> it{impl.fst, impl.id}; !it.Done();
             it.Next()) {
          const arc_t& arc = it.Value();
          h = H::combine(std::move(h), arc.ilabel, arc.nextstate,
                         arc.weight.Impl());
        }
        return std::move(h);
      }
    };

    size_t operator()(stateid_t id, const fst_t& fst) const noexcept {
      return absl::HashOf(Impl{fst, id});
    }
  };

  class StateEmplace {
   public:
    explicit StateEmplace(stats_t& stats) noexcept : _stats(&stats) {}

    stateid_t operator()(const State& s, fst_t& fst) const {
      const stateid_t id = fst.AddState();

      if (s.final) {
        fst.SetFinal(id, s.out);
        (*_stats)(s.out);
      }

      for (const Arc& a : s.arcs) {
        fst.EmplaceArc(id, a.label, a.label, a.out, a.id);
        (*_stats)(a.out);
      }

      ++_stats->num_states;
      _stats->num_arcs += s.arcs.size();

      return id;
    }

   private:
    stats_t* _stats;
  };

  using states_map = FstStatesMap<fst_t, State, StateEmplace, StateHash,
                                  StateEqual, fst::kNoStateId>;

  void add_states(size_t size) {
    // reserve size + 1 for root state
    if (_states.size() < ++size) {
      _states.resize(size);
    }
  }

  void minimize(size_t pref) {
    SDB_ASSERT(pref > 0);

    for (size_t i = _last.size(); i >= pref; --i) {
      State& s = _states[i];
      State& p = _states[i - 1];

      SDB_ASSERT(!p.arcs.empty());
      p.arcs.back().id = s.arcs.empty() && s.final
                           ? FstBuilder::kFinal
                           : _states_map.insert(s, _fst);

      s.clear();
    }
  }

  stats_t _stats;
  states_map _states_map;
  std::vector<State> _states;  // current states
  weight_t _start_out;         // output for "empty" input
  key_t _last;
  fst_t& _fst;
};

}  // namespace irs
