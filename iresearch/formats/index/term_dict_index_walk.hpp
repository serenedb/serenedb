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

#include <array>
#include <cstring>
#include <limits>
#include <span>

#include "iresearch/formats/index/term_dict_index.hpp"
#include "iresearch/utils/byte_set.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kDeadNode = kNoNode - 1;

struct LabelRange {
  uint32_t lo;
  uint32_t hi;
};

template<typename A>
LabelRange LiveRangeOf(const A& a, const typename A::State& state) {
  LabelRange range{.lo = 1, .hi = 0};
  a.LiveRange(state, range.lo, range.hi);
  return range;
}

inline constexpr size_t kMaxGuess = 4;
inline constexpr size_t kDeadEnd = std::numeric_limits<size_t>::max();

using Tail = std::array<byte_type, kMaxGuess>;

inline bool Gated(const PrefixTrie* trie, uint32_t node) noexcept {
  return trie && node < kDeadNode && !trie->Open(node);
}

inline uint32_t ChildNode(const PrefixTrie* trie, uint32_t node,
                          byte_type label) noexcept {
  if (!trie || node >= kDeadNode) {
    return node;
  }
  if (trie->Open(node)) {
    return kNoNode;
  }
  const auto child = trie->ChildOf(node, label);
  return child == kNoNode ? kDeadNode : child;
}

template<typename A>
struct Live {
  uint32_t label;
  typename A::State next;
  uint32_t node;
};

class ChildLabels {
 public:
  ChildLabels(const PrefixTrie* trie, uint32_t node, uint32_t from) noexcept
    : _gated{Gated(trie, node)} {
    if (_gated) {
      _children = trie->ChildrenOf(node, from);
    } else if (node != kDeadNode) {
      _label = from;
    }
  }

  uint32_t Label() const noexcept {
    return _gated ? _children.Label() : _label;
  }
  uint32_t Node() const noexcept { return _gated ? _children.Node() : kNoNode; }

  void Next() noexcept {
    if (_gated) {
      _children.Next();
    } else {
      ++_label;
    }
  }

 private:
  PrefixTrie::Children _children;
  uint32_t _label = kNoLabel;
  bool _gated;
};

template<typename A>
Live<A> NextLive(const A& a, ChildLabels& labels,
                 const typename A::State& state, uint32_t hi) {
  for (; labels.Label() <= hi; labels.Next()) {
    const auto next = a.Step(state, static_cast<byte_type>(labels.Label()));
    if (A::Alive(next)) {
      return {.label = labels.Label(), .next = next, .node = labels.Node()};
    }
  }
  return {.label = kNoLabel, .next = {}, .node = kNoNode};
}

template<typename A>
size_t Continuation(const A& a, const PrefixTrie* trie, uint32_t node,
                    const typename A::State& state, std::span<byte_type> tail) {
  if constexpr (A::kMayBeUnknown) {
    if (A::Unknown(state)) {
      return 0;
    }
  }
  typename A::PayloadType payload{};
  if (tail.empty() || a.Accept(state, payload)) {
    return 0;
  }
  const bool gated = Gated(trie, node) || node == kDeadNode;
  const auto range = LiveRangeOf(a, state);
  for (ChildLabels labels{trie, node, range.lo};; labels.Next()) {
    const auto live = NextLive(a, labels, state, range.hi);
    if (live.label == kNoLabel) {
      return gated ? kDeadEnd : 0;
    }
    if (live.label == 0) {
      return 0;
    }
    const size_t rest =
      Continuation(a, trie, live.node, live.next, tail.subspan(1));
    if (rest != kDeadEnd) {
      tail[0] = static_cast<byte_type>(live.label);
      return rest + 1;
    }
  }
}

struct Branch {
  uint32_t label;
  size_t extra;
  Tail tail;
};

template<typename A>
Branch NextBranch(const A& a, const PrefixTrie* trie, uint32_t node,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  Branch branch{.label = kNoLabel, .extra = 0, .tail = {}};
  for (ChildLabels labels{trie, node, std::max(from, range.lo)};;
       labels.Next()) {
    const auto live = NextLive(a, labels, state, range.hi);
    branch.label = live.label;
    if (live.label == kNoLabel) {
      return branch;
    }
    branch.extra = Continuation(a, trie, live.node, live.next, branch.tail);
    if (branch.extra != kDeadEnd) {
      return branch;
    }
  }
}

template<typename A>
bool UsesPrefixes(const DictIndex& index, const A& a) noexcept {
  if (index.Trie().Empty()) {
    return false;
  }
  if constexpr (requires { a.DeadBytes(); }) {
    return index.MayHoldAny(a.DeadBytes());
  }
  return true;
}

template<typename A>
class Targeter {
 public:
  using State = typename A::State;

  static constexpr size_t kNoGate = std::numeric_limits<size_t>::max();

  Targeter(const DictIndex& index, const A& a)
    : _a{&a},
      _trie{UsesPrefixes(index, a) ? &index.Trie() : nullptr},
      _k{index.KeyPrefix().size()},
      _states(_k + 1, a.Start()),
      _ranges(_k + 1),
      _nodes(1, _trie ? _trie->Root() : 0) {}

  size_t GateOf(const DictIndex& index, uint32_t block) const noexcept {
    return _trie ? _k + index.Link(block).depth : kNoGate;
  }

  const State& StateAt(size_t depth) const noexcept { return _states[depth]; }
  void Assign(bytes_view prefix, bytes_view key) {
    _target.Assign(prefix, key);
  }
  bytes_view Key() const noexcept { return _target.Key(); }
  Probe View() const noexcept { return _target.View(); }

  LabelRange LiveRange(size_t depth) {
    if (depth >= _ranged) {
      for (size_t i = _ranged; i <= depth; ++i) {
        _ranges[i].hi = kUnranged;
      }
      _ranged = depth + 1;
    }
    auto& range = _ranges[depth];
    if (range.hi == kUnranged) {
      range = LiveRangeOf(*_a, _states[depth]);
    }
    return range;
  }

  size_t StepAlong(bytes_view path, size_t depth) {
    if (_states.size() <= path.size()) {
      _states.resize(path.size() + 1);
      _ranges.resize(path.size() + 1);
    }
    _ranged = std::min(_ranged, depth + 1);
    if constexpr (A::kCheapRuns) {
      while (depth != path.size()) {
        const auto from = _states[depth];
        State next{};
        const size_t run =
          _a->StepRun(from, path.data() + depth, path.size() - depth, next);
        std::fill_n(_states.begin() + depth + 1, run, from);
        depth += run;
        if (depth == path.size()) {
          break;
        }
        if (!A::Alive(next)) {
          return depth;
        }
        _states[++depth] = next;
      }
      return depth;
    }
    for (; depth != path.size(); ++depth) {
      const auto next = _a->Step(_states[depth], path[depth]);
      if (!A::Alive(next)) {
        return depth;
      }
      _states[depth + 1] = next;
    }
    return depth;
  }

  bool NextTarget(bytes_view path, size_t& depth, size_t gate) {
    SDB_ASSERT(depth >= _k && depth < path.size());
    for (;;) {
      const auto branch =
        NextBranch(*_a, _trie, NodeAt(path, depth, gate), _states[depth],
                   path[depth] + 1u, LiveRange(depth));
      if (branch.label != kNoLabel) {
        auto* target = Retarget(path, depth, 1 + branch.extra);
        target[0] = static_cast<byte_type>(branch.label);
        std::memcpy(target + 1, branch.tail.data(), branch.extra);
        return true;
      }
      if (depth == _k) {
        return false;
      }
      --depth;
    }
  }

  bool TargetFrom(bytes_view key, size_t& valid) {
    size_t depth = StepAlong(key, valid);
    if (_trie) {
      for (size_t at = valid; at < depth; ++at) {
        if (NodeAt(key, at + 1, kNoGate) == kDeadNode) {
          depth = at;
          break;
        }
      }
    }
    if (depth == key.size()) {
      Tail tail;
      const size_t extra = Continuation(*_a, _trie, NodeAt(key, depth, kNoGate),
                                        _states[depth], tail);
      if (extra != kDeadEnd) {
        std::memcpy(Retarget(key, key.size(), extra), tail.data(), extra);
        valid = depth;
        return true;
      }
      if (depth == _k) {
        return false;
      }
      --depth;
    }
    if (!NextTarget(key, depth, kNoGate)) {
      return false;
    }
    valid = depth;
    return true;
  }

 private:
  static constexpr uint32_t kUnranged = std::numeric_limits<uint32_t>::max();

  uint32_t NodeAt(bytes_view path, size_t depth, size_t gate) {
    if (!_trie || depth > gate) {
      return kNoNode;
    }
    const auto term = path.substr(_k, depth - _k);
    size_t have = CommonPrefixLength(bytes_view{_node_path}, term);
    _node_path.resize(have);
    _nodes.resize(have + 1);
    for (; have != term.size(); ++have) {
      _nodes.push_back(ChildNode(_trie, _nodes.back(), term[have]));
      _node_path.push_back(term[have]);
    }
    return _nodes.back();
  }

  byte_type* Retarget(bytes_view path, size_t keep, size_t extra) {
    const bool own = path.data() == _target.Data();
    auto* target = _target.Resize(keep + extra);
    if (!own) {
      std::memcpy(target, path.data(), keep);
    }
    return target + keep;
  }

  const A* _a;
  const PrefixTrie* _trie;
  size_t _k;
  size_t _ranged = 0;
  containers::SmallVector<State, 64> _states;
  containers::SmallVector<LabelRange, 64> _ranges;
  containers::SmallVector<uint32_t, 64> _nodes;
  bstring _node_path;
  PaddedKey _target;
};

template<typename A>
class BlockFilter {
 public:
  using State = typename A::State;

  BlockFilter(const DictIndex& index, const A& a) noexcept
    : _index{&index},
      _a{&a},
      _trie{UsesPrefixes(index, a) ? &index.Trie() : nullptr},
      _states(1, a.Start()) {}

  bool Live(uint32_t block) {
    SDB_ASSERT(!_index->Small());
    if (!Holds(block)) {
      return false;
    }
    if (block != _last) {
      _last = block;
      _live = Compute(block);
    }
    return _live;
  }

  bool Explores() const noexcept { return _trie && !_index->Small(); }
  bool Holds(uint32_t block) noexcept {
    if (!_needed) [[unlikely]] {
      _need = _index->NeedOf(_a->RequiredBytes());
      _needed = true;
    }
    return _index->Holds(block, _need);
  }

  template<typename StateAt>
  bool LiveAt(uint32_t block, StateAt&& state_at) {
    SDB_ASSERT(Explores());
    if (block != _last) {
      _last = block;
      const auto link = _index->Link(block);
      _live = ExploreBlock(block, link, state_at(link.depth));
    }
    return _live;
  }

  uint32_t Next(uint32_t block, uint32_t end) {
    while (block < end && !Live(block)) {
      ++block;
    }
    return block;
  }

  template<typename Emit>
  void Run(uint32_t first, uint32_t end, Emit&& emit) {
    for (auto block = Next(first, end); block < end;
         block = Next(block + 1, end)) {
      emit(block, block + 1);
    }
  }

 private:
  bool Compute(uint32_t block) {
    const auto prefix =
      _index->BlockPrefix(block).substr(_index->KeyPrefix().size());
    const size_t reuse = CommonPrefixLength(bytes_view{_path}, prefix);
    _path.resize(reuse);
    _states.resize(reuse + 1);
    for (size_t at = reuse; at != prefix.size(); ++at) {
      const auto next = _a->Step(_states.back(), prefix[at]);
      if (!A::Alive(next)) {
        return false;
      }
      _states.push_back(next);
      _path.push_back(prefix[at]);
    }
    if (!_trie) {
      return true;
    }
    const auto link = _index->Link(block);
    return ExploreBlock(block, link, _states[link.depth]);
  }

  bool ExploreBlock(uint32_t block, DictIndex::BlockLink link,
                    const State& state) const {
    const size_t skip = _index->KeyPrefix().size() + link.depth;
    const auto bounds = _index->Bounds(block);
    return Explore(link.node, state, bounds.lower.substr(skip),
                   bounds.upper.substr(skip), true, true, bounds.last);
  }

  bool Accepts(const State& state) const {
    if constexpr (A::kMayBeUnknown) {
      if (A::Unknown(state)) {
        return true;
      }
    }
    typename A::PayloadType payload{};
    return _a->Accept(state, payload);
  }

  bool Explore(uint32_t node, const State& state, bytes_view lo, bytes_view hi,
               bool lo_on, bool hi_on, bool inclusive) const {
    const auto& trie = *_trie;
    const bool term =
      trie.Terminal(node) && (!lo_on || lo.empty()) && Accepts(state);
    if (hi_on && hi.empty()) {
      return inclusive && term;
    }
    if (term || trie.Open(node)) {
      return true;
    }
    const auto range = LiveRangeOf(*_a, state);
    uint32_t from = range.lo;
    uint32_t to = range.hi;
    if (lo_on && !lo.empty()) {
      from = std::max<uint32_t>(from, lo[0]);
    }
    if (hi_on) {
      to = std::min<uint32_t>(to, hi[0]);
    }
    for (auto child = trie.ChildrenOf(node, from); child.Label() <= to;
         child.Next()) {
      const uint32_t label = child.Label();
      const auto next = _a->Step(state, static_cast<byte_type>(label));
      if (!A::Alive(next)) {
        continue;
      }
      const bool low = lo_on && !lo.empty() && label == lo[0];
      const bool high = hi_on && label == hi[0];
      if (Explore(child.Node(), next, low ? lo.substr(1) : bytes_view{},
                  high ? hi.substr(1) : bytes_view{}, low, high, inclusive)) {
        return true;
      }
    }
    return false;
  }

  const DictIndex* _index;
  const A* _a;
  const PrefixTrie* _trie;
  BlockNeed _need;
  uint32_t _last = std::numeric_limits<uint32_t>::max();
  bool _needed = false;
  bool _live = false;
  bstring _path;
  containers::SmallVector<State, 32> _states;
};

}  // namespace irs::term_dict
