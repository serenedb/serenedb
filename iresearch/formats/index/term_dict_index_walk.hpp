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

template<typename A>
Live<A> FirstLive(const A& a, const PrefixTrie* trie, uint32_t node,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  if (node == kDeadNode) {
    return {.label = kNoLabel, .next = {}, .node = kNoNode};
  }
  const bool gated = Gated(trie, node);
  for (auto label = std::max(from, range.lo); label <= range.hi;) {
    uint32_t child = kNoNode;
    if (gated) {
      const auto next = trie->NextChild(node, label);
      if (next.label > range.hi) {
        break;
      }
      label = next.label;
      child = next.node;
    }
    const auto next = a.Step(state, static_cast<byte_type>(label));
    if (A::Alive(next)) {
      return {.label = label, .next = next, .node = child};
    }
    ++label;
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
  for (uint32_t from = 0;;) {
    const auto live = FirstLive(a, trie, node, state, from, range);
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
    from = live.label + 1;
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
  for (;; from = branch.label + 1) {
    const auto live = FirstLive(a, trie, node, state, from, range);
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

  Targeter(const DictIndex& index, const A& a)
    : _a{&a},
      _trie{UsesPrefixes(index, a) ? &index.Trie() : nullptr},
      _k{index.KeyPrefix().size()} {}

  void Start() {
    if (_states.empty()) {
      _states.assign(_k + 1, _a->Start());
      _ranges.resize(_k + 1);
      _ranged = 0;
      _nodes.assign(1, PrefixTrie::kRoot);
    }
  }

  const State& StateAt(size_t depth) const noexcept { return _states[depth]; }
  Probe Assign(bytes_view prefix, bytes_view key) {
    return _target.Assign(prefix, key);
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

  bool NextTarget(bytes_view path, size_t& depth) {
    SDB_ASSERT(depth >= _k && depth < path.size());
    for (;;) {
      const auto branch =
        NextBranch(*_a, _trie, NodeAt(path, depth), _states[depth],
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
        if (NodeAt(key, at + 1) == kDeadNode) {
          depth = at;
          break;
        }
      }
    }
    if (depth == key.size()) {
      Tail tail;
      const size_t extra =
        Continuation(*_a, _trie, NodeAt(key, depth), _states[depth], tail);
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
    if (!NextTarget(key, depth)) {
      return false;
    }
    valid = depth;
    return true;
  }

 private:
  static constexpr uint32_t kUnranged = std::numeric_limits<uint32_t>::max();

  uint32_t NodeAt(bytes_view path, size_t depth) {
    if (!_trie) {
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
const ByteSet* NeedOf(const DictIndex& index, const A& a) noexcept {
  const auto& need = a.RequiredBytes();
  return index.Masked() && (need.words[0] | need.words[1]) != 0 ? &need
                                                                : nullptr;
}

template<typename A>
class BlockFilter {
 public:
  using State = typename A::State;

  BlockFilter(const DictIndex& index, const A& a) noexcept
    : _index{&index},
      _a{&a},
      _need{NeedOf(index, a)},
      _trie{UsesPrefixes(index, a) ? &index.Trie() : nullptr} {}

  bool Live(uint32_t block) {
    if (_need && !_index->Holds(block, *_need)) {
      return false;
    }
    if (!_trie || _index->Small()) {
      return true;
    }
    if (block != _last) {
      _last = block;
      _live = Compute(block);
    }
    return _live;
  }

  template<typename StateAt>
  bool LiveAt(uint32_t block, StateAt&& state_at) {
    if (_need && !_index->Holds(block, *_need)) {
      return false;
    }
    if (!_trie || _index->Small()) {
      return true;
    }
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
    return std::min(block, end);
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
    const auto& index = *_index;
    const size_t k = index.KeyPrefix().size();
    const bytes_view lower =
      block == 0 ? index.MinKey() : index.Separator(block);
    const bytes_view upper =
      block + 1 == index.Blocks() ? index.MaxKey() : index.Separator(block + 1);
    const auto prefix = lower.substr(k, CommonPrefixLength(lower, upper) - k);
    if (_states.empty()) {
      _states.assign(1, _a->Start());
    }
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
    const auto link = index.Link(block);
    return ExploreBlock(block, link, _states[link.depth]);
  }

  bool ExploreBlock(uint32_t block, DictIndex::BlockLink link,
                    const State& state) const {
    const auto& index = *_index;
    const size_t skip = index.KeyPrefix().size() + link.depth;
    const bool last = block + 1 == index.Blocks();
    const bytes_view lower =
      block == 0 ? index.MinKey() : index.Separator(block);
    const bytes_view upper = last ? index.MaxKey() : index.Separator(block + 1);
    return Explore(link.node, state, lower.substr(skip), upper.substr(skip),
                   true, true, last);
  }

  bool Explore(uint32_t node, const State& state, bytes_view lo, bytes_view hi,
               bool lo_on, bool hi_on, bool inclusive) const {
    const auto& trie = *_trie;
    if (trie.Terminal(node) && (!lo_on || lo.empty()) &&
        (!hi_on || !hi.empty() || inclusive)) {
      return true;
    }
    if (trie.Open(node)) {
      return true;
    }
    if (hi_on && hi.empty()) {
      return false;
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
    for (auto child = trie.NextChild(node, from); child.label <= to;
         child = trie.NextChild(node, child.label + 1)) {
      const auto next = _a->Step(state, static_cast<byte_type>(child.label));
      if (!A::Alive(next)) {
        continue;
      }
      const bool low = lo_on && !lo.empty() && child.label == lo[0];
      const bool high = hi_on && child.label == hi[0];
      if (Explore(child.node, next, low ? lo.substr(1) : bytes_view{},
                  high ? hi.substr(1) : bytes_view{}, low, high, inclusive)) {
        return true;
      }
    }
    return false;
  }

  const DictIndex* _index;
  const A* _a;
  const ByteSet* _need;
  const PrefixTrie* _trie;
  uint32_t _last = std::numeric_limits<uint32_t>::max();
  bool _live = false;
  bstring _path;
  containers::SmallVector<State, 32> _states;
};

}  // namespace irs::term_dict
