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

#include <absl/algorithm/container.h>

#include <array>
#include <cstring>
#include <limits>
#include <span>

#include "iresearch/formats/index/term_dict_index.hpp"
#include "iresearch/utils/byte_set.hpp"
#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

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

inline constexpr size_t kMaxTail = 32;
inline constexpr size_t kMaxGuess = 4;

template<typename A>
struct Live {
  uint32_t label;
  typename A::State next;
  TrieCursor child;
};

template<typename A>
Live<A> FirstLive(const PrefixTrie& trie, TrieCursor head, const A& a,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  const auto lo = std::max(from, range.lo);
  if (!PrefixTrie::Gates(head)) {
    for (auto label = lo; label <= range.hi; ++label) {
      const auto next = a.Step(state, static_cast<byte_type>(label));
      if (A::Alive(next)) {
        return {.label = label, .next = next, .child = head};
      }
    }
    return {.label = kNoLabel, .next = {}, .child = head};
  }
  const auto labels = trie.Labels(head);
  for (auto it = absl::c_lower_bound(labels, lo);
       it != labels.end() && *it <= range.hi; ++it) {
    const auto next = a.Step(state, *it);
    if (A::Alive(next)) {
      return {
        .label = *it,
        .next = next,
        .child = trie.ChildAt(head, static_cast<uint32_t>(it - labels.begin())),
      };
    }
  }
  return {.label = kNoLabel, .next = {}, .child = head};
}

enum class EdgeStep : uint8_t {
  Live,
  Dead,
  Unknown,
};

template<typename A>
EdgeStep StepEdge(const A& a, typename A::State& state, bytes_view edge) {
  for (size_t i = 0; i != edge.size(); ++i) {
    if constexpr (A::kCheapRuns) {
      typename A::State next{};
      i += a.StepRun(state, edge.data() + i, edge.size() - i, next);
      if (i == edge.size()) {
        break;
      }
      state = next;
    } else {
      state = a.Step(state, edge[i]);
    }
    if (!A::Alive(state)) {
      return EdgeStep::Dead;
    }
    if constexpr (A::kMayBeUnknown) {
      if (A::Unknown(state)) {
        return EdgeStep::Unknown;
      }
    }
  }
  return EdgeStep::Live;
}

inline constexpr size_t kDeadEnd = std::numeric_limits<size_t>::max();

using Tail = std::array<byte_type, kMaxTail>;

template<typename A>
size_t Continuation(const PrefixTrie& trie, TrieCursor head, const A& a,
                    typename A::State state, std::span<byte_type> tail) {
  if constexpr (A::kMayBeUnknown) {
    if (A::Unknown(state)) {
      return 0;
    }
  }
  const bool gated = PrefixTrie::Gates(head);
  if (!gated) {
    tail = tail.first(std::min(tail.size(), kMaxGuess));
  }
  size_t extra = 0;
  if (const auto edge = trie.Edge(head); !edge.empty()) {
    extra = std::min(edge.size(), tail.size());
    absl::c_copy(edge.substr(0, extra), tail.begin());
    switch (StepEdge(a, state, edge)) {
      case EdgeStep::Dead:
        return kDeadEnd;
      case EdgeStep::Unknown:
        return extra;
      case EdgeStep::Live:
        break;
    }
    head.matched += static_cast<uint32_t>(edge.size());
    tail = extra == edge.size() ? tail.subspan(extra) : std::span<byte_type>{};
  }
  typename A::PayloadType payload{};
  if (trie.MayEnd(head) && a.Accept(state, payload)) {
    return extra;
  }
  const auto range = LiveRangeOf(a, state);
  if (tail.empty()) {
    return gated && FirstLive(trie, head, a, state, 0, range).label == kNoLabel
             ? kDeadEnd
             : extra;
  }
  for (uint32_t from = 0;;) {
    const auto live = FirstLive(trie, head, a, state, from, range);
    if (live.label == kNoLabel) {
      return gated ? kDeadEnd : extra;
    }
    const size_t rest =
      Continuation(trie, live.child, a, live.next, tail.subspan(1));
    if (rest != kDeadEnd) {
      tail[0] = static_cast<byte_type>(live.label);
      return extra + 1 + rest;
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
Branch NextBranch(const PrefixTrie& trie, TrieCursor head, const A& a,
                  const typename A::State& state, uint32_t from,
                  LabelRange range) {
  Branch branch{.label = kNoLabel, .extra = 0, .tail = {}};
  for (;; from = branch.label + 1) {
    const auto live = FirstLive(trie, head, a, state, from, range);
    branch.label = live.label;
    if (live.label == kNoLabel) {
      return branch;
    }
    branch.extra = Continuation(trie, live.child, a, live.next, branch.tail);
    if (branch.extra != kDeadEnd) {
      return branch;
    }
  }
}

class TrieHeads {
 public:
  TrieHeads(const PrefixTrie& trie, TrieCursor root, size_t k)
    : _trie{&trie}, _k{k} {
    _heads.push_back(root);
  }

  TrieCursor operator[](size_t depth) const noexcept {
    SDB_ASSERT(depth >= _k);
    const size_t at = depth - _k;
    SDB_ASSERT(at < _heads.size() || !PrefixTrie::Gates(_heads.back()));
    return at < _heads.size() ? _heads[at] : _heads.back();
  }

  size_t Extend(bytes_view path, size_t depth) {
    SDB_ASSERT(_k <= depth && depth <= path.size());
    if (!PrefixTrie::Gates(_heads.front())) {
      return depth;
    }
    size_t at = CommonPrefixLength(bytes_view{_path}, path.substr(_k));
    _path.resize(at);
    _heads.resize(at + 1);
    const auto want = path.substr(_k, depth - _k);
    while (at < want.size() && PrefixTrie::Gates(_heads.back())) {
      const auto head = _heads.back();
      if (const auto edge = _trie->Edge(head); !edge.empty()) {
        const auto matched = CommonPrefixLength(edge, want.substr(at));
        for (size_t i = 1; i <= matched; ++i) {
          auto next = head;
          next.matched += static_cast<uint32_t>(i);
          _heads.push_back(next);
        }
        _path.append(want.substr(at, matched));
        at += matched;
        if (matched != edge.size() && at != want.size()) {
          return _k + at;
        }
        continue;
      }
      const auto child = _trie->Child(head, want[at]);
      if (child.node == kTrieMissing) {
        return _k + at;
      }
      _heads.push_back(child);
      _path.push_back(want[at]);
      ++at;
    }
    return depth;
  }

  bool Branchable(bytes_view path, size_t& depth) const noexcept {
    if (!PrefixTrie::Gates(_heads.front())) {
      return true;
    }
    for (;;) {
      const auto head = (*this)[depth];
      const auto edge = _trie->Edge(head);
      if (edge.empty() || edge.front() > path[depth]) {
        return true;
      }
      if (depth == _k + head.matched) {
        return false;
      }
      depth -= head.matched + 1;
    }
  }

 private:
  const PrefixTrie* _trie;
  containers::SmallVector<TrieCursor, 16> _heads;
  bstring _path;
  size_t _k;
};

template<typename A>
class Targeter {
 public:
  using State = typename A::State;

  Targeter(const DictIndex& index, const A& a)
    : _trie{&index.Trie()},
      _a{&a},
      _k{index.KeyPrefix().size()},
      _heads{index.Trie(), index.TrieRoot(), _k} {}

  void Start() {
    if (_states.empty()) {
      _states.assign(_k + 1, _a->Start());
      _ranges.resize(_k + 1);
      _ranged = 0;
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
    SDB_ASSERT(depth >= _k);
    depth = _heads.Extend(path, depth);
    for (;;) {
      if (!_heads.Branchable(path, depth)) {
        return false;
      }
      const auto branch = NextBranch(*_trie, _heads[depth], *_a, _states[depth],
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
    size_t depth = _heads.Extend(key, StepAlong(key, valid));
    if (depth == key.size()) {
      Tail tail;
      const size_t extra =
        Continuation(*_trie, _heads[depth], *_a, _states[depth], tail);
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

  byte_type* Retarget(bytes_view path, size_t keep, size_t extra) {
    const bool own = path.data() == _target.Data();
    auto* target = _target.Resize(keep + extra);
    if (!own) {
      std::memcpy(target, path.data(), keep);
    }
    return target + keep;
  }

  const PrefixTrie* _trie;
  const A* _a;
  size_t _k;
  size_t _ranged = 0;
  containers::SmallVector<State, 64> _states;
  containers::SmallVector<LabelRange, 64> _ranges;
  PaddedKey _target;
  TrieHeads _heads;
};

template<typename A>
const ByteSet* NeedOf(const DictIndex& index, const A& a) noexcept {
  const auto& need = a.RequiredBytes();
  return index.Masked() && (need.words[0] | need.words[1]) != 0 ? &need
                                                                : nullptr;
}

template<typename A>
class BlockMatcher {
 public:
  using State = typename A::State;

  BlockMatcher(const DictIndex& index, const A& a, bool whole) noexcept
    : _index{&index}, _a{&a}, _need{NeedOf(index, a)}, _whole{whole} {}

  template<typename Emit>
  void Run(uint32_t first, uint32_t end, Emit&& emit) {
    _first = first;
    _seen = 0;
    _end = end;
    if (_first == _end) {
      return;
    }
    if (const auto root = _index->TrieRoot();
        PrefixTrie::Gates(root) && !_whole) {
      Visit(root, _a->Start(), 0, emit);
    } else {
      Add(_first, _end - 1, emit);
    }
  }

 private:
  static constexpr size_t kMaxDepth = 64;

  template<typename Emit>
  bool Visit(TrieCursor cursor, State state, size_t depth, Emit& emit) {
    if (cursor.hi < _first || cursor.hi < _seen) {
      return true;
    }
    if (cursor.lo >= _end) {
      return false;
    }
    if constexpr (A::kMayBeUnknown) {
      if (A::Unknown(state)) {
        Add(cursor.lo, cursor.hi, emit);
        return true;
      }
    }
    const auto& trie = _index->Trie();
    if (cursor.lo == cursor.hi || cursor.node == kTrieBelow ||
        depth == kMaxDepth) {
      Tail tail;
      if (Continuation(trie, cursor, *_a, state,
                       std::span{tail}.first(kMaxGuess)) != kDeadEnd) {
        Add(cursor.lo, cursor.hi, emit);
      }
      return true;
    }
    if (const auto edge = trie.Edge(cursor); !edge.empty()) {
      switch (StepEdge(*_a, state, edge)) {
        case EdgeStep::Dead:
          return true;
        case EdgeStep::Unknown:
          Add(cursor.lo, cursor.hi, emit);
          return true;
        case EdgeStep::Live:
          break;
      }
      cursor.matched += static_cast<uint32_t>(edge.size());
    }
    if (trie.MayEnd(cursor)) {
      typename A::PayloadType payload{};
      if (_a->Accept(state, payload)) {
        Add(cursor.lo, cursor.lo, emit);
      }
    }
    const auto labels = trie.Labels(cursor);
    const auto range = LiveRangeOf(*_a, state);
    uint32_t reach = 0;
    for (auto slot = static_cast<uint32_t>(
           absl::c_lower_bound(labels, range.lo) - labels.begin());
         slot < labels.size(); ++slot) {
      if (const uint32_t from = std::max(_first, _seen); from != reach) {
        reach = from;
        slot = trie.SlotReaching(cursor, slot, reach);
        if (slot == labels.size()) {
          break;
        }
      }
      if (labels[slot] > range.hi) {
        break;
      }
      const auto next = _a->Step(state, labels[slot]);
      if (A::Alive(next) &&
          !Visit(trie.ChildAt(cursor, slot), next, depth + 1, emit)) {
        return false;
      }
    }
    return true;
  }

  template<typename Emit>
  void Add(uint32_t lo, uint32_t hi, Emit& emit) {
    lo = std::max(lo, _first);
    const uint32_t end = std::min(hi + 1, _end);
    if (lo >= end) {
      return;
    }
    _seen = std::max(_seen, end);
    if (!_need) {
      emit(lo, end);
      return;
    }
    for (auto block = _index->Holding(lo, end, *_need); block < end;
         block = _index->Holding(block + 1, end, *_need)) {
      emit(block, block + 1);
    }
  }

  const DictIndex* _index;
  const A* _a;
  const ByteSet* _need;
  uint32_t _first = 0;
  uint32_t _end = 0;
  uint32_t _seen = 0;
  bool _whole;
};

}  // namespace irs::term_dict
