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
#include <cstdint>
#include <limits>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "iresearch/types.hpp"

namespace irs {

class DataOutput;
class IndexInput;

inline constexpr uint32_t kHnswInvalidNode = std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kHnswDefaultM = 32;
inline constexpr uint32_t kHnswDefaultEfConstruction = 200;
inline constexpr uint32_t kHnswDefaultEfSearch = 64;
inline constexpr uint32_t kHnswSerialWarmup = 256;
inline constexpr uint32_t kHnswFormatVersion = 1;
inline constexpr uint64_t kHnswBuildSeed = 0x9E3779B97F4A7C15ULL;

class HnswVisited {
 public:
  void Reset(size_t n) {
    if (_marks.size() < n) {
      _marks.assign(n, 0);
      _generation = 0;
    }
    Advance();
  }

  void Advance() noexcept {
    if (++_generation == 0) {
      std::ranges::fill(_marks, 0);
      _generation = 1;
    }
  }

  bool TestAndSet(uint32_t id) noexcept {
    SDB_ASSERT(id < _marks.size());
    const bool seen = _marks[id] == _generation;
    _marks[id] = _generation;
    return seen;
  }

 private:
  std::vector<uint32_t> _marks;
  uint32_t _generation = 0;
};

struct HnswCandidate {
  score_t score;
  uint32_t node;
};

struct HnswNearestOrder {
  bool operator()(const HnswCandidate& l,
                  const HnswCandidate& r) const noexcept {
    return l.score > r.score;
  }
};

struct HnswFrontierOrder {
  bool operator()(const HnswCandidate& l,
                  const HnswCandidate& r) const noexcept {
    return l.score < r.score;
  }
};

struct HnswSearchScratch {
  HnswVisited visited;
  std::vector<HnswCandidate> nearest;
  std::vector<HnswCandidate> frontier;
  std::vector<uint32_t> batch;
  std::vector<score_t> scores;
};

class HnswGraph {
 public:
  HnswGraph() = default;

  void Reset(size_t nodes, uint32_t m);

  void AllocateLinks();

  size_t Size() const noexcept { return _levels.size(); }
  uint32_t M() const noexcept { return _m; }
  uint32_t M0() const noexcept { return _m0; }
  uint32_t MaxLevel() const noexcept { return _max_level; }
  uint32_t EntryPoint() const noexcept { return _entry; }
  bool Empty() const noexcept { return _entry == kHnswInvalidNode; }

  uint32_t LevelOf(uint32_t node) const noexcept { return _levels[node]; }

  void SetLevel(uint32_t node, uint32_t level) noexcept {
    _levels[node] = static_cast<uint8_t>(level);
    _max_level = std::max(_max_level, level);
  }

  void SetEntryPoint(uint32_t node) noexcept { _entry = node; }

  std::span<uint32_t> Neighbors(uint32_t node, uint32_t level) noexcept {
    const auto width = level == 0 ? _m0 : _m;
    return {_neighbors.data() + Base(node, level), width};
  }

  std::span<const uint32_t> Neighbors(uint32_t node,
                                      uint32_t level) const noexcept {
    const auto width = level == 0 ? _m0 : _m;
    return {_neighbors.data() + Base(node, level), width};
  }

  void Serialize(DataOutput& out) const;
  static HnswGraph Deserialize(IndexInput& in);

  size_t ByteSize() const noexcept;

 private:
  uint64_t Base(uint32_t node, uint32_t level) const noexcept {
    SDB_ASSERT(level < _levels[node]);
    return _offsets[node] + (level == 0 ? 0 : _m0 + uint64_t{level - 1} * _m);
  }

  std::vector<uint8_t> _levels;
  std::vector<uint64_t> _offsets;
  std::vector<uint32_t> _neighbors;
  uint32_t _entry = kHnswInvalidNode;
  uint32_t _max_level = 0;
  uint32_t _m = kHnswDefaultM;
  uint32_t _m0 = 2 * kHnswDefaultM;
};

uint32_t HnswRandomLevel(uint64_t& rng_state, uint32_t m) noexcept;

// Score `ids` against `q` four rows at a time, keeping the query in registers
// across the group and running four independent accumulator chains. Both write
// larger-is-nearer scores, so L2Sqr is negated.
void HnswBatchL2Sqr(const float* q, const float* base, uint32_t d,
                    std::span<const uint32_t> ids, score_t* out) noexcept;

void HnswBatchIp(const float* q, const float* base, uint32_t d,
                 std::span<const uint32_t> ids, score_t* out) noexcept;

template<typename Dist>
void HnswSearchLevel(const HnswGraph& graph, Dist& dist, uint32_t level,
                     uint32_t ef, HnswSearchScratch& s) {
  auto& nearest = s.nearest;
  auto& frontier = s.frontier;
  frontier.assign(nearest.begin(), nearest.end());
  std::make_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
  std::make_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});

  while (!frontier.empty()) {
    std::pop_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    const auto cur = frontier.back();
    frontier.pop_back();

    if (nearest.size() >= ef && cur.score < nearest.front().score) {
      break;
    }

    s.batch.clear();
    for (const auto id : graph.Neighbors(cur.node, level)) {
      if (id == kHnswInvalidNode) {
        break;
      }
      if (s.visited.TestAndSet(id)) {
        continue;
      }
      s.batch.push_back(id);
      dist.Prefetch(id);
    }
    if (s.batch.empty()) {
      continue;
    }

    s.scores.resize(s.batch.size());
    dist.Batch(s.batch, s.scores.data());

    for (size_t i = 0; i < s.batch.size(); ++i) {
      const HnswCandidate cand{s.scores[i], s.batch[i]};
      if (nearest.size() >= ef && cand.score <= nearest.front().score) {
        continue;
      }
      nearest.push_back(cand);
      std::push_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
      if (nearest.size() > ef) {
        std::pop_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
        nearest.pop_back();
      }
      frontier.push_back(cand);
      std::push_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    }
  }
}

template<typename Dist>
HnswCandidate HnswGreedyDescent(const HnswGraph& graph, Dist& dist,
                                HnswCandidate cur, uint32_t from_level,
                                uint32_t to_level, HnswSearchScratch& s) {
  for (uint32_t level = from_level; level > to_level; --level) {
    for (bool improved = true; improved;) {
      improved = false;
      s.batch.clear();
      for (const auto id : graph.Neighbors(cur.node, level)) {
        if (id == kHnswInvalidNode) {
          break;
        }
        s.batch.push_back(id);
        dist.Prefetch(id);
      }
      if (s.batch.empty()) {
        break;
      }
      s.scores.resize(s.batch.size());
      dist.Batch(s.batch, s.scores.data());
      for (size_t i = 0; i < s.batch.size(); ++i) {
        if (s.scores[i] > cur.score) {
          cur = {s.scores[i], s.batch[i]};
          improved = true;
        }
      }
    }
  }
  return cur;
}

template<typename Dist>
void HnswSelectNeighbors(Dist& dist, std::span<const HnswCandidate> sorted,
                         uint32_t limit, std::vector<uint32_t>& out) {
  out.clear();
  for (const auto& cand : sorted) {
    if (out.size() >= limit) {
      break;
    }
    bool keep = true;
    for (const auto accepted : out) {
      if (dist.Pair(cand.node, accepted) > cand.score) {
        keep = false;
        break;
      }
    }
    if (keep) {
      out.push_back(cand.node);
    }
  }
}

struct HnswBuildScratch {
  HnswSearchScratch search;
  std::vector<uint32_t> selected;
  std::vector<uint32_t> reverse_selected;
  std::vector<HnswCandidate> peer_candidates;
};

template<typename Dist>
void HnswLinkReverse(HnswGraph& graph, Dist& dist, uint32_t peer, uint32_t node,
                     uint32_t level, HnswBuildScratch& s) {
  auto links = graph.Neighbors(peer, level);
  auto& cur = s.peer_candidates;
  cur.clear();
  for (const auto id : links) {
    if (id == kHnswInvalidNode) {
      break;
    }
    if (id == node) {
      return;
    }
    cur.push_back({dist.Pair(peer, id), id});
  }

  if (cur.size() < links.size()) {
    links[cur.size()] = node;
    return;
  }

  const score_t score = dist.Pair(peer, node);
  size_t pos = 0;
  while (pos < cur.size() && cur[pos].score > score) {
    ++pos;
  }

  for (size_t j = 0; j < pos; ++j) {
    if (dist.Pair(node, cur[j].node) > score) {
      return;
    }
  }

  cur.insert(cur.begin() + static_cast<ptrdiff_t>(pos), {score, node});
  for (size_t k = pos + 1; k < cur.size();) {
    if (dist.Pair(cur[k].node, node) > cur[k].score) {
      cur.erase(cur.begin() + static_cast<ptrdiff_t>(k));
    } else {
      ++k;
    }
  }
  if (cur.size() > links.size()) {
    cur.resize(links.size());
  }

  for (size_t i = 0; i < links.size(); ++i) {
    links[i] = i < cur.size() ? cur[i].node : kHnswInvalidNode;
  }
}

template<typename Dist>
void HnswInsert(HnswGraph& graph, uint32_t node, Dist& dist,
                uint32_t ef_construction, HnswBuildScratch& s) {
  const uint32_t top = graph.LevelOf(node) - 1;
  if (graph.Empty()) {
    graph.SetEntryPoint(node);
    return;
  }

  const uint32_t entry = graph.EntryPoint();
  const uint32_t entry_top = graph.LevelOf(entry) - 1;

  HnswCandidate cur{dist.One(entry), entry};
  if (entry_top > top) {
    cur = HnswGreedyDescent(graph, dist, cur, entry_top, top, s.search);
  }

  const uint32_t start = std::min(top, entry_top);
  for (uint32_t level = start + 1; level-- > 0;) {
    s.search.visited.Advance();
    s.search.visited.TestAndSet(cur.node);
    s.search.nearest.assign(1, cur);
    HnswSearchLevel(graph, dist, level, ef_construction, s.search);

    auto& found = s.search.nearest;
    std::ranges::sort(found, [](const HnswCandidate& l, const HnswCandidate& r) {
      return l.score > r.score;
    });

    const uint32_t width = level == 0 ? graph.M0() : graph.M();
    HnswSelectNeighbors(dist, found, width, s.selected);

    auto links = graph.Neighbors(node, level);
    for (size_t i = 0; i < links.size(); ++i) {
      links[i] = i < s.selected.size() ? s.selected[i] : kHnswInvalidNode;
    }
    for (const auto peer : s.selected) {
      HnswLinkReverse(graph, dist, peer, node, level, s);
    }

    cur = found.front();
  }

  if (top > entry_top) {
    graph.SetEntryPoint(node);
  }
}

template<typename Dist>
void HnswSearchTopK(const HnswGraph& graph, Dist& dist, uint32_t ef,
                    HnswSearchScratch& s) {
  s.nearest.clear();
  if (graph.Empty()) {
    return;
  }
  const uint32_t entry = graph.EntryPoint();
  const uint32_t entry_top = graph.LevelOf(entry) - 1;

  s.visited.Reset(graph.Size());
  HnswCandidate cur{dist.One(entry), entry};
  if (entry_top > 0) {
    cur = HnswGreedyDescent(graph, dist, cur, entry_top, 0, s);
  }
  s.visited.Advance();
  s.visited.TestAndSet(cur.node);
  s.nearest.assign(1, cur);
  HnswSearchLevel(graph, dist, 0, ef, s);
  std::ranges::sort(s.nearest,
                    [](const HnswCandidate& l, const HnswCandidate& r) {
                      return l.score > r.score;
                    });
}

template<typename Dist>
void HnswSearchRadius(const HnswGraph& graph, Dist& dist, score_t threshold,
                      size_t max_results, HnswSearchScratch& s) {
  auto& found = s.nearest;
  auto& frontier = s.frontier;
  if (graph.Empty()) {
    found.clear();
    return;
  }

  const uint32_t entry = graph.EntryPoint();
  const uint32_t entry_top = graph.LevelOf(entry) - 1;

  s.visited.Reset(graph.Size());
  HnswCandidate cur{dist.One(entry), entry};
  if (entry_top > 0) {
    cur = HnswGreedyDescent(graph, dist, cur, entry_top, 0, s);
  }

  s.visited.Advance();
  s.visited.TestAndSet(cur.node);
  found.clear();
  frontier.assign(1, cur);
  if (cur.score >= threshold) {
    found.push_back(cur);
  }

  while (!frontier.empty() && found.size() < max_results) {
    std::pop_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    const auto node = frontier.back();
    frontier.pop_back();
    if (node.score < threshold) {
      break;
    }

    s.batch.clear();
    for (const auto id : graph.Neighbors(node.node, 0)) {
      if (id == kHnswInvalidNode) {
        break;
      }
      if (s.visited.TestAndSet(id)) {
        continue;
      }
      s.batch.push_back(id);
      dist.Prefetch(id);
    }
    if (s.batch.empty()) {
      continue;
    }

    s.scores.resize(s.batch.size());
    dist.Batch(s.batch, s.scores.data());
    for (size_t i = 0; i < s.batch.size(); ++i) {
      if (s.scores[i] < threshold) {
        continue;
      }
      const HnswCandidate cand{s.scores[i], s.batch[i]};
      found.push_back(cand);
      frontier.push_back(cand);
      std::push_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    }
  }
}

}  // namespace irs