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

#include <faiss/utils/distances.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <limits>
#include <mutex>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"
namespace irs {

class DataOutput;
class IndexInput;

inline constexpr uint32_t kHnswInvalidNode =
  std::numeric_limits<uint32_t>::max();
inline constexpr uint32_t kHnswDefaultM = 32;
inline constexpr uint32_t kHnswDefaultEfConstruction = 200;
inline constexpr uint32_t kHnswDefaultEfSearch = 64;
inline constexpr uint32_t kHnswSerialWarmup = 256;
inline constexpr size_t kHnswMinRowsPerWorker = 1024;
inline constexpr uint32_t kHnswMaxWorkers = 64;
inline constexpr size_t kHnswInsertGranule = 256;
inline constexpr uint64_t kHnswTrainSample = 262144;
inline constexpr uint32_t kHnswMaxLevel = std::numeric_limits<uint8_t>::max();
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
  std::vector<uint16_t> _marks;
  uint16_t _generation = 0;
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
    SDB_ASSERT(level != 0);
    SDB_ASSERT(level <= kHnswMaxLevel);
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

template<VectorMetric M>
void HnswComputeDistances(const float* q, const float* base, uint32_t d,
                          std::span<const uint32_t> ids,
                          score_t* out) noexcept {
  constexpr auto kKernel = EffectiveQuantMetric(M);
  constexpr float kSign = kKernel == VectorMetric::L2Sqr ? -1.f : 1.f;
  const auto row = [base, d](uint32_t id) noexcept {
    return base + static_cast<size_t>(id) * d;
  };

  size_t i = 0;
  if constexpr (M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct) {
    for (; i + 4 <= ids.size(); i += 4) {
      float d0 = 0.f;
      float d1 = 0.f;
      float d2 = 0.f;
      float d3 = 0.f;
      if constexpr (kKernel == VectorMetric::L2Sqr) {
        faiss::fvec_L2sqr_batch_4(q, row(ids[i]), row(ids[i + 1]),
                                  row(ids[i + 2]), row(ids[i + 3]), d, d0, d1,
                                  d2, d3);
      } else {
        faiss::fvec_inner_product_batch_4(q, row(ids[i]), row(ids[i + 1]),
                                          row(ids[i + 2]), row(ids[i + 3]), d,
                                          d0, d1, d2, d3);
      }
      out[i] = kSign * d0;
      out[i + 1] = kSign * d1;
      out[i + 2] = kSign * d2;
      out[i + 3] = kSign * d3;
    }
  }

  for (; i < ids.size(); ++i) {
    if constexpr (kKernel == VectorMetric::L2Sqr) {
      out[i] = kSign * faiss::fvec_L2sqr(q, row(ids[i]), d);
    } else if constexpr (kKernel == VectorMetric::InnerProduct) {
      out[i] = faiss::fvec_inner_product(q, row(ids[i]), d);
    } else {
      out[i] =
        ComputeDistance<kKernel>(q, row(ids[i]), static_cast<uint16_t>(d));
    }
  }
}

inline constexpr score_t kHnswNoThreshold =
  std::numeric_limits<score_t>::lowest();

inline auto HnswLoadLink(const uint32_t& slot) noexcept -> uint32_t {
  return std::atomic_ref<uint32_t>{const_cast<uint32_t&>(slot)}.load(
    std::memory_order_acquire);
}

inline void HnswStoreLink(uint32_t& slot, uint32_t id) noexcept {
  std::atomic_ref<uint32_t>{slot}.store(id, std::memory_order_release);
}

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
    const auto neighbors = graph.Neighbors(cur.node, level);
    for (size_t i = 0; i < neighbors.size(); ++i) {
      const auto id = HnswLoadLink(neighbors[i]);
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
    dist.Batch(s.batch, s.scores.data(),
               nearest.size() >= ef ? nearest.front().score : kHnswNoThreshold);

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
      const auto neighbors = graph.Neighbors(cur.node, level);
      for (size_t i = 0; i < neighbors.size(); ++i) {
        const auto id = HnswLoadLink(neighbors[i]);
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
        break;
      }
      s.scores.resize(s.batch.size());
      dist.Batch(s.batch, s.scores.data(), cur.score);
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

struct HnswPendingLink {
  uint32_t peer;
  uint32_t level;
};

struct HnswBuildScratch {
  HnswSearchScratch search;
  std::vector<uint32_t> selected;
  std::vector<HnswCandidate> peer_candidates;
  std::vector<uint32_t> link_ids;
  std::vector<score_t> peer_scores;
  std::vector<score_t> node_scores;
  std::vector<HnswPendingLink> pending;
  std::vector<uint8_t> select_blocked;
  std::vector<uint32_t> select_ids;
};

template<typename Dist>
void HnswSelectNeighbors(Dist& dist, std::span<const HnswCandidate> sorted,
                         uint32_t limit, HnswBuildScratch& s) {
  auto& out = s.selected;
  out.clear();
  SDB_ASSERT(!sorted.empty() && limit != 0);
  if (dist.CheapPair()) {
    for (const auto& cand : sorted) {
      bool keep = true;
      for (const auto accepted : out) {
        if (dist.Pair(cand.node, accepted) > cand.score) {
          keep = false;
          break;
        }
      }
      if (!keep) {
        continue;
      }
      out.push_back(cand.node);
      if (out.size() >= limit) {
        break;
      }
    }
    return;
  }
  auto& blocked = s.select_blocked;
  blocked.assign(sorted.size(), 0);
  auto& rest = s.select_ids;
  auto& scores = s.node_scores;
  for (size_t i = 0; i < sorted.size(); ++i) {
    if (blocked[i] != 0) {
      continue;
    }
    out.push_back(sorted[i].node);
    if (out.size() >= limit || i + 1 == sorted.size()) {
      break;
    }
    rest.clear();
    for (size_t j = i + 1; j < sorted.size(); ++j) {
      if (blocked[j] == 0) {
        rest.push_back(sorted[j].node);
      }
    }
    if (rest.empty()) {
      break;
    }
    scores.resize(rest.size());
    dist.PairBatch(sorted[i].node, rest, scores.data());
    size_t k = 0;
    for (size_t j = i + 1; j < sorted.size(); ++j) {
      if (blocked[j] != 0) {
        continue;
      }
      if (scores[k] > sorted[j].score) {
        blocked[j] = 1;
      }
      ++k;
    }
  }
}

struct HnswNoSync {
  struct Guard {};
  static Guard Lock(uint32_t /*node*/) noexcept { return {}; }
};

class HnswStripeSync {
 public:
  explicit HnswStripeSync(size_t stripes)
    : _stripes(std::bit_ceil(std::clamp<size_t>(4 * stripes, 256, 4096))) {}

  using Guard = std::unique_lock<std::mutex>;

  Guard Lock(uint32_t node) noexcept {
    const auto h = (node * kHnswBuildSeed) >> 32;
    return Guard{_stripes[h & (_stripes.size() - 1)].lock};
  }

 private:
  struct alignas(64) Stripe {
    std::mutex lock;
  };

  std::vector<Stripe> _stripes;
};

template<typename Dist, typename Sync = HnswNoSync>
void HnswLinkReverse(HnswGraph& graph, Dist& dist, uint32_t peer, uint32_t node,
                     uint32_t level, HnswBuildScratch& s, Sync&& sync = {}) {
  auto guard = sync.Lock(peer);
  auto links = graph.Neighbors(peer, level);
  auto& ids = s.link_ids;
  ids.clear();
  for (const auto id : links) {
    if (id == kHnswInvalidNode) {
      break;
    }
    if (id == node) {
      return;
    }
    ids.push_back(id);
  }

  if (ids.size() < links.size()) {
    HnswStoreLink(links[ids.size()], node);
    return;
  }

  s.peer_scores.resize(ids.size());
  dist.PairBatch(peer, ids, s.peer_scores.data());

  const score_t score = dist.Pair(peer, node);
  size_t pos = 0;
  while (pos < ids.size() && s.peer_scores[pos] > score) {
    ++pos;
  }

  s.node_scores.resize(ids.size());
  if (dist.CheapPair()) {
    for (size_t j = 0; j < pos; ++j) {
      s.node_scores[j] = dist.Pair(node, ids[j]);
      if (s.node_scores[j] > score) {
        return;
      }
    }
    if (pos < ids.size()) {
      dist.PairBatch(node, std::span<const uint32_t>{ids}.subspan(pos),
                     s.node_scores.data() + pos);
    }
  } else {
    dist.PairBatch(node, ids, s.node_scores.data());
    for (size_t j = 0; j < pos; ++j) {
      if (s.node_scores[j] > score) {
        return;
      }
    }
  }

  auto& cur = s.peer_candidates;
  cur.clear();
  for (size_t i = 0; i < pos; ++i) {
    cur.push_back({s.peer_scores[i], ids[i]});
  }
  cur.push_back({score, node});
  for (size_t t = pos; t < ids.size(); ++t) {
    if (s.node_scores[t] <= s.peer_scores[t]) {
      cur.push_back({s.peer_scores[t], ids[t]});
    }
  }
  if (cur.size() > links.size()) {
    cur.resize(links.size());
  }

  for (size_t i = 0; i < links.size(); ++i) {
    HnswStoreLink(links[i], i < cur.size() ? cur[i].node : kHnswInvalidNode);
  }
}

template<typename Dist, typename Sync = HnswNoSync>
void HnswInsert(HnswGraph& graph, uint32_t node, Dist& dist,
                uint32_t ef_construction, HnswBuildScratch& s,
                Sync&& sync = {}) {
  const uint32_t top = graph.LevelOf(node) - 1;
  SDB_ASSERT(!graph.Empty());

  const uint32_t entry = graph.EntryPoint();
  const uint32_t entry_top = graph.LevelOf(entry) - 1;

  HnswCandidate cur{dist.One(entry), entry};
  if (entry_top > top) {
    s.search.visited.Advance();
    cur = HnswGreedyDescent(graph, dist, cur, entry_top, top, s.search);
  }

  s.pending.clear();
  const uint32_t start = std::min(top, entry_top);
  for (uint32_t level = start + 1; level-- > 0;) {
    s.search.visited.Advance();
    s.search.visited.TestAndSet(cur.node);
    s.search.nearest.assign(1, cur);
    HnswSearchLevel(graph, dist, level, ef_construction, s.search);

    auto& found = s.search.nearest;
    std::ranges::sort(found,
                      [](const HnswCandidate& l, const HnswCandidate& r) {
                        return l.score > r.score;
                      });

    const uint32_t width = level == 0 ? graph.M0() : graph.M();
    HnswSelectNeighbors(dist, found, width, s);

    {
      auto guard = sync.Lock(node);
      auto links = graph.Neighbors(node, level);
      for (size_t i = 0; i < links.size(); ++i) {
        HnswStoreLink(links[i],
                      i < s.selected.size() ? s.selected[i] : kHnswInvalidNode);
      }
    }
    for (const auto peer : s.selected) {
      s.pending.push_back({.peer = peer, .level = level});
    }

    cur = found.front();
  }

  for (const auto& [peer, level] : s.pending) {
    HnswLinkReverse(graph, dist, peer, node, level, s, sync);
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
}

template<bool Inclusive, typename Dist>
void HnswSearchRadius(const HnswGraph& graph, Dist& dist, score_t threshold,
                      size_t max_results, HnswSearchScratch& s) {
  const auto accept = [threshold](score_t score) {
    if constexpr (Inclusive) {
      return score >= threshold;
    } else {
      return score > threshold;
    }
  };
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
  found.assign(1, cur);
  HnswSearchLevel(graph, dist, 0, kHnswDefaultEfSearch, s);

  frontier.assign(found.begin(), found.end());
  std::make_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
  found.clear();
  for (const auto& seed : frontier) {
    if (accept(seed.score)) {
      found.push_back(seed);
    }
  }

  while (!frontier.empty() && found.size() < max_results) {
    std::pop_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    const auto node = frontier.back();
    frontier.pop_back();
    if (node.score < threshold) {
      break;
    }

    s.batch.clear();
    const auto neighbors = graph.Neighbors(node.node, 0);
    for (size_t i = 0; i < neighbors.size(); ++i) {
      const auto id = HnswLoadLink(neighbors[i]);
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
    dist.Batch(s.batch, s.scores.data(), kHnswNoThreshold);
    for (size_t i = 0; i < s.batch.size(); ++i) {
      if (s.scores[i] < threshold) {
        continue;
      }
      const HnswCandidate cand{s.scores[i], s.batch[i]};
      if (accept(s.scores[i])) {
        found.push_back(cand);
      }
      frontier.push_back(cand);
      std::push_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    }
  }
}

}  // namespace irs
