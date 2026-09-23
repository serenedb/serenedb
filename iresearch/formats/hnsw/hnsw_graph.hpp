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

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
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
// Rows fed to a quantizer that trains on a bounded sample -- k-means and the
// like, where the cost is in the training and more rows buy little. A trainer
// that streams (scalar quantization, whose training is a per-dimension
// min/max) is given every row instead: there a row the trainer never saw is a
// row the encoder silently clamps.
inline constexpr uint64_t kHnswTrainSample = 262144;
inline constexpr uint32_t kHnswMaxLevel = std::numeric_limits<uint8_t>::max();
inline constexpr uint32_t kHnswFormatVersion = 1;
inline constexpr uint64_t kHnswBuildSeed = 0x9E3779B97F4A7C15ULL;

class HnswVisited {
 public:
  void Reset(size_t n) {
    const size_t words = (n + 63) / 64;
    if (_words.size() < words) {
      _words.assign(words, 0);
      _dirty.clear();
      return;
    }
    Next();
  }

  void Next() noexcept {
    for (const auto w : _dirty) {
      _words[w] = 0;
    }
    _dirty.clear();
  }

  bool TestAndSet(uint32_t id) {
    SDB_ASSERT(id / 64 < _words.size());
    auto& word = _words[id / 64];
    const auto bit = uint64_t{1} << (id % 64);
    if ((word & bit) != 0) {
      return true;
    }
    if (word == 0) {
      _dirty.push_back(id / 64);
    }
    word |= bit;
    return false;
  }

  bool Test(uint32_t id) const noexcept {
    SDB_ASSERT(id / 64 < _words.size());
    return ((_words[id / 64] >> (id % 64)) & 1) != 0;
  }

  void Set(uint32_t id) {
    SDB_ASSERT(id / 64 < _words.size());
    auto& word = _words[id / 64];
    if (word == 0) {
      _dirty.push_back(id / 64);
    }
    word |= uint64_t{1} << (id % 64);
  }

 private:
  std::vector<uint64_t> _words;
  std::vector<uint32_t> _dirty;
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

struct HnswWalkFilterScratch {
  std::vector<uint64_t> known;
  std::vector<uint64_t> pass;
  std::vector<uint32_t> dirty;
  std::vector<doc_id_t> ask;
  std::vector<uint32_t> keep;

  void Grow(size_t words) {
    if (known.size() < words) {
      known.resize(words, 0);
      pass.resize(words, 0);
    }
  }

  void Clear() {
    for (const auto w : dirty) {
      known[w] = 0;
      pass[w] = 0;
    }
    dirty.clear();
  }
};

struct HnswSearchScratch {
  HnswWalkFilterScratch walk_filter;
  HnswVisited visited;
  // Rejected nodes reached as a neighbour's neighbour (TwoHop): explored
  // once from there, yet still handled when reached as a direct neighbour.
  HnswVisited visited_hop2;
  std::vector<HnswCandidate> nearest;
  std::vector<HnswCandidate> frontier;
  std::vector<uint32_t> batch;
  std::vector<score_t> scores;
  // Distances computed by the level walk of the current search.
  uint64_t scored = 0;
};

// The predicate of an unfiltered search: every node may enter the result.
struct HnswAcceptAll {
  static constexpr bool kAll = true;
  bool operator()(uint32_t) const noexcept { return true; }
};

// What the level walk does with a node the predicate rejects.
enum class HnswWalk : uint8_t {
  // Scored and passed through, never admitted: the graph stays connected
  // whatever the predicate, at the price of scoring rejected nodes.
  Through,
  // Neither scored nor expanded: cheap, but the admitted subgraph may fall
  // apart under a selective predicate.
  Prune,
  // Not scored; its neighbours are the candidates instead (ACORN-1), up to
  // the level's width per rejected node.
  TwoHop,
  // Scored and passed through for one hop: a rejected node's expansion only
  // scores its admitted neighbours, so rejected regions are crossed but not
  // explored.
  Bridge,
};

inline constexpr uint64_t kHnswNoBudget = std::numeric_limits<uint64_t>::max();

class HnswGraph {
 public:
  HnswGraph() = default;

  size_t Size() const noexcept { return _levels.size(); }
  uint32_t M() const noexcept { return _m; }
  uint32_t M0() const noexcept { return _m0; }
  uint32_t EntryPoint() const noexcept { return _entry; }
  bool Empty() const noexcept { return EntryPoint() == kHnswInvalidNode; }

  uint32_t LevelOf(uint32_t node) const noexcept { return _levels[node]; }

  std::span<const uint32_t> Neighbors(uint32_t node,
                                      uint32_t level) const noexcept {
    const auto width = level == 0 ? _m0 : _m;
    return {_neighbors.data() + Base(node, level), width};
  }

  void Serialize(DataOutput& out) const;
  static HnswGraph Deserialize(IndexInput& in);

 private:
  friend class HnswGraphWriter;

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

class HnswGraphWriter {
 public:
  HnswGraphWriter() = default;

  void Reset(size_t nodes, uint32_t m);

  void AllocateLinks();

  const HnswGraph& Graph() const noexcept { return _graph; }

  size_t Size() const noexcept { return _graph.Size(); }
  uint32_t M() const noexcept { return _graph.M(); }
  uint32_t M0() const noexcept { return _graph.M0(); }
  uint32_t EntryPoint() const noexcept { return _graph.EntryPoint(); }
  bool Empty() const noexcept { return _graph.Empty(); }
  uint32_t LevelOf(uint32_t node) const noexcept {
    return _graph.LevelOf(node);
  }

  void SetLevel(uint32_t node, uint32_t level) noexcept {
    SDB_ASSERT(level != 0);
    SDB_ASSERT(level <= kHnswMaxLevel);
    _graph._levels[node] = static_cast<uint8_t>(level);
    _graph._max_level = std::max(_graph._max_level, level);
  }

  uint32_t Processed(uint32_t node, uint32_t level) const noexcept {
    if (_processed.empty()) {
      return 0;
    }
    return _processed[_proc_offsets[node] + level];
  }

  void SetProcessed(uint32_t node, uint32_t level, uint32_t n) noexcept {
    if (_processed.empty()) {
      return;
    }
    _processed[_proc_offsets[node] + level] = static_cast<uint8_t>(n);
  }

  void SetEntryPoint(uint32_t node) noexcept { _graph._entry = node; }

  std::span<uint32_t> Neighbors(uint32_t node, uint32_t level) noexcept {
    const auto width = level == 0 ? _graph._m0 : _graph._m;
    return {_graph._neighbors.data() + _graph.Base(node, level), width};
  }

  std::span<const uint32_t> Neighbors(uint32_t node,
                                      uint32_t level) const noexcept {
    return _graph.Neighbors(node, level);
  }

  void Serialize(DataOutput& out) const { _graph.Serialize(out); }

 private:
  HnswGraph _graph;
  std::vector<uint32_t> _proc_offsets;
  std::vector<uint8_t> _processed;
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

inline uint32_t HnswLoadLink(const uint32_t& slot) noexcept {
  return std::atomic_ref<uint32_t>{const_cast<uint32_t&>(slot)}.load(
    std::memory_order_acquire);
}

inline void HnswStoreLink(uint32_t& slot, uint32_t id) noexcept {
  std::atomic_ref<uint32_t>{slot}.store(id, std::memory_order_release);
}

// Best-first expansion of `level` from the seeded heaps: `s.frontier` holds
// the nodes still to expand and `s.nearest` the admitted results, both already
// heaps. The walk passes through every reachable node so a predicate never
// disconnects the graph, but only nodes `accept` passes enter `nearest`, and
// the beam is measured in admitted nodes. Returns false once `budget`
// distances were computed with the frontier still live: the caller then knows
// the predicate is too sparse for the graph and answers another way.
template<HnswWalk Walk = HnswWalk::Through, typename Dist, typename Accept>
bool HnswExpandLevel(const HnswGraph& graph, Dist& dist, uint32_t level,
                     uint32_t ef, HnswSearchScratch& s, const Accept& accept,
                     uint64_t budget = kHnswNoBudget) {
  auto& nearest = s.nearest;
  auto& frontier = s.frontier;

  while (!frontier.empty()) {
    std::pop_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    const auto cur = frontier.back();
    frontier.pop_back();

    if (nearest.size() >= ef && cur.score < nearest.front().score) {
      break;
    }

    s.batch.clear();
    const auto neighbors = graph.Neighbors(cur.node, level);
    const auto width = neighbors.size();
    // Bridge: a node reached through a rejected one only offers its admitted
    // neighbours; an admitted node offers them all, as Through does.
    const bool through = Walk == HnswWalk::Through ||
                         (Walk == HnswWalk::Bridge && accept(cur.node));
    for (size_t i = 0; i < width; ++i) {
      const auto id = HnswLoadLink(neighbors[i]);
      if (id == kHnswInvalidNode) {
        break;
      }
      if (s.visited.TestAndSet(id)) {
        continue;
      }
      if (through) {
        s.batch.push_back(id);
        dist.Prefetch(id);
        continue;
      }
      if (accept(id)) {
        s.batch.push_back(id);
        dist.Prefetch(id);
        continue;
      }
      if constexpr (Walk == HnswWalk::TwoHop) {
        // The rejected node is a bridge: its admitted neighbours stand in
        // for it, at most one level width of them.
        const auto limit = s.batch.size() + width;
        const auto hop = graph.Neighbors(id, level);
        for (size_t j = 0; j < hop.size() && s.batch.size() < limit; ++j) {
          const auto w = HnswLoadLink(hop[j]);
          if (w == kHnswInvalidNode) {
            break;
          }
          if (s.visited.Test(w) || s.visited_hop2.TestAndSet(w)) {
            continue;
          }
          if (accept(w)) {
            s.visited.Set(w);
            s.batch.push_back(w);
            dist.Prefetch(w);
          }
        }
      }
    }
    if (s.batch.empty()) {
      continue;
    }

    s.scores.resize(s.batch.size());
    dist.Batch(s.batch, s.scores.data(),
               nearest.size() >= ef ? nearest.front().score : kHnswNoThreshold);
    s.scored += s.batch.size();

    // An acceptor that answers a whole hop at once says so, and is handed the
    // hop before it is asked about any single node. A predicate only the
    // columnstore answers costs a positioned read per call, so asking about
    // thirty-two scattered nodes one at a time is thirty-two of them; asking
    // once, in ascending order, is one. A plain callable has no Prepare and is
    // untouched.
    if constexpr (requires { accept.Prepare(std::span<const uint32_t>{}); }) {
      accept.Prepare(std::span<const uint32_t>{s.batch});
    }

    for (size_t i = 0; i < s.batch.size(); ++i) {
      const HnswCandidate cand{s.scores[i], s.batch[i]};
      if (nearest.size() >= ef && cand.score <= nearest.front().score) {
        continue;
      }
      if ((Walk != HnswWalk::Through && Walk != HnswWalk::Bridge) ||
          accept(cand.node)) {
        nearest.push_back(cand);
        std::push_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
        if (nearest.size() > ef) {
          std::pop_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
          nearest.pop_back();
        }
      }
      // The candidate's links are what its expansion reads first; fetching
      // them now overlaps that miss with the rest of this batch.
      __builtin_prefetch(graph.Neighbors(cand.node, level).data(), 0, 1);
      frontier.push_back(cand);
      std::push_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    }
    if (s.scored > budget) {
      return false;
    }
  }
  return true;
}

// The classic level search: `s.nearest` seeds the frontier and every node is
// admitted. Used by construction and as the seed pass of a radius search.
template<typename Dist>
void HnswSearchLevel(const HnswGraph& graph, Dist& dist, uint32_t level,
                     uint32_t ef, HnswSearchScratch& s) {
  auto& nearest = s.nearest;
  auto& frontier = s.frontier;
  frontier.assign(nearest.begin(), nearest.end());
  std::make_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
  std::make_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
  HnswExpandLevel(graph, dist, level, ef, s, HnswAcceptAll{});
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

struct HnswReverseItem {
  score_t score;
  uint32_t node;
  bool processed;
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
  std::vector<score_t> select_scores;
  std::vector<HnswReverseItem> rev_items;
  std::vector<uint32_t> rev_kept;
  std::vector<score_t> rev_kept_scores;
  std::vector<uint8_t> rev_kept_processed;
};

// The "not closer than base" test: a candidate is dropped when a link already
// selected sits closer to it than the target does. The comparison is strict
// except between candidates exactly equidistant from the target -- without
// that exception a group of byte-identical vectors never prunes itself, fills
// every slot of every member, and turns the group into a sink the walk cannot
// leave.
inline bool HnswRedundant(score_t to_selected, score_t candidate,
                          score_t selected) noexcept {
  return candidate == selected ? to_selected >= candidate
                               : to_selected > candidate;
}

template<typename Dist>
void HnswSelectNeighbors(Dist& dist, std::span<const HnswCandidate> sorted,
                         uint32_t limit, HnswBuildScratch& s) {
  auto& out = s.selected;
  out.clear();
  SDB_ASSERT(!sorted.empty() && limit != 0);
  if (dist.CheapPair()) {
    auto& out_scores = s.select_scores;
    out_scores.clear();
    for (const auto& cand : sorted) {
      bool keep = true;
      for (size_t w = 0; w < out.size(); ++w) {
        if (HnswRedundant(dist.Pair(cand.node, out[w]), cand.score,
                          out_scores[w])) {
          keep = false;
          break;
        }
      }
      if (!keep) {
        continue;
      }
      out.push_back(cand.node);
      out_scores.push_back(cand.score);
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
      if (HnswRedundant(scores[k], sorted[j].score, sorted[i].score)) {
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

  using Guard = std::unique_lock<absl::Mutex>;

  Guard Lock(uint32_t node) noexcept {
    const auto h = (node * kHnswBuildSeed) >> 32;
    return Guard{_stripes[h & (_stripes.size() - 1)].lock};
  }

 private:
  struct alignas(64) Stripe {
    absl::Mutex lock;
  };

  std::vector<Stripe> _stripes;
};

template<typename Dist, typename Sync = HnswNoSync>
void HnswLinkReverse(HnswGraphWriter& graph, Dist& dist, uint32_t peer,
                     uint32_t node, uint32_t level, HnswBuildScratch& s,
                     Sync&& sync = {}) {
  [[maybe_unused]] auto guard = sync.Lock(peer);
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

  const uint32_t processed = graph.Processed(peer, level);
  s.peer_scores.resize(ids.size());
  dist.PairBatch(peer, ids, s.peer_scores.data());

  auto& items = s.rev_items;
  items.clear();
  items.reserve(ids.size() + 1);
  for (size_t i = 0; i < ids.size(); ++i) {
    items.push_back(
      {.score = s.peer_scores[i], .node = ids[i], .processed = i < processed});
  }
  items.push_back(
    {.score = dist.Pair(peer, node), .node = node, .processed = false});
  // Stable: the links the last heuristic kept are already in its order, and a
  // score tie between two of them must not reshuffle them, or the pairs it
  // cleared below stop lining up.
  std::ranges::stable_sort(
    items, [](const HnswReverseItem& l, const HnswReverseItem& r) {
      return l.score > r.score;
    });

  auto& kept = s.rev_kept;
  auto& kept_scores = s.rev_kept_scores;
  auto& kept_processed = s.rev_kept_processed;
  kept.clear();
  kept_scores.clear();
  kept_processed.clear();
  for (const auto& cand : items) {
    bool keep = true;
    for (size_t w = 0; w < kept.size(); ++w) {
      // Two links a previous heuristic already kept side by side cleared this
      // test then, against the same target and the same scores.
      if (cand.processed && kept_processed[w] != 0) {
        continue;
      }
      if (HnswRedundant(dist.Pair(cand.node, kept[w]), cand.score,
                        kept_scores[w])) {
        keep = false;
        break;
      }
    }
    if (!keep) {
      continue;
    }
    kept.push_back(cand.node);
    kept_scores.push_back(cand.score);
    kept_processed.push_back(cand.processed ? uint8_t{1} : uint8_t{0});
    if (kept.size() >= links.size()) {
      break;
    }
  }

  for (size_t i = 0; i < links.size(); ++i) {
    HnswStoreLink(links[i], i < kept.size() ? kept[i] : kHnswInvalidNode);
  }
  graph.SetProcessed(peer, level, static_cast<uint32_t>(kept.size()));
}

template<typename Dist, typename Sync = HnswNoSync>
void HnswInsert(HnswGraphWriter& graph, uint32_t node, Dist& dist,
                uint32_t ef_construction, HnswBuildScratch& s,
                Sync&& sync = {}) {
  const uint32_t top = graph.LevelOf(node) - 1;

  SDB_ASSERT(!graph.Empty());
  const uint32_t entry = graph.EntryPoint();
  const uint32_t entry_top = graph.LevelOf(entry) - 1;

  HnswCandidate cur{dist.One(entry), entry};
  if (entry_top > top) {
    s.search.visited.Next();
    cur = HnswGreedyDescent(graph.Graph(), dist, cur, entry_top, top, s.search);
  }

  s.pending.clear();
  const uint32_t start = std::min(top, entry_top);
  for (uint32_t level = start + 1; level-- > 0;) {
    s.search.visited.Next();
    s.search.visited.TestAndSet(cur.node);
    s.search.nearest.assign(1, cur);
    HnswSearchLevel(graph.Graph(), dist, level, ef_construction, s.search);

    auto& found = s.search.nearest;
    std::ranges::sort(found,
                      [](const HnswCandidate& l, const HnswCandidate& r) {
                        return l.score > r.score;
                      });

    const uint32_t width = level == 0 ? graph.M0() : graph.M();
    HnswSelectNeighbors(dist, found, width, s);

    {
      [[maybe_unused]] auto guard = sync.Lock(node);
      auto links = graph.Neighbors(node, level);
      for (size_t i = 0; i < links.size(); ++i) {
        HnswStoreLink(links[i],
                      i < s.selected.size() ? s.selected[i] : kHnswInvalidNode);
      }
      graph.SetProcessed(node, level, static_cast<uint32_t>(s.selected.size()));
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

// Top-`ef` search. With a predicate the descent is unfiltered (it only
// navigates), the level-0 walk admits what `accept` passes and treats the
// rest per `Walk`, and `budget` caps the distances the walk may spend; see
// HnswExpandLevel for the false return.
template<HnswWalk Walk = HnswWalk::Through, typename Dist,
         typename Accept = HnswAcceptAll>
bool HnswSearchTopK(const HnswGraph& graph, Dist& dist, uint32_t ef,
                    HnswSearchScratch& s, const Accept& accept = {},
                    uint64_t budget = kHnswNoBudget) {
  s.nearest.clear();
  s.frontier.clear();
  s.scored = 0;
  if (graph.Empty()) {
    return true;
  }
  const uint32_t entry = graph.EntryPoint();
  const uint32_t entry_top = graph.LevelOf(entry) - 1;

  s.visited.Reset(graph.Size());
  if constexpr (Walk == HnswWalk::TwoHop) {
    s.visited_hop2.Reset(graph.Size());
  }
  HnswCandidate cur{dist.One(entry), entry};
  if (entry_top > 0) {
    cur = HnswGreedyDescent(graph, dist, cur, entry_top, 0, s);
  }
  s.visited.Next();
  s.visited.TestAndSet(cur.node);
  // The landing node seeds the frontier whatever the predicate says: the walk
  // must start somewhere, and a Prune walk that could not pass through it
  // would otherwise never leave it.
  s.frontier.assign(1, cur);
  if (accept(cur.node)) {
    s.nearest.assign(1, cur);
  }
  return HnswExpandLevel<Walk>(graph, dist, 0, ef, s, accept, budget);
}

template<bool Inclusive, typename Dist, typename Accept = HnswAcceptAll>
void HnswSearchRadius(const HnswGraph& graph, Dist& dist, score_t threshold,
                      size_t max_results, HnswSearchScratch& s,
                      const Accept& admit = {}) {
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

  s.visited.Next();
  s.visited.TestAndSet(cur.node);
  found.assign(1, cur);
  HnswSearchLevel(graph, dist, 0, kHnswDefaultEfSearch, s);

  frontier.assign(found.begin(), found.end());
  std::make_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
  found.clear();
  for (const auto& seed : frontier) {
    if (accept(seed.score) && admit(seed.node)) {
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
      if (accept(s.scores[i]) && admit(cand.node)) {
        found.push_back(cand);
      }
      frontier.push_back(cand);
      std::push_heap(frontier.begin(), frontier.end(), HnswFrontierOrder{});
    }
  }
}

}  // namespace irs
