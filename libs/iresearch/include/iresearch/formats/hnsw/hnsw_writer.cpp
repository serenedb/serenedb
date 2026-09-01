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

#include "iresearch/formats/hnsw/hnsw_writer.hpp"

#include "iresearch/utils/bytes_output.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <yaclib/async/run.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/assert.h"
#include "iresearch/store/directory.hpp"
#include "iresearch/store/fs_directory.hpp"
#include "basics/down_cast.h"
#include "basics/misc.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

template<VectorMetric M>
struct HnswRawDist {
  const float* base;
  uint32_t d;
  const float* q = nullptr;

  void SetQuery(uint32_t id) noexcept { q = Row(id); }

  const float* Row(uint32_t id) const noexcept {
    return base + static_cast<size_t>(id) * d;
  }

  void BatchFrom(const float* query, std::span<const uint32_t> ids,
                 score_t* out) const noexcept {
    if constexpr (EffectiveQuantMetric(M) == VectorMetric::L2Sqr) {
      HnswBatchL2Sqr(query, base, d, ids, out);
    } else if constexpr (EffectiveQuantMetric(M) ==
                         VectorMetric::InnerProduct) {
      HnswBatchIp(query, base, d, ids, out);
    } else {
      for (size_t i = 0; i < ids.size(); ++i) {
        out[i] = ComputeDistance<EffectiveQuantMetric(M)>(
          query, Row(ids[i]), static_cast<uint16_t>(d));
      }
    }
  }

  score_t One(uint32_t id) const noexcept {
    score_t s{};
    BatchFrom(q, {&id, 1}, &s);
    return s;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t /*threshold*/ = 0.f) const noexcept {
    BatchFrom(q, ids, out);
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }

  score_t Pair(uint32_t a, uint32_t b) const noexcept {
    score_t s{};
    BatchFrom(Row(a), {&b, 1}, &s);
    return s;
  }

  void PairBatch(uint32_t from, std::span<const uint32_t> to,
                 score_t* out) const noexcept {
    BatchFrom(Row(from), to, out);
  }
};

// Largest merge source whose graph we can adopt wholesale, or nullptr. Only
// the donor needs a reusable graph -- every other source's rows are inserted
// as a delta, exactly as a fresh flush would insert them.
struct MergeDonor {
  const HnswIndex* index = nullptr;
  const SubReader* reader = nullptr;
  const DocumentMask* mask = nullptr;
  uint64_t out_base = 0;
  uint64_t alive = 0;
};

// qdrant's healing_threshold: past this share of deleted rows the donor's link
// structure has decayed enough that healing costs more than it saves.
inline constexpr double kHnswMaxMissingRatio = 0.3;

MergeDonor PickMergeDonor(std::span<const MergeSource> sources, field_id column,
                          uint32_t d, VectorMetric metric, uint32_t m) {
  MergeDonor best;
  uint64_t out_base = 0;
  for (const auto& src : sources) {
    const uint64_t base = out_base;
    out_base += src.alive_count;
    if (src.reader == nullptr || src.alive_count <= best.alive) {
      continue;
    }
    const auto* ann = src.reader->Ann(column);
    if (ann == nullptr || ann->Kind() != AnnKind::Hnsw || ann->Empty()) {
      continue;
    }
    const auto& hnsw = sdb::basics::downCast<const HnswIndex>(*ann);
    const auto& header = hnsw.Header();
    if (header.d != d || header.metric != metric || header.rows == 0) {
      continue;
    }
    const double missing = 1.0 - static_cast<double>(src.alive_count) /
                                   static_cast<double>(header.rows);
    if (missing > kHnswMaxMissingRatio) {
      continue;
    }
    best = MergeDonor{.index = &hnsw,
                      .reader = src.reader,
                      .mask = src.mask,
                      .out_base = base,
                      .alive = src.alive_count};
  }
  if (best.index != nullptr) {
    auto data = best.index->Load(*best.reader);
    if (!data || data->graph.Empty() || data->graph.M() != m) {
      return {};
    }
  }
  return best;
}

struct HealScratch {
  std::vector<HnswCandidate> pool;
  std::vector<uint32_t> stack;
  HnswVisited seen;
  HnswVisited walked;
};

// Re-select a node's neighbours at `level` after some were dropped. Candidates
// are the surviving links plus the frontier found by walking the DONOR graph
// *through* the deleted nodes -- the survivors a deleted node used to bridge to
// are unreachable in the new graph, since it no longer contains that node.
template<typename Dist>
void HealNode(HnswGraph& graph, const HnswGraph& src_graph,
              std::span<const uint32_t> remap, Dist& dist, uint32_t src_node,
              uint32_t node, uint32_t level, HnswBuildScratch& scratch,
              HealScratch& heal) {
  auto links = graph.Neighbors(node, level);
  auto& pool = heal.pool;
  auto& stack = heal.stack;
  auto& seen = heal.seen;
  auto& walked = heal.walked;
  pool.clear();
  seen.Advance();
  walked.Advance();
  seen.TestAndSet(node);
  for (const auto id : links) {
    if (id == kHnswInvalidNode) {
      break;
    }
    if (!seen.TestAndSet(id)) {
      pool.push_back({dist.Pair(node, id), id});
    }
  }

  stack.clear();
  for (const auto id : src_graph.Neighbors(src_node, level)) {
    if (id == kHnswInvalidNode) {
      break;
    }
    if (remap[id] == kHnswInvalidNode) {
      stack.push_back(id);
    }
  }
  while (!stack.empty()) {
    const auto cur = stack.back();
    stack.pop_back();
    if (walked.TestAndSet(cur)) {
      continue;
    }
    for (const auto nb : src_graph.Neighbors(cur, level)) {
      if (nb == kHnswInvalidNode) {
        break;
      }
      const auto mapped = remap[nb];
      if (mapped == kHnswInvalidNode) {
        stack.push_back(nb);
      } else if (!seen.TestAndSet(mapped)) {
        pool.push_back({dist.Pair(node, mapped), mapped});
      }
    }
  }

  if (pool.empty()) {
    return;
  }
  std::ranges::sort(pool, [](const HnswCandidate& l, const HnswCandidate& r) {
    return l.score > r.score;
  });
  HnswSelectNeighbors(dist, pool, static_cast<uint32_t>(links.size()),
                      scratch);
  for (size_t i = 0; i < links.size(); ++i) {
    HnswStoreLink(links[i], i < scratch.selected.size()
                              ? scratch.selected[i]
                              : kHnswInvalidNode);
  }
  for (const auto peer : scratch.selected) {
    HnswLinkReverse(graph, dist, peer, node, level, scratch);
  }
}

// Streams the column in batches, normalising in place when the metric asks for
// it. Called twice -- once to train and once to encode -- so that the raw
// matrix never has to be held whole.
template<typename Fn>
uint64_t ScanVectors(const ColumnReader& col, ReadContext& ctx, uint64_t rows,
                     uint32_t d, bool normalize, std::vector<float>& buf,
                     Fn&& fn) {
  ColumnReader::VectorScratch scratch{col.Type()};
  auto scan = col.InitScan(ctx);
  for (uint64_t done = 0; done < rows;) {
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - done));
    auto& batch = scratch.Reset();
    col.Scan(scan, batch, take);
    const auto& mask = duckdb::FlatVector::Validity(batch);
    const auto* src =
      duckdb::FlatVector::GetData<float>(duckdb::ArrayVector::GetChild(batch));
    const size_t chunk = static_cast<size_t>(take) * d;
    buf.assign(src, src + chunk);
    if (normalize) {
      for (duckdb::idx_t i = 0; i < take; ++i) {
        float* v = buf.data() + static_cast<size_t>(i) * d;
        vector::L2Space<float, float, float>::Normalize(
          reinterpret_cast<const byte_type*>(v), static_cast<uint16_t>(d), v);
      }
    }
    fn(buf.data(), static_cast<size_t>(take), done, mask);
    done += take;
  }
  return rows;
}

template<VectorMetric M>
struct HnswRawDistFactory {
  const float* base;
  uint32_t d;

  using Dist = HnswRawDist<M>;

  Dist Make() const { return Dist{.base = base, .d = d}; }
};

// Scores stored rows against each other through their codes. The inserted
// node's query and the pairwise `from` need different keys and interleave --
// the heuristic re-keys between the levels of one insertion -- so each gets its
// own reader rather than one that would clobber the other.
struct HnswCodeBuildDist {
  const byte_type* codes = nullptr;
  uint32_t record_size = 0;
  uint32_t d = 0;
  std::shared_ptr<const QuantizerCodebook> query_book;
  std::shared_ptr<const QuantizerCodebook> pair_book;
  std::unique_ptr<QuantizerReader> query_reader;
  std::unique_ptr<QuantizerReader> pair_reader;
  // Separate buffers, not one shared scratch: a scalar reader's SetQuery keeps
  // the pointer it is handed, so re-keying the pair side would otherwise
  // repoint the query side at the pair vector.
  std::vector<float> decoded_query;
  std::vector<float> decoded_pair;
  uint32_t pair_key = kHnswInvalidNode;

  const byte_type* Row(uint32_t id) const noexcept {
    return codes + static_cast<size_t>(id) * record_size;
  }

  void SetQuery(uint32_t id) {
    query_reader->Decode(Row(id), decoded_query.data());
    query_reader->SetQuery(decoded_query);
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t threshold = kHnswNoThreshold) {
    query_reader->ComputeGathered(codes, record_size, ids, threshold, out);
  }

  score_t One(uint32_t id) {
    score_t s{};
    Batch({&id, 1}, &s);
    return s;
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }

  void RekeyPair(uint32_t from) {
    if (pair_key == from) {
      return;
    }
    pair_reader->Decode(Row(from), decoded_pair.data());
    pair_reader->SetQuery(decoded_pair);
    pair_key = from;
  }

  score_t Pair(uint32_t a, uint32_t b) {
    RekeyPair(a);
    score_t s{};
    pair_reader->ComputeGathered(codes, record_size, {&b, 1}, kHnswNoThreshold,
                                 &s);
    return s;
  }

  void PairBatch(uint32_t from, std::span<const uint32_t> to, score_t* out) {
    RekeyPair(from);
    pair_reader->ComputeGathered(codes, record_size, to, kHnswNoThreshold, out);
  }
};

struct HnswCodeDistFactory {
  const byte_type* codes;
  uint32_t record_size;
  const QuantizerStats* stats;
  const float* centroid;
  uint32_t d;

  using Dist = HnswCodeBuildDist;

  // A reader that cannot re-key from a stored code would score every candidate
  // against a zero query, so prove it works before the graph depends on it.
  bool CanRekey() const {
    auto dist = Make();
    const bool dec = dist.query_reader->Decode(codes, dist.decoded_query.data());
    return dec && dist.query_reader->SetQuery(dist.decoded_query);
  }

  Dist Make() const {
    Dist dist;
    dist.codes = codes;
    dist.record_size = record_size;
    dist.d = d;
    dist.decoded_query.assign(d, 0.f);
    dist.decoded_pair.assign(d, 0.f);
    dist.query_book = stats->MakeCodebook(dist.decoded_query);
    dist.pair_book = stats->MakeCodebook(dist.decoded_query);
    dist.query_reader = MakeQuantizerReader(dist.query_book);
    dist.pair_reader = MakeQuantizerReader(dist.pair_book);
    dist.query_reader->StartCluster(centroid);
    dist.pair_reader->StartCluster(centroid);
    return dist;
  }
};

// Shared state for a parallel insert pass. Workers pull granules off one cursor
// rather than taking a static slice: insert cost grows with the graph, so an
// even split by count leaves the last worker running alone.
template<typename Factory>
struct HnswInsertJob {
  HnswInsertJob(HnswGraph& graph, const Factory& factory, uint32_t ef,
                std::span<const uint32_t> nodes, size_t workers, size_t rows)
    : _graph{graph}, _ef{ef}, _nodes{nodes}, _scratch(workers) {
    _dists.reserve(workers);
    for (size_t i = 0; i < workers; ++i) {
      _dists.push_back(factory.Make());
    }
    for (auto& s : _scratch) {
      s.search.visited.Reset(rows);
    }
  }

  void RunWorker(size_t worker) noexcept {
    auto& scratch = _scratch[worker];
    auto& dist = _dists[worker];
    for (auto begin = _cursor.fetch_add(kHnswInsertGranule);
         begin < _nodes.size();
         begin = _cursor.fetch_add(kHnswInsertGranule)) {
      const auto end = std::min(begin + kHnswInsertGranule, _nodes.size());
      for (auto i = begin; i < end; ++i) {
        const auto node = _nodes[i];
        SDB_ASSERT(_graph.LevelOf(node) <=
                   _graph.LevelOf(_graph.EntryPoint()));
        dist.SetQuery(node);
        HnswInsert(_graph, node, dist, _ef, scratch, _sync);
      }
    }
  }

 private:
  HnswGraph& _graph;
  uint32_t _ef;
  std::span<const uint32_t> _nodes;
  HnswStripeSync _sync;
  std::atomic<size_t> _cursor{0};
  std::vector<HnswBuildScratch> _scratch;
  std::vector<typename Factory::Dist> _dists;
};

// Insert `nodes`, fanning out onto the pool when the env grants helpers. The
// first `warmup` go in serially: until the graph has structure every writer
// lands in the same neighbourhood and serializes on the same rows anyway.
template<typename Factory>
auto InsertNodes(HnswGraph& graph, const Factory& factory, uint32_t ef,
                 std::span<const uint32_t> nodes, size_t rows, uint32_t warmup,
                 const AnnBuildEnv* env) -> yaclib::Future<> {
  const auto want = static_cast<uint32_t>(
    std::min<size_t>(kHnswMaxHelpers, nodes.size() / kHnswMinRowsPerWorker));
  const uint32_t helpers =
    (env != nullptr && env->executor != nullptr && want != 0) ? env->acquire(want)
                                                              : 0;
  absl::Cleanup release_helpers = [&] {
    if (helpers != 0) {
      env->release(helpers);
    }
  };

  const size_t serial =
    helpers == 0 ? nodes.size() : std::min<size_t>(nodes.size(), warmup);
  {
    auto dist = factory.Make();
    HnswBuildScratch scratch;
    scratch.search.visited.Reset(rows);
    for (size_t i = 0; i < serial; ++i) {
      dist.SetQuery(nodes[i]);
      HnswInsert(graph, nodes[i], dist, ef, scratch);
    }
  }
  if (serial == nodes.size()) {
    co_return {};
  }

  HnswInsertJob<Factory> job{graph, factory,      ef, nodes.subspan(serial),
                             helpers + 1UL, rows};
  std::vector<yaclib::FutureOn<>> runs;
  runs.reserve(helpers);
  for (uint32_t i = 0; i < helpers; ++i) {
    runs.push_back(yaclib::Run(*env->executor,
                               [&job, w = size_t{i} + 1] { job.RunWorker(w); }));
  }
  job.RunWorker(0);
  co_await yaclib::Await(runs.begin(), runs.end());
  // A helper dropped after the pool stopped never claimed anything, so the
  // cursor can still hold work; draining again costs nothing when it does not.
  job.RunWorker(0);
  co_return {};
}

// Seed `graph` from the donor's links (remapped to output ids, dropping links
// to deleted rows), heal the nodes that lost neighbours, then insert every
// remaining row. Returns false if the donor is unusable, leaving `graph` for
// the caller to build from scratch.
template<typename Factory>
auto BuildGraphFromMerge(HnswGraph& graph, const Factory& factory,
                         std::span<const uint8_t> valid, uint32_t m,
                         uint32_t ef_construction, uint64_t seed,
                         const MergeDonor& donor, const AnnBuildEnv* env)
  -> yaclib::Future<bool> {
  auto data = donor.index->Load(*donor.reader);
  if (!data) {
    co_return false;
  }
  const auto& src_graph = data->graph;
  const auto src_rows = src_graph.Size();
  const auto rows = valid.size();

  std::vector<uint32_t> remap(src_rows, kHnswInvalidNode);
  uint64_t rank = 0;
  for (size_t r = 0; r < src_rows; ++r) {
    const auto doc = static_cast<doc_id_t>(r) + doc_limits::min();
    if (donor.mask != nullptr && donor.mask->contains(doc)) {
      continue;
    }
    if (donor.out_base + rank >= rows) {
      co_return false;
    }
    remap[r] = static_cast<uint32_t>(donor.out_base + rank);
    ++rank;
  }
  if (rank != donor.alive) {
    co_return false;
  }

  graph.Reset(rows, m);
  uint64_t rng = seed;
  for (size_t r = 0; r < src_rows; ++r) {
    if (remap[r] != kHnswInvalidNode &&
        src_graph.LevelOf(static_cast<uint32_t>(r)) != 0) {
      graph.SetLevel(remap[r], src_graph.LevelOf(static_cast<uint32_t>(r)));
    }
  }
  const auto donor_end = donor.out_base + donor.alive;
  for (size_t i = 0; i < rows; ++i) {
    if (i >= donor.out_base && i < donor_end) {
      continue;
    }
    if (valid[i] != 0) {
      graph.SetLevel(static_cast<uint32_t>(i), HnswRandomLevel(rng, m));
    }
  }
  graph.AllocateLinks();

  std::vector<std::array<uint32_t, 3>> to_heal;
  uint32_t entry = kHnswInvalidNode;
  uint32_t entry_level = 0;
  for (size_t r = 0; r < src_rows; ++r) {
    const auto node = remap[r];
    if (node == kHnswInvalidNode) {
      continue;
    }
    const auto levels = src_graph.LevelOf(static_cast<uint32_t>(r));
    if (levels == 0) {
      continue;
    }
    if (levels > entry_level) {
      entry_level = levels;
      entry = node;
    }
    for (uint32_t level = 0; level < levels; ++level) {
      auto in = src_graph.Neighbors(static_cast<uint32_t>(r), level);
      auto out = graph.Neighbors(node, level);
      size_t k = 0;
      bool lost = false;
      for (const auto id : in) {
        if (id == kHnswInvalidNode) {
          break;
        }
        const auto mapped = remap[id];
        if (mapped == kHnswInvalidNode) {
          lost = true;
          continue;
        }
        if (k < out.size()) {
          out[k++] = mapped;
        }
      }
      for (size_t j = k; j < out.size(); ++j) {
        out[j] = kHnswInvalidNode;
      }
      if (lost) {
        to_heal.push_back({static_cast<uint32_t>(r), node, level});
      }
    }
  }
  if (entry == kHnswInvalidNode) {
    co_return false;
  }
  graph.SetEntryPoint(entry);

  // Delta rows draw fresh levels and can out-rank every donor survivor. The
  // highest such row is inserted before the rest so it, not they, raises the
  // entry -- afterwards no insert can, so SetEntryPoint stays off the insert
  // path. Pinning the donor entry instead would leave the taller delta rows
  // with their top levels never searched.
  uint32_t delta_entry = kHnswInvalidNode;
  uint32_t delta_levels = entry_level;
  for (size_t i = 0; i < rows; ++i) {
    if ((i >= donor.out_base && i < donor_end) || valid[i] == 0) {
      continue;
    }
    const auto node = static_cast<uint32_t>(i);
    if (graph.LevelOf(node) > delta_levels) {
      delta_levels = graph.LevelOf(node);
      delta_entry = node;
    }
  }

  auto dist = factory.Make();
  HnswBuildScratch scratch;
  scratch.search.visited.Reset(rows);
  HealScratch heal;
  heal.seen.Reset(rows);
  heal.walked.Reset(src_rows);
  for (const auto& [src_node, node, level] : to_heal) {
    dist.SetQuery(node);
    HealNode(graph, src_graph, remap, dist, src_node, node, level, scratch,
             heal);
  }

  if (delta_entry != kHnswInvalidNode) {
    dist.SetQuery(delta_entry);
    HnswInsert(graph, delta_entry, dist, ef_construction, scratch);
  }

  std::vector<uint32_t> nodes;
  nodes.reserve(rows);
  for (size_t i = 0; i < rows; ++i) {
    if ((i >= donor.out_base && i < donor_end) || valid[i] == 0 ||
        static_cast<uint32_t>(i) == delta_entry) {
      continue;
    }
    nodes.push_back(static_cast<uint32_t>(i));
  }

  // No warm-up: the donor's seeded and healed graph is already the warm graph.
  co_await InsertNodes(graph, factory, ef_construction, nodes, rows,
                          /*warmup=*/0, env);
  co_return true;
}

template<typename Factory>
auto BuildGraph(HnswGraph& graph, const Factory& factory,
                std::span<const uint8_t> valid, uint32_t m,
                uint32_t ef_construction, uint64_t seed,
                const AnnBuildEnv* env) -> yaclib::Future<> {
  const auto rows = valid.size();
  graph.Reset(rows, m);

  uint64_t rng = seed;
  for (size_t i = 0; i < rows; ++i) {
    if (valid[i] != 0) {
      graph.SetLevel(static_cast<uint32_t>(i), HnswRandomLevel(rng, m));
    }
  }
  graph.AllocateLinks();

  // Levels are final before any insert, so the highest-level node can be the
  // entry from the start. No insert can then raise the entry, which keeps
  // SetEntryPoint off the insert path entirely. Like the node that used to seed
  // an empty graph, it is not inserted -- its links accrue from peers.
  uint32_t entry = kHnswInvalidNode;
  uint32_t entry_levels = 0;
  for (size_t i = 0; i < rows; ++i) {
    if (valid[i] == 0) {
      continue;
    }
    const auto node = static_cast<uint32_t>(i);
    if (graph.LevelOf(node) > entry_levels) {
      entry_levels = graph.LevelOf(node);
      entry = node;
    }
  }
  if (entry == kHnswInvalidNode) {
    co_return {};
  }
  graph.SetEntryPoint(entry);

  std::vector<uint32_t> nodes;
  nodes.reserve(rows);
  for (size_t i = 0; i < rows; ++i) {
    if (valid[i] == 0 || static_cast<uint32_t>(i) == entry) {
      continue;
    }
    nodes.push_back(static_cast<uint32_t>(i));
  }

  co_await InsertNodes(graph, factory, ef_construction, nodes, rows,
                          std::max(kHnswSerialWarmup, ef_construction), env);
  co_return {};
}

// Named coroutine rather than a coroutine lambda inside ResolveEnum: a lambda
// coroutine captures by pointer, and the closure would be gone by the first
// resume. The lambda below only stores the Future, which owns its own frame.
template<typename Factory>
auto BuildDispatch(HnswGraph& graph, const Factory& factory,
                   std::span<const uint8_t> valid, uint32_t m, uint32_t ef,
                   uint64_t seed, const MergeDonor& donor,
                   const AnnBuildEnv* env) -> yaclib::Future<> {
  if (donor.index != nullptr &&
      co_await BuildGraphFromMerge(graph, factory, valid, m, ef, seed, donor,
                                   env)) {
    co_return {};
  }
  co_await BuildGraph(graph, factory, valid, m, ef, seed, env);
  co_return {};
}

}  // namespace

HnswWriter::HnswWriter(AnnInfo info) : _info{std::move(info)} {}

HnswWriter::~HnswWriter() = default;

auto HnswWriter::Compute(const ColumnReader& col, ReadContext& ctx,
                         const AnnBuildEnv* env) -> yaclib::Future<> {
  _d = static_cast<uint32_t>(col.ArraySize());
  _rows = col.RowCount();
  if (_d == 0 || _rows == 0) {
    co_return {};
  }

  std::vector<uint8_t> valid(_rows, 1);
  std::vector<float> batch_buf;
  const bool normalize = _info.metric == VectorMetric::Cosine;

  if (_info.quant.kind != VectorQuantization::None) {
    _qw = MakeQuantizerWriter(_info.quant.kind, _d,
                              EffectiveQuantMetric(_info.metric),
                              _info.quant.pq_m, 0, _info.quant.nb_bits,
                              /*row_major=*/true);
  }
  const bool train =
    _qw && _qw->TrainSamples(_rows) == QuantizerWriter::kTrainStreaming;
  const bool needs_centroid =
    _qw && QuantizerNeedsCentroid(_info.quant.kind);
  if (needs_centroid) {
    _centroid.assign(_d, 0.f);
  }

  // The whole column is mapped, so every pass over it makes the segment
  // resident -- 1.65 GiB per 500k rows. Training and the centroid converge long
  // before the last row, so bound that pass and let only the encode read all of
  // it; without this the column is faulted in twice and the cgroup pays for both.
  uint64_t trained_rows = 0;
  if (_qw) {
    const uint64_t sample = std::min<uint64_t>(_rows, kHnswTrainSample);
    trained_rows = ScanVectors(
      col, ctx, sample, _d, normalize, batch_buf,
      [&](const float* rows, size_t n, uint64_t, const duckdb::ValidityMask&) {
        if (train) {
          _qw->Train(rows, n);
        }
        if (needs_centroid) {
          for (size_t i = 0; i < n; ++i) {
            const float* row = rows + i * size_t{_d};
            for (uint32_t j = 0; j < _d; ++j) {
              _centroid[j] += row[j];
            }
          }
        }
      });
  }

  if (needs_centroid && trained_rows != 0) {
    const float inv = 1.f / static_cast<float>(trained_rows);
    for (uint32_t j = 0; j < _d; ++j) {
      _centroid[j] *= inv;
    }
    _qw->SetClusterCentroid(_centroid.data());
  }

  bool encoding = _qw && _qw->BlockSetting().group_size == 1;
  if (encoding) {
    _record_size = _qw->BlockSetting().record_size;
    _codes.resize(static_cast<size_t>(_rows) * _record_size);
  } else if (!_qw) {
    _vectors.resize(static_cast<size_t>(_rows) * _d);
  }

  ScanVectors(col, ctx, _rows, _d, normalize, batch_buf,
              [&](const float* rows, size_t n, uint64_t first,
                  const duckdb::ValidityMask& mask) {
                for (size_t i = 0; i < n; ++i) {
                  if (!mask.RowIsValid(i)) {
                    valid[first + i] = 0;
                  }
                }
                if (encoding) {
                  encoding = _qw->EncodeInto(
                    _codes.data() + first * _record_size, rows, n);
                } else if (!_qw) {
                  std::memcpy(_vectors.data() + first * _d, rows,
                              n * _d * sizeof(float));
                }
              });

  std::shared_ptr<const QuantizerStats> stats;
  if (encoding) {
    BytesOutput blob_out{_stats_blob};
    _qw->Serialize(blob_out);
    stats = MakeQuantizerStats(
      _info.quant.kind, _d,
      std::span<const byte_type>{_stats_blob}.subspan(sizeof(uint64_t)),
      EffectiveQuantMetric(_info.metric), /*row_major=*/true);
  }
  if (!stats) {
    _codes.clear();
    _codes.shrink_to_fit();
  }

  const auto m = _info.m != 0 ? _info.m : kHnswDefaultM;
  const auto ef = _info.ef_construction != 0 ? _info.ef_construction
                                             : kHnswDefaultEfConstruction;
  const auto donor =
    PickMergeDonor(_merge_sources, _info.centroids_id, _d, _info.metric, m);

  if (stats) {
    HnswCodeDistFactory factory{.codes = _codes.data(),
                                .record_size = _record_size,
                                .stats = stats.get(),
                                .centroid =
                                  _centroid.empty() ? nullptr
                                                    : _centroid.data(),
                                .d = _d};
    if (factory.CanRekey()) {
      co_await BuildDispatch(_graph, factory, valid, m, ef, kHnswBuildSeed,
                             donor, env);
      co_return {};
    }
    _codes.clear();
    _codes.shrink_to_fit();
    _stats_blob.clear();
  }

  // No usable code layout: fall back to scoring the raw vectors, which means
  // materialising them.
  if (_qw) {
    _vectors.resize(static_cast<size_t>(_rows) * _d);
    ScanVectors(col, ctx, _rows, _d, normalize, batch_buf,
                [&](const float* rows, size_t n, uint64_t first,
                    const duckdb::ValidityMask&) {
                  std::memcpy(_vectors.data() + first * _d, rows,
                              n * _d * sizeof(float));
                });
  }
  yaclib::Future<> built;
  ResolveEnum<VectorMetric>(_info.metric, [&]<VectorMetric M>() {
    HnswRawDistFactory<M> factory{.base = _vectors.data(), .d = _d};
    built = BuildDispatch(_graph, factory, valid, m, ef, kHnswBuildSeed, donor,
                          env);
  });
  SDB_ASSERT(built.Valid());
  co_await std::move(built);
  co_return {};
}

void HnswWriter::Flush() {
  if (_graph.Empty()) {
    return;
  }
  SDB_ASSERT(_idx != nullptr);

  auto& out = _idx->BlocksOut();
  const uint64_t offset = out.Position();

  out.WriteU32(kHnswFormatVersion);
  out.WriteU32(_d);
  out.WriteU32(static_cast<uint32_t>(_info.metric));
  out.WriteU32(static_cast<uint32_t>(_info.quant.kind));
  out.WriteU32(_info.ef_construction);
  out.WriteU32(_record_size);
  out.WriteU64(_rows);
  _graph.Serialize(out);
  if (_qw) {
    _qw->Serialize(out);
    if (!_centroid.empty()) {
      out.WriteData(reinterpret_cast<const byte_type*>(_centroid.data()),
                    _centroid.size() * sizeof(float));
    }
    if (!_codes.empty()) {
      out.WriteData(_codes.data(), _codes.size());
    } else {
      _qw->Encode(out, _vectors.data(), _rows);
      _qw->Finish(out);
    }
  } else {
    out.WriteData(reinterpret_cast<const byte_type*>(_vectors.data()),
                  static_cast<size_t>(_rows) * _d * sizeof(float));
  }

  _idx->AddHnsw(
    _info.centroids_id,
    HnswMeta{.offset = offset, .byte_size = out.Position() - offset});

  _vectors.clear();
  _vectors.shrink_to_fit();
  _codes.clear();
  _codes.shrink_to_fit();
  _stats_blob.clear();
  _stats_blob.shrink_to_fit();
}

}  // namespace irs
