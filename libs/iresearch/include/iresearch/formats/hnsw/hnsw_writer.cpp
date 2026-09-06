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

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <yaclib/async/run.hpp>
#include <yaclib/async/wait.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "basics/topic.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/vector.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

template<VectorMetric M>
struct HnswRawDist {
  const float* base;
  uint32_t d;
  const float* q = nullptr;

  void SetQuery(uint32_t id) noexcept { q = Row(id); }

  void SetWindow(const float*, uint32_t, uint32_t) noexcept {}

  const float* Row(uint32_t id) const noexcept {
    return base + static_cast<size_t>(id) * d;
  }

  void BatchFrom(const float* query, std::span<const uint32_t> ids,
                 score_t* out) const noexcept {
    HnswComputeDistances<M>(query, base, d, ids, out);
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

  // No query state to build: a pair is one distance over two stored rows.
  static constexpr bool CheapPair() noexcept { return true; }
};

struct MergeDonor {
  const HnswIndex* index = nullptr;
  const SubReader* reader = nullptr;
  const DocumentMask* mask = nullptr;
  uint64_t out_base = 0;
  uint64_t alive = 0;
};

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
  HnswSelectNeighbors(dist, pool, static_cast<uint32_t>(links.size()), scratch);
  for (size_t i = 0; i < links.size(); ++i) {
    HnswStoreLink(links[i], i < scratch.selected.size() ? scratch.selected[i]
                                                        : kHnswInvalidNode);
  }
  for (const auto peer : scratch.selected) {
    HnswLinkReverse(graph, dist, peer, node, level, scratch);
  }
}

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

inline constexpr uint64_t kHnswMinRowsPerEncoder = 32768;

bool EncodeColumn(const ColumnReader& col, ReadContext& ctx, uint64_t rows,
                  uint32_t d, bool normalize, QuantizerWriter& qw,
                  byte_type* codes, uint32_t record_size,
                  std::vector<uint8_t>& valid, const AnnBuildEnv* env) {
  const bool can_fan_out = env != nullptr && env->executor != nullptr;
  const auto want = can_fan_out
                      ? static_cast<uint32_t>(std::clamp<uint64_t>(
                          rows / kHnswMinRowsPerEncoder, 1, kHnswMaxWorkers))
                      : 1U;
  const uint32_t workers = want > 1 ? env->acquire(want) : 1;
  absl::Cleanup release_workers = [&] {
    if (want > 1) {
      env->release(workers);
    }
  };

  std::vector<std::unique_ptr<QuantizerWriter>> spare;
  for (uint32_t i = 1; i < workers; ++i) {
    auto clone = qw.CloneForEncode();
    if (!clone) {
      break;
    }
    spare.push_back(std::move(clone));
  }
  const size_t parts = spare.size() + 1;

  std::vector<float> owned;
  std::vector<yaclib::FutureOn<>> runs;
  runs.reserve(spare.size());
  ColumnReader::VectorScratch scratch{col.Type()};
  auto scan = col.InitScan(ctx);
  std::atomic<bool> ok{true};

  for (uint64_t done = 0; done < rows;) {
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - done));
    auto& batch = scratch.Reset();
    col.Scan(scan, batch, take);
    const auto& mask = duckdb::FlatVector::Validity(batch);
    auto* src =
      duckdb::FlatVector::GetData<float>(duckdb::ArrayVector::GetChild(batch));
    const float* vecs = src;
    if (normalize) {
      owned.assign(src, src + static_cast<size_t>(take) * d);
      vecs = owned.data();
    }
    for (duckdb::idx_t i = 0; i < take; ++i) {
      if (!mask.RowIsValid(i)) {
        valid[done + i] = 0;
      }
    }

    const size_t n = take;
    const auto run_part = [&](size_t w) {
      const size_t lo = n * w / parts;
      const size_t hi = n * (w + 1) / parts;
      if (lo >= hi) {
        return;
      }
      if (normalize) {
        for (size_t i = lo; i < hi; ++i) {
          float* v = owned.data() + i * size_t{d};
          vector::L2Space<float, float, float>::Normalize(
            reinterpret_cast<const byte_type*>(v), static_cast<uint16_t>(d), v);
        }
      }
      auto& writer = w == 0 ? qw : *spare[w - 1];
      if (!writer.EncodeInto(codes + (done + lo) * size_t{record_size},
                             vecs + lo * size_t{d}, hi - lo)) {
        ok.store(false, std::memory_order_relaxed);
      }
    };

    runs.clear();
    for (size_t w = 1; w < parts; ++w) {
      runs.push_back(
        yaclib::Run(*env->executor, [&run_part, w] { run_part(w); }));
    }
    run_part(0);
    if (!runs.empty()) {
      yaclib::Wait(runs.begin(), runs.end());
    }
    if (!ok.load(std::memory_order_relaxed)) {
      return false;
    }
    done += take;
  }
  return true;
}

template<VectorMetric M>
struct HnswRawDistFactory {
  const float* base;
  uint32_t d;

  using Dist = HnswRawDist<M>;

  Dist Make() const { return Dist{.base = base, .d = d}; }
};

struct HnswCodeBuildDist {
  const byte_type* codes = nullptr;
  uint32_t record_size = 0;
  uint32_t d = 0;
  std::span<const float> pair_terms;
  std::shared_ptr<const QuantizerCodebook> query_book;
  std::unique_ptr<QuantizerReader> query_reader;
  std::vector<float> decoded_query;
  bool symmetric = false;
  std::shared_ptr<const QuantizerCodebook> pair_book;
  std::unique_ptr<QuantizerReader> pair_reader;
  std::vector<float> decoded_pair;
  uint32_t pair_key = kHnswInvalidNode;
  const float* window = nullptr;
  uint32_t window_first = 0;
  uint32_t window_count = 0;

  const byte_type* Row(uint32_t id) const noexcept {
    return codes + static_cast<size_t>(id) * record_size;
  }

  void SetWindow(const float* base, uint32_t first, uint32_t count) noexcept {
    window = base;
    window_first = first;
    window_count = count;
  }

  void SetQuery(uint32_t id) {
    if (window != nullptr && id >= window_first &&
        id - window_first < window_count) {
      query_reader->SetQuery(
        {window + static_cast<size_t>(id - window_first) * d, d});
      return;
    }
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

  score_t Pair(uint32_t a, uint32_t b) {
    score_t s{};
    PairBatch(a, {&b, 1}, &s);
    return s;
  }

  void PairBatch(uint32_t from, std::span<const uint32_t> to, score_t* out) {
    if (symmetric) {
      query_reader->ScorePairBatch(codes, record_size, pair_terms, from, to,
                                   out);
      return;
    }
    if (pair_key != from) {
      pair_reader->Decode(Row(from), decoded_pair.data());
      pair_reader->SetQuery(decoded_pair);
      pair_key = from;
    }
    pair_reader->ComputeGathered(codes, record_size, to, kHnswNoThreshold, out);
  }

  bool CheapPair() const noexcept { return symmetric; }
};

struct HnswCodeDistFactory {
  const byte_type* codes;
  uint32_t record_size;
  const QuantizerStats* stats;
  const float* centroid;
  uint32_t d;
  std::span<const float> pair_terms;

  using Dist = HnswCodeBuildDist;

  bool CanRekey() const {
    auto dist = Make();
    const bool dec =
      dist.query_reader->Decode(codes, dist.decoded_query.data());
    return dec && dist.query_reader->SetQuery(dist.decoded_query);
  }

  bool PreparePairTerms(uint64_t rows, std::vector<float>& terms) const {
    auto reader = MakeReader();
    if (!reader->SupportsPairScores()) {
      return false;
    }
    return reader->PreparePairTerms(codes, record_size, rows, terms);
  }

  Dist Make() const {
    Dist dist;
    dist.codes = codes;
    dist.record_size = record_size;
    dist.d = d;
    dist.pair_terms = pair_terms;
    dist.symmetric = !pair_terms.empty();
    dist.decoded_query.assign(d, 0.f);
    dist.query_book = stats->MakeCodebook(dist.decoded_query);
    dist.query_reader = MakeQuantizerReader(dist.query_book);
    dist.query_reader->StartCluster(centroid);
    if (!dist.symmetric) {
      dist.decoded_pair.assign(d, 0.f);
      dist.pair_book = stats->MakeCodebook(dist.decoded_pair);
      dist.pair_reader = MakeQuantizerReader(dist.pair_book);
      dist.pair_reader->StartCluster(centroid);
    }
    return dist;
  }

 private:
  std::unique_ptr<QuantizerReader> MakeReader() const {
    std::vector<float> scratch(d, 0.f);
    auto book = stats->MakeCodebook(scratch);
    auto reader = MakeQuantizerReader(book);
    reader->StartCluster(centroid);
    return reader;
  }
};

template<typename Factory>
struct HnswInsertJob {
  HnswInsertJob(HnswGraph& graph, const Factory& factory, uint32_t ef,
                std::span<const uint32_t> nodes, size_t workers, size_t rows)
    : _graph{graph},
      _ef{ef},
      _nodes{nodes},
      _sync{workers * graph.M()},
      _scratch{workers} {
    _dists.reserve(workers);
    for (size_t i = 0; i < workers; ++i) {
      _dists.push_back(factory.Make());
    }
    for (auto& s : _scratch) {
      s.search.visited.Reset(rows);
    }
  }

  void SetWindow(const float* base, uint32_t first, uint32_t count) noexcept {
    for (auto& d : _dists) {
      d.SetWindow(base, first, count);
    }
  }

  void Restart(std::span<const uint32_t> nodes) noexcept {
    _nodes = nodes;
    _cursor.store(0, std::memory_order_relaxed);
  }

  void RunWorker(size_t worker) noexcept {
    auto& scratch = _scratch[worker];
    auto& dist = _dists[worker];
    for (auto begin = _cursor.fetch_add(kHnswInsertGranule);
         begin < _nodes.size(); begin = _cursor.fetch_add(kHnswInsertGranule)) {
      const auto end = std::min(begin + kHnswInsertGranule, _nodes.size());
      for (auto i = begin; i < end; ++i) {
        const auto node = _nodes[i];
        SDB_ASSERT(_graph.LevelOf(node) <= _graph.LevelOf(_graph.EntryPoint()));
        dist.SetQuery(node);
        HnswInsert(_graph, node, dist, _ef, scratch, _sync);
      }
    }
  }

 private:
  HnswGraph& _graph;
  uint32_t _ef;
  std::span<const uint32_t> _nodes;  // retargeted per window by Restart()
  HnswStripeSync _sync;
  std::atomic<size_t> _cursor{0};
  std::vector<HnswBuildScratch> _scratch;
  std::vector<typename Factory::Dist> _dists;
};

std::vector<uint32_t> SeedGraph(HnswGraph& graph,
                                std::span<const uint8_t> valid, uint32_t m,
                                uint64_t seed) {
  const auto rows = valid.size();
  graph.Reset(rows, m);

  uint64_t rng = seed;
  for (size_t i = 0; i < rows; ++i) {
    if (valid[i] != 0) {
      graph.SetLevel(static_cast<uint32_t>(i), HnswRandomLevel(rng, m));
    }
  }
  graph.AllocateLinks();

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
    return {};
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
  return nodes;
}

template<typename Factory>
auto InsertNodes(HnswGraph& graph, const Factory& factory, uint32_t ef,
                 std::span<const uint32_t> nodes, size_t rows, uint32_t warmup,
                 const AnnBuildEnv* env) -> yaclib::Future<> {
  const bool can_fan_out = env != nullptr && env->executor != nullptr;
  const auto want =
    can_fan_out ? static_cast<uint32_t>(std::clamp<size_t>(
                    nodes.size() / kHnswMinRowsPerWorker, 1, kHnswMaxWorkers))
                : 1U;
  const uint32_t workers = want > 1 ? env->acquire(want) : 1;
  absl::Cleanup release_workers = [&] {
    if (want > 1) {
      env->release(workers);
    }
  };
  const uint32_t helpers = workers - 1;

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

  HnswInsertJob<Factory> job{graph,   factory, ef, nodes.subspan(serial),
                             workers, rows};
  std::vector<yaclib::FutureOn<>> runs;
  runs.reserve(helpers);
  for (uint32_t i = 0; i < helpers; ++i) {
    runs.push_back(yaclib::Run(
      *env->executor, [&job, w = size_t{i} + 1] { job.RunWorker(w); }));
  }
  job.RunWorker(0);
  co_await yaclib::Await(runs.begin(), runs.end());
  job.RunWorker(0);
  co_return {};
}

template<typename Factory>
auto BuildGraphFromMerge(HnswGraph& graph, const Factory& factory,
                         std::span<const uint8_t> valid, uint32_t m,
                         uint32_t ef_construction, uint64_t seed,
                         const MergeDonor& donor, const AnnBuildEnv* env)
  -> yaclib::Future<bool> {
  auto data = donor.index->Load(*donor.reader);
  SDB_ASSERT(data);
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
    graph.SetEntryPoint(delta_entry);
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

  co_await InsertNodes(graph, factory, ef_construction, nodes, rows,
                       /*warmup=*/0, env);
  co_return true;
}

template<typename Factory>
auto BuildGraph(HnswGraph& graph, const Factory& factory,
                std::span<const uint8_t> valid, uint32_t m,
                uint32_t ef_construction, uint64_t seed, const AnnBuildEnv* env)
  -> yaclib::Future<> {
  const auto nodes = SeedGraph(graph, valid, m, seed);
  if (graph.Empty()) {
    co_return {};
  }
  co_await InsertNodes(graph, factory, ef_construction, nodes, valid.size(),
                       std::max(kHnswSerialWarmup, ef_construction), env);
  co_return {};
}

struct HnswOriginals {
  const ColumnReader* col = nullptr;
  ReadContext* ctx = nullptr;
  bool normalize = false;
};

inline constexpr uint32_t kHnswOriginalsWindow = 32768;

template<typename Factory>
void BuildGraphStreamed(HnswGraph& graph, const Factory& factory,
                        std::span<const uint8_t> valid, uint32_t m,
                        uint32_t ef_construction, uint64_t seed,
                        const AnnBuildEnv* env, const HnswOriginals& src,
                        uint32_t d) {
  const auto rows = valid.size();
  const auto nodes = SeedGraph(graph, valid, m, seed);
  if (nodes.empty()) {
    return;
  }

  const bool can_fan_out = env != nullptr && env->executor != nullptr;
  const auto want =
    can_fan_out ? static_cast<uint32_t>(std::clamp<size_t>(
                    nodes.size() / kHnswMinRowsPerWorker, 1, kHnswMaxWorkers))
                : 1U;
  const uint32_t workers = want > 1 ? env->acquire(want) : 1;
  absl::Cleanup release_workers = [&] {
    if (want > 1) {
      env->release(workers);
    }
  };

  HnswInsertJob<Factory> job{graph, factory, ef_construction,
                             {},    workers, rows};

  std::vector<yaclib::FutureOn<>> runs;
  auto run_span = [&](std::span<const uint32_t> span, uint32_t par) {
    if (span.empty()) {
      return;
    }
    job.Restart(span);
    if (par <= 1) {
      job.RunWorker(0);
      return;
    }
    runs.clear();
    runs.reserve(par - 1);
    for (uint32_t i = 1; i < par; ++i) {
      runs.push_back(yaclib::Run(*env->executor,
                                 [&job, w = size_t{i}] { job.RunWorker(w); }));
    }
    job.RunWorker(0);
    yaclib::Wait(runs.begin(), runs.end());
  };

  std::vector<float> window;
  window.reserve(static_cast<size_t>(kHnswOriginalsWindow) * d);
  std::vector<float> batch_buf;
  size_t next = 0;
  uint64_t warmed = 0;
  const uint64_t warmup =
    std::max<uint64_t>(kHnswSerialWarmup, ef_construction);
  uint32_t win_first = 0;

  auto flush_window = [&](uint32_t first, uint32_t count) {
    job.SetWindow(window.data(), first, count);
    const uint32_t last = first + count;
    const size_t begin = next;
    while (next < nodes.size() && nodes[next] < last) {
      ++next;
    }
    std::span<const uint32_t> span{nodes.data() + begin, next - begin};
    if (warmed < warmup) {
      const auto take = std::min<size_t>(span.size(), warmup - warmed);
      run_span(span.first(take), 1);
      warmed += take;
      span = span.subspan(take);
    }
    run_span(span, workers);
  };

  ScanVectors(*src.col, *src.ctx, rows, d, src.normalize, batch_buf,
              [&](const float* src_rows, size_t n, uint64_t first,
                  const duckdb::ValidityMask&) {
                if (window.empty()) {
                  win_first = static_cast<uint32_t>(first);
                }
                window.insert(window.end(), src_rows, src_rows + n * d);
                const auto have = static_cast<uint32_t>(window.size() / d);
                if (have >= kHnswOriginalsWindow) {
                  flush_window(win_first, have);
                  window.clear();
                }
              });
  if (!window.empty()) {
    flush_window(win_first, static_cast<uint32_t>(window.size() / d));
  }
  job.SetWindow(nullptr, 0, 0);
}

template<typename Factory>
auto BuildDispatch(HnswGraph& graph, const Factory& factory,
                   std::span<const uint8_t> valid, uint32_t m, uint32_t ef,
                   uint64_t seed, const MergeDonor& donor,
                   const AnnBuildEnv* env, const HnswOriginals& src, uint32_t d)
  -> yaclib::Future<> {
  if (donor.index != nullptr &&
      co_await BuildGraphFromMerge(graph, factory, valid, m, ef, seed, donor,
                                   env)) {
    co_return {};
  }
  if (src.col != nullptr) {
    BuildGraphStreamed(graph, factory, valid, m, ef, seed, env, src, d);
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
  SDB_ASSERT(_d != 0);
  if (_rows == 0) {
    co_return {};
  }

  using Clock = std::chrono::steady_clock;
  const auto t_begin = Clock::now();
  const auto ms_since = [](Clock::time_point from) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                                 from)
      .count();
  };

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
  const bool needs_centroid = _qw && QuantizerNeedsCentroid(_info.quant.kind);
  if (needs_centroid) {
    _centroid.assign(_d, 0.f);
  }

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

  if (needs_centroid) {
    SDB_ASSERT(trained_rows != 0);
    const float inv = 1.f / static_cast<float>(trained_rows);
    for (uint32_t j = 0; j < _d; ++j) {
      _centroid[j] *= inv;
    }
    _qw->SetClusterCentroid(_centroid.data());
  }

  if (_qw) {
    const uint64_t refine = _qw->RefineSamples(_rows);
    if (refine != 0) {
      ScanVectors(col, ctx, std::min<uint64_t>(_rows, refine), _d, normalize,
                  batch_buf,
                  [&](const float* rows, size_t n, uint64_t,
                      const duckdb::ValidityMask&) { _qw->Refine(rows, n); });
      _qw->RefineDone();
    }
  }

  bool encoding = _qw && _qw->BlockSetting().group_size == 1;
  if (encoding) {
    _record_size = _qw->BlockSetting().record_size;
    _codes.resize(static_cast<size_t>(_rows) * _record_size);
  } else if (!_qw) {
    _vectors.resize(static_cast<size_t>(_rows) * _d);
  }

  if (encoding) {
    encoding = EncodeColumn(col, ctx, _rows, _d, normalize, *_qw, _codes.data(),
                            _record_size, valid, env);
  } else {
    ScanVectors(col, ctx, _rows, _d, normalize, batch_buf,
                [&](const float* rows, size_t n, uint64_t first,
                    const duckdb::ValidityMask& mask) {
                  for (size_t i = 0; i < n; ++i) {
                    if (!mask.RowIsValid(i)) {
                      valid[first + i] = 0;
                    }
                  }
                  if (!_qw) {
                    std::memcpy(_vectors.data() + first * _d, rows,
                                n * _d * sizeof(float));
                  }
                });
  }

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

  const auto encode_ms = ms_since(t_begin);

  const auto m = _info.m;
  const auto ef = _info.ef_construction;
  SDB_ASSERT(m != 0 && ef != 0);
  const auto donor =
    PickMergeDonor(_merge_sources, _info.centroids_id, _d, _info.metric, m);
  const auto t_graph = Clock::now();
  const absl::Cleanup log_phases = [&] {
    SDB_INFO(IRESEARCH, "hnsw build: rows=", _rows, " d=", _d, " m=", m,
             " ef=", ef, " sources=", _merge_sources.size(),
             " donor_rows=", donor.alive, " encode_ms=", encode_ms,
             " graph_ms=", ms_since(t_graph));
  };

  if (stats) {
    HnswCodeDistFactory factory{
      .codes = _codes.data(),
      .record_size = _record_size,
      .stats = stats.get(),
      .centroid = _centroid.empty() ? nullptr : _centroid.data(),
      .d = _d};
    SDB_ENSURE(factory.CanRekey(),
               "hnsw: quantizer produced a code layout the build cannot score "
               "(Decode/SetQuery round-trip failed) for quant kind ",
               static_cast<uint32_t>(_info.quant.kind));
    std::vector<float> pair_terms;
    if (factory.PreparePairTerms(_rows, pair_terms)) {
      factory.pair_terms = pair_terms;
    }
    const HnswOriginals originals{
      .col = &col, .ctx = &ctx, .normalize = normalize};
    co_await BuildDispatch(_graph, factory, valid, m, ef, kHnswBuildSeed, donor,
                           env, originals, _d);
    co_return {};
  }

  SDB_ENSURE(
    !_qw, "hnsw: quantizer kind ", static_cast<uint32_t>(_info.quant.kind),
    " produced no per-row codes (group_size=", _qw->BlockSetting().group_size,
    ", record_size=", _qw->BlockSetting().record_size,
    "); refusing to fall back to a "
    "raw-float build of ",
    _rows, " x ", _d, " vectors");

  yaclib::Future<> built;
  ResolveEnum<VectorMetric>(
    EffectiveQuantMetric(_info.metric), [&]<VectorMetric M>() {
      HnswRawDistFactory<M> factory{.base = _vectors.data(), .d = _d};
      built = BuildDispatch(_graph, factory, valid, m, ef, kHnswBuildSeed,
                            donor, env, HnswOriginals{}, _d);
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
