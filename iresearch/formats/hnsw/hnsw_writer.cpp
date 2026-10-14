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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <core_functions/array_kernels.hpp>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <optional>
#include <yaclib/async/run.hpp>
#include <yaclib/async/wait.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/topic.hpp"

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
    const auto& hnsw = irs::utils::downCast<const HnswIndex>(*ann);
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
void HealNode(HnswGraphWriter& graph, const HnswGraph& src_graph,
              std::span<const uint32_t> remap, Dist& dist, uint32_t src_node,
              uint32_t node, uint32_t level, HnswBuildScratch& scratch,
              HealScratch& heal) {
  auto links = graph.Neighbors(node, level);
  auto& pool = heal.pool;
  auto& stack = heal.stack;
  auto& seen = heal.seen;
  auto& walked = heal.walked;
  pool.clear();
  seen.Next();
  walked.Next();
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
        duckdb::L2NormalizeOp::Operation(v, v, d);
      }
    }
    fn(buf.data(), static_cast<size_t>(take), done, mask);
    done += take;
  }
  return rows;
}

template<typename Fn>
uint64_t ScanValidVectors(const ColumnReader& col, ReadContext& ctx,
                          uint64_t rows, uint64_t limit, uint32_t d,
                          bool normalize, std::vector<float>& buf, Fn&& fn) {
  ColumnReader::VectorScratch scratch{col.Type()};
  auto scan = col.InitScan(ctx);
  uint64_t delivered = 0;
  for (uint64_t done = 0; done < rows && delivered < limit;) {
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - done));
    auto& batch = scratch.Reset();
    col.Scan(scan, batch, take);
    const auto& mask = duckdb::FlatVector::Validity(batch);
    const auto* src =
      duckdb::FlatVector::GetData<float>(duckdb::ArrayVector::GetChild(batch));
    buf.clear();
    size_t n = 0;
    for (duckdb::idx_t i = 0; i < take && delivered + n < limit; ++i) {
      if (mask.RowIsValid(i)) {
        buf.insert(buf.end(), src + static_cast<size_t>(i) * d,
                   src + static_cast<size_t>(i + 1) * d);
        ++n;
      }
    }
    if (normalize) {
      for (size_t i = 0; i < n; ++i) {
        float* v = buf.data() + i * d;
        duckdb::L2NormalizeOp::Operation(v, v, d);
      }
    }
    if (n != 0) {
      fn(buf.data(), n);
    }
    delivered += n;
    done += take;
  }
  return delivered;
}

inline constexpr uint64_t kHnswMinRowsPerEncoder = 32768;

class HnswCrew {
 public:
  HnswCrew(const AnnBuildEnv* env, uint32_t want) noexcept
    : _env{env != nullptr && env->executor != nullptr && want > 1 ? env
                                                                  : nullptr},
      _want{_env != nullptr ? want : 1U} {
    if (_env != nullptr) {
      _held = _env->acquire(_want, 0);
    }
  }

  HnswCrew(const HnswCrew&) = delete;
  HnswCrew& operator=(const HnswCrew&) = delete;

  ~HnswCrew() {
    Join();
    if (_env != nullptr) {
      _env->release(_held);
    }
  }

  bool CanGrow() const noexcept { return _env != nullptr; }
  uint32_t Want() const noexcept { return _want; }

  uint32_t Take() {
    if (_env != nullptr && _held < _want) {
      _held += _env->acquire(_want - _held, _held);
    }
    return _held;
  }

  template<typename F>
  void Spawn(F& work) {
    for (; _spawned < _held; ++_spawned) {
      _runs.push_back(
        yaclib::Run(*_env->executor, [&work, w = _spawned] { work(w); }));
    }
  }

  template<typename F>
  void Grow(F& work) {
    Take();
    Spawn(work);
  }

  std::vector<yaclib::FutureOn<>>& Runs() noexcept { return _runs; }

  void Join() {
    if (!_runs.empty()) {
      yaclib::Wait(_runs.begin(), _runs.end());
      _runs.clear();
    }
    _spawned = 1;
  }

 private:
  const AnnBuildEnv* _env;
  uint32_t _want;
  uint32_t _held = 1;
  uint32_t _spawned = 1;
  std::vector<yaclib::FutureOn<>> _runs;
};

void FanOut(const AnnBuildEnv* env, size_t tasks,
            absl::FunctionRef<void(size_t)> task) {
  std::atomic<size_t> next{0};
  const auto work = [&](size_t) {
    for (auto t = next.fetch_add(1); t < tasks; t = next.fetch_add(1)) {
      task(t);
    }
  };
  HnswCrew crew{
    env, static_cast<uint32_t>(std::clamp<size_t>(tasks, 1, kHnswMaxWorkers))};
  crew.Spawn(work);
  for (auto t = next.fetch_add(1); t < tasks; t = next.fetch_add(1)) {
    task(t);
    crew.Grow(work);
  }
  crew.Join();
}

bool EncodeColumn(const ColumnReader& col, ReadContext& ctx, uint64_t rows,
                  uint32_t d, bool normalize, QuantizerWriter& qw,
                  byte_type* codes, uint32_t record_size,
                  std::vector<uint8_t>& valid, const AnnBuildEnv* env) {
  HnswCrew crew{env, static_cast<uint32_t>(std::clamp<uint64_t>(
                       rows * qw.EncodeWidth() / kHnswMinRowsPerEncoder, 1,
                       kHnswMaxWorkers))};
  std::vector<std::unique_ptr<QuantizerWriter>> spare;
  bool clonable = true;

  std::vector<float> owned;
  std::vector<yaclib::FutureOn<>> runs;
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
    bool nulls = false;
    for (duckdb::idx_t i = 0; i < take; ++i) {
      if (!mask.RowIsValid(i)) {
        valid[done + i] = 0;
        nulls = true;
      }
    }
    const bool rewrite = normalize || nulls;
    if (rewrite) {
      owned.assign(src, src + static_cast<size_t>(take) * d);
      vecs = owned.data();
    }

    const uint32_t held = crew.Take();
    while (clonable && spare.size() + 1 < held) {
      auto clone = qw.CloneForEncode();
      if (!clone) {
        clonable = false;
        break;
      }
      spare.push_back(std::move(clone));
    }
    const size_t parts = spare.size() + 1;
    const size_t n = take;
    const auto run_part = [&](size_t w) {
      const size_t lo = n * w / parts;
      const size_t hi = n * (w + 1) / parts;
      if (lo >= hi) {
        return;
      }
      if (rewrite) {
        for (size_t i = lo; i < hi; ++i) {
          float* v = owned.data() + i * size_t{d};
          if (valid[done + i] == 0) {
            std::fill_n(v, d, 0.f);
          } else if (normalize) {
            duckdb::L2NormalizeOp::Operation(v, v, d);
          }
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
  const float* resident = nullptr;
  uint32_t rows = 0;

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
    dist.query_book = stats->MakeBuildCodebook(dist.decoded_query);
    dist.query_reader = MakeQuantizerReader(dist.query_book);
    dist.query_reader->StartCluster(centroid);
    if (!dist.symmetric) {
      dist.decoded_pair.assign(d, 0.f);
      dist.pair_book = stats->MakeBuildCodebook(dist.decoded_pair);
      dist.pair_reader = MakeQuantizerReader(dist.pair_book);
      dist.pair_reader->StartCluster(centroid);
    }
    dist.SetWindow(resident, 0, resident != nullptr ? rows : 0);
    return dist;
  }

 private:
  std::unique_ptr<QuantizerReader> MakeReader() const {
    std::vector<float> scratch(d, 0.f);
    auto book = stats->MakeBuildCodebook(scratch);
    auto reader = MakeQuantizerReader(book);
    reader->StartCluster(centroid);
    return reader;
  }
};

template<typename Factory>
struct HnswInsertJob {
  HnswInsertJob(HnswGraphWriter& graph, const Factory& factory, uint32_t ef,
                std::span<const uint32_t> nodes, size_t workers, size_t rows)
    : _graph{graph},
      _factory{factory},
      _ef{ef},
      _rows{rows},
      _nodes{nodes},
      _sync{workers * graph.M()},
      _slots(workers) {}

  void SetWindow(const float* base, uint32_t first, uint32_t count) noexcept {
    _window = {.base = base, .first = first, .count = count};
    for (auto& slot : _slots) {
      if (slot) {
        slot->dist.SetWindow(base, first, count);
      }
    }
  }

  void Restart(std::span<const uint32_t> nodes) noexcept {
    _nodes = nodes;
    _cursor.store(0, std::memory_order_relaxed);
  }

  void RunWorker(size_t worker) noexcept {
    RunWorker(worker, [] {});
  }

  template<typename Between>
  void RunWorker(size_t worker, Between&& between) noexcept {
    auto begin = _cursor.fetch_add(kHnswInsertGranule);
    if (begin >= _nodes.size()) {
      return;
    }
    auto& slot = Slot(worker);
    for (; begin < _nodes.size();
         begin = _cursor.fetch_add(kHnswInsertGranule)) {
      const auto end = std::min(begin + kHnswInsertGranule, _nodes.size());
      for (auto i = begin; i < end; ++i) {
        const auto node = _nodes[i];
        SDB_ASSERT(_graph.LevelOf(node) <= _graph.LevelOf(_graph.EntryPoint()));
        slot.dist.SetQuery(node);
        HnswInsert(_graph, node, slot.dist, _ef, slot.scratch, _sync);
      }
      between();
    }
  }

 private:
  struct Window {
    const float* base = nullptr;
    uint32_t first = 0;
    uint32_t count = 0;
  };

  struct WorkerSlot {
    explicit WorkerSlot(typename Factory::Dist d) : dist{std::move(d)} {}

    typename Factory::Dist dist;
    HnswBuildScratch scratch;
  };

  WorkerSlot& Slot(size_t worker) {
    auto& slot = _slots[worker];
    if (!slot) {
      slot = std::make_unique<WorkerSlot>(_factory.Make());
      slot->scratch.search.visited.Reset(_rows);
      if (_window.base != nullptr) {
        slot->dist.SetWindow(_window.base, _window.first, _window.count);
      }
    }
    return *slot;
  }

  HnswGraphWriter& _graph;
  const Factory& _factory;
  uint32_t _ef;
  size_t _rows;
  std::span<const uint32_t> _nodes;  // retargeted per window by Restart()
  Window _window;
  HnswStripeSync _sync;
  std::atomic<size_t> _cursor{0};
  std::vector<std::unique_ptr<WorkerSlot>> _slots;
};

std::vector<uint32_t> SeedGraph(HnswGraphWriter& graph,
                                std::span<const uint8_t> valid, uint32_t m,
                                uint64_t seed) {
  const auto rows = valid.size();
  graph.Reset(rows, m);

  pcg32 rng{seed};
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
auto InsertNodes(HnswGraphWriter& graph, const Factory& factory, uint32_t ef,
                 std::span<const uint32_t> nodes, size_t rows, uint32_t warmup,
                 const AnnBuildEnv* env) -> yaclib::Future<> {
  HnswCrew crew{env,
                static_cast<uint32_t>(std::clamp<size_t>(
                  nodes.size() / kHnswMinRowsPerWorker, 1, kHnswMaxWorkers))};

  const size_t serial =
    crew.CanGrow() ? std::min<size_t>(nodes.size(), warmup) : nodes.size();
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

  HnswInsertJob<Factory> job{graph,       factory, ef, nodes.subspan(serial),
                             crew.Want(), rows};
  const auto work = [&job](size_t w) { job.RunWorker(w); };
  crew.Spawn(work);
  job.RunWorker(0, [&] { crew.Grow(work); });
  co_await yaclib::Await(crew.Runs().begin(), crew.Runs().end());
  crew.Join();
  job.RunWorker(0);
  co_return {};
}

template<typename Factory>
auto BuildGraphFromMerge(HnswGraphWriter& graph, const Factory& factory,
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
  auto it_mask = donor.reader->MaskedDocs();
  for (size_t r = 0; r < src_rows; ++r) {
    const auto doc = static_cast<doc_id_t>(r) + doc_limits::min();
    if (it_mask.Contains(doc)) {
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
  pcg32 rng{seed};
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
auto BuildGraph(HnswGraphWriter& graph, const Factory& factory,
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
void BuildGraphStreamed(HnswGraphWriter& graph, const Factory& factory,
                        std::span<const uint8_t> valid, uint32_t m,
                        uint32_t ef_construction, uint64_t seed,
                        const AnnBuildEnv* env, const HnswOriginals& src,
                        uint32_t d) {
  const auto rows = valid.size();
  const auto nodes = SeedGraph(graph, valid, m, seed);
  if (nodes.empty()) {
    return;
  }

  HnswCrew crew{env,
                static_cast<uint32_t>(std::clamp<size_t>(
                  nodes.size() / kHnswMinRowsPerWorker, 1, kHnswMaxWorkers))};
  HnswInsertJob<Factory> job{graph, factory,     ef_construction,
                             {},    crew.Want(), rows};
  const auto work = [&job](size_t w) { job.RunWorker(w); };

  auto run_span = [&](std::span<const uint32_t> span, bool parallel) {
    if (span.empty()) {
      return;
    }
    job.Restart(span);
    if (!parallel || !crew.CanGrow()) {
      job.RunWorker(0);
      return;
    }
    crew.Grow(work);
    job.RunWorker(0, [&] { crew.Grow(work); });
    crew.Join();
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
      run_span(span.first(take), false);
      warmed += take;
      span = span.subspan(take);
    }
    run_span(span, true);
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
auto BuildDispatch(HnswGraphWriter& graph, const Factory& factory,
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

inline constexpr size_t kPanoramaMinRows = 1024;
inline constexpr size_t kPanoramaTrainRows = 4096;
inline constexpr size_t kPanoramaApplyRows = 4096;

bool PanoramaEnough(std::span<const uint8_t> valid, uint32_t d) {
  const size_t floor = std::max(kPanoramaMinRows, size_t{8} * d);
  return static_cast<size_t>(absl::c_count(valid, uint8_t{1})) >= floor;
}

std::vector<float> TrainPanoramaBasis(const std::vector<float>& vectors,
                                      std::span<const uint8_t> valid,
                                      uint32_t d) {
  const size_t rows = valid.size();
  const size_t want = std::max(kPanoramaTrainRows, size_t{8} * d);
  const size_t live = static_cast<size_t>(absl::c_count(valid, uint8_t{1}));
  const size_t step = std::max<size_t>(1, live / want);
  std::vector<float> sample;
  sample.reserve(std::min(live, want) * d);
  for (size_t row = 0, seen = 0; row < rows && sample.size() < want * d;
       ++row) {
    if (valid[row] == 0) {
      continue;
    }
    if (seen++ % step == 0) {
      sample.insert(sample.end(), vectors.begin() + row * d,
                    vectors.begin() + (row + 1) * d);
    }
  }
  return std::move(TrainPcaRotation(sample.data(), sample.size() / d, d).A);
}

void RotateForPanorama(std::vector<float>& vectors, size_t rows, uint32_t d,
                       std::span<const float> basis, const AnnBuildEnv* env,
                       std::vector<float>& rotation,
                       std::vector<float>& norms) {
  SDB_ASSERT(basis.size() == size_t{d} * d);
  faiss::LinearTransform transform{static_cast<int>(d), static_cast<int>(d),
                                   /*have_bias=*/false};
  transform.A.assign(basis.begin(), basis.end());
  transform.is_trained = true;
  rotation.assign(basis.begin(), basis.end());
  FanOut(env, (rows + kPanoramaApplyRows - 1) / kPanoramaApplyRows,
         [&](size_t part) {
           const size_t first = part * kPanoramaApplyRows;
           const size_t n = std::min(kPanoramaApplyRows, rows - first);
           std::vector<float> chunk(n * d);
           transform.apply_noalloc(static_cast<faiss::idx_t>(n),
                                   vectors.data() + first * d, chunk.data());
           std::copy_n(chunk.data(), n * d, vectors.data() + first * d);
         });
  norms.resize(rows);
  for (size_t row = 0; row < rows; ++row) {
    norms[row] = faiss::fvec_norm_L2sqr(vectors.data() + row * d, d);
  }
}

bool RotatesCodes(const AnnInfo& info, uint32_t d) noexcept {
  return info.quant.kind == VectorQuantization::SQ8 &&
         EffectiveQuantMetric(info.metric) == VectorMetric::InnerProduct &&
         d >= kHnswPanoramaMinDim && d <= kHnswPanoramaMaxDim &&
         PanoramaApplies(info.metric, d);
}

HnswBases::Basis AcquireBasis(HnswBases* bases, field_id field,
                              const MergeDonor& donor,
                              const std::vector<float>& vectors,
                              std::span<const uint8_t> valid, uint32_t d,
                              bool enough) {
  HnswBases::Basis basis;
  if (bases != nullptr) {
    basis = bases->Find(field);
  }
  if (!basis && donor.index != nullptr) {
    if (auto data = donor.index->Load(*donor.reader)) {
      basis = data->rotation;
    }
  }
  if (!basis && enough) {
    basis = std::make_shared<const std::vector<float>>(
      TrainPanoramaBasis(vectors, valid, d));
  }
  if (basis && bases != nullptr) {
    basis = bases->Publish(field, std::move(basis));
  }
  SDB_ASSERT(!basis || basis->size() == size_t{d} * d);
  return basis;
}

void ComputeCodeTails(const QuantizerStats& stats, const byte_type* codes,
                      uint32_t record_size, size_t rows, uint32_t d,
                      const AnnBuildEnv* env, std::vector<float>& tails) {
  const uint32_t stride = HnswCodeTails(d);
  tails.assign(rows * stride, 0.f);
  FanOut(env, (rows + kPanoramaApplyRows - 1) / kPanoramaApplyRows,
         [&](size_t part) {
           const std::vector<float> zero(d, 0.f);
           auto reader = MakeQuantizerReader(stats.MakeCodebook(zero));
           std::vector<float> row(d);
           const size_t first = part * kPanoramaApplyRows;
           const size_t last = std::min(rows, first + kPanoramaApplyRows);
           for (size_t r = first; r < last; ++r) {
             reader->Decode(codes + r * record_size, row.data());
             float* out = tails.data() + r * stride;
             float sum = 0.f;
             for (uint32_t k = stride; k > 0; --k) {
               const uint32_t b = k * kHnswCodeLevel;
               sum += faiss::fvec_norm_L2sqr(row.data() + b,
                                             std::min(kHnswCodeLevel, d - b));
               out[k - 1] = std::sqrt(sum);
             }
           }
         });
}

}  // namespace

HnswWriter::HnswWriter(AnnInfo info, HnswBases* bases)
  : _info{std::move(info)}, _bases{bases} {}

HnswWriter::~HnswWriter() = default;

yaclib::Task<> HnswWriter::Compute(const ColumnReader& col, ReadContext& ctx,
                                   const AnnBuildEnv* env) {
  _d = static_cast<uint32_t>(col.ArraySize());
  _rows = col.RowCount();
  SDB_ASSERT(_d != 0);
  if (_rows == 0) {
    co_return {};
  }

  std::vector<uint8_t> valid(_rows, 1);
  std::vector<float> batch_buf;
  const bool normalize = _info.metric == VectorMetric::Cosine;

  if (_info.quant.kind != VectorQuantization::None) {
    _qw = MakeQuantizerWriter(
      _info.quant.kind, _d, EffectiveQuantMetric(_info.metric),
      _info.quant.pq_m, 0, _info.quant.nb_bits,
      /*row_major=*/true, _info.quant.half_bit, _info.quant.query_bits);
  }

  const auto m = _info.m;
  const auto ef = _info.ef_construction;
  SDB_ASSERT(m != 0 && ef != 0);
  const auto donor =
    PickMergeDonor(_merge_sources, _info.centroids_id, _d, _info.metric, m);

  if (_qw && RotatesCodes(_info, _d)) {
    _vectors.resize(static_cast<size_t>(_rows) * _d);
    ScanVectors(col, ctx, _rows, _d, normalize, batch_buf,
                [&](const float* rows, size_t n, uint64_t first,
                    const duckdb::ValidityMask& mask) {
                  std::memcpy(_vectors.data() + first * _d, rows,
                              n * _d * sizeof(float));
                  for (size_t i = 0; i < n; ++i) {
                    if (!mask.RowIsValid(i)) {
                      valid[first + i] = 0;
                      std::fill_n(_vectors.data() + (first + i) * _d, _d, 0.f);
                    }
                  }
                });
    const bool enough = PanoramaEnough(valid, _d);
    const auto basis = AcquireBasis(_bases, _info.centroids_id, donor, _vectors,
                                    valid, _d, enough);
    if (basis && enough) {
      RotateForPanorama(_vectors, valid.size(), _d, *basis, env, _rotation,
                        _norms);
      _norms = {};
      for (size_t first = 0; first < valid.size();) {
        if (valid[first] == 0) {
          ++first;
          continue;
        }
        size_t last = first;
        while (last < valid.size() && valid[last] != 0) {
          ++last;
        }
        _qw->Train(_vectors.data() + first * _d, last - first);
        first = last;
      }
      _record_size = _qw->BlockSetting().record_size;
      _codes.resize(static_cast<size_t>(_rows) * _record_size);
      SDB_ENSURE(_qw->EncodeInto(_codes.data(), _vectors.data(), _rows),
                 "hnsw: sq8 encodes rows in place");
      BytesOutput blob_out{_stats_blob};
      _qw->Serialize(blob_out);
      const auto stats = MakeQuantizerStats(
        _info.quant.kind, _d,
        std::span<const byte_type>{_stats_blob}.subspan(sizeof(uint64_t)),
        EffectiveQuantMetric(_info.metric), /*row_major=*/true);
      SDB_ENSURE(stats != nullptr, "hnsw: sq8 statistics round-trip");
      ComputeCodeTails(*stats, _codes.data(), _record_size, _rows, _d, env,
                       _tails);
      HnswCodeDistFactory factory{.codes = _codes.data(),
                                  .record_size = _record_size,
                                  .stats = stats.get(),
                                  .centroid = nullptr,
                                  .d = _d,
                                  .resident = _vectors.data(),
                                  .rows = static_cast<uint32_t>(_rows)};
      std::vector<float> pair_terms;
      if (factory.PreparePairTerms(_rows, pair_terms)) {
        factory.pair_terms = pair_terms;
      }
      co_await BuildDispatch(_graph, factory, valid, m, ef, kHnswBuildSeed,
                             donor, env, HnswOriginals{}, _d);
      _vectors = {};
      co_return {};
    }
    _vectors = {};
  }

  const bool train =
    _qw && _qw->TrainSamples(_rows) == QuantizerWriter::kTrainStreaming;
  const size_t train_rows = _qw && !train ? _qw->TrainSamples(_rows) : 0;
  std::vector<float> train_buf;
  const bool needs_centroid = _qw && QuantizerNeedsCentroid(_info.quant.kind);
  if (needs_centroid) {
    _centroid.assign(_d, 0.f);
  }

  uint64_t trained_rows = 0;
  if (_qw && (train || train_rows != 0 || needs_centroid)) {
    // A streaming trainer asked for every row and has to get every row. For
    // scalar quantization what it trains is a per-dimension min/max, and
    // faiss silently clamps a component outside that range -- so a row the
    // trainer never saw encodes with an error the range cannot describe, and
    // on a segment past the sample that was three quarters of the rows. The
    // pass costs one sequential read of a column this build reads again to
    // encode, and the arithmetic is two comparisons per component. Trainers
    // that want a bounded sample (k-means and friends) still get one.
    const uint64_t sample =
      train ? _rows : std::min<uint64_t>(_rows, kHnswTrainSample);
    trained_rows = ScanValidVectors(
      col, ctx, _rows, sample, _d, normalize, batch_buf,
      [&](const float* rows, size_t n) {
        if (train) {
          _qw->Train(rows, n);
        }
        if (const size_t have = train_buf.size() / _d; have < train_rows) {
          const size_t take = std::min(n, train_rows - have);
          train_buf.insert(train_buf.end(), rows, rows + take * _d);
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
    if (trained_rows == 0) {
      co_return {};
    }
    if (train_rows != 0) {
      _qw->TrainParallel(
        train_buf.data(), train_buf.size() / _d,
        [env](size_t tasks, absl::FunctionRef<void(size_t)> task) {
          FanOut(env, tasks, task);
        });
      train_buf = {};
    }
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
      ScanValidVectors(
        col, ctx, _rows, std::min<uint64_t>(_rows, refine), _d, normalize,
        batch_buf, [&](const float* rows, size_t n) { _qw->Refine(rows, n); });
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
    if (!_qw && _d >= kHnswPanoramaMinDim && _d <= kHnswPanoramaMaxDim &&
        PanoramaApplies(_info.metric, _d)) {
      const bool enough = PanoramaEnough(valid, _d);
      const auto basis = AcquireBasis(_bases, _info.centroids_id, donor,
                                      _vectors, valid, _d, enough);
      if (basis && enough) {
        RotateForPanorama(_vectors, valid.size(), _d, *basis, env, _rotation,
                          _norms);
      }
    }
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

  out.WriteU32(_d);
  out.WriteU32(static_cast<uint32_t>(_info.metric));
  out.WriteU32(static_cast<uint32_t>(_info.quant.kind));
  out.WriteU32(_info.ef_construction);
  out.WriteU32(_record_size);
  out.WriteU64(_rows);
  out.WriteByte(_rotation.empty() ? 0 : 1);
  _graph.Serialize(out);
  if (_qw) {
    _qw->Serialize(out);
    if (!_centroid.empty()) {
      out.WriteData(reinterpret_cast<const byte_type*>(_centroid.data()),
                    _centroid.size() * sizeof(float));
    }
    out.WriteData(_codes.data(), _codes.size());
    if (!_rotation.empty()) {
      out.WriteData(reinterpret_cast<const byte_type*>(_rotation.data()),
                    _rotation.size() * sizeof(float));
      out.WriteData(reinterpret_cast<const byte_type*>(_tails.data()),
                    _tails.size() * sizeof(float));
    }
  } else {
    out.WriteData(reinterpret_cast<const byte_type*>(_vectors.data()),
                  static_cast<size_t>(_rows) * _d * sizeof(float));
    if (!_rotation.empty()) {
      out.WriteData(reinterpret_cast<const byte_type*>(_rotation.data()),
                    _rotation.size() * sizeof(float));
      out.WriteData(reinterpret_cast<const byte_type*>(_norms.data()),
                    _norms.size() * sizeof(float));
    }
  }

  _idx->AddHnsw(
    _info.centroids_id,
    HnswMeta{.offset = offset, .byte_size = out.Position() - offset});

  _vectors.clear();
  _vectors.shrink_to_fit();
  _rotation.clear();
  _rotation.shrink_to_fit();
  _norms.clear();
  _norms.shrink_to_fit();
  _tails.clear();
  _tails.shrink_to_fit();
  _codes.clear();
  _codes.shrink_to_fit();
  _stats_blob.clear();
  _stats_blob.shrink_to_fit();
}

}  // namespace irs
