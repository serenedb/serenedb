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

#include "iresearch/formats/ivf/hnsw.hpp"

#include <algorithm>
#include <limits>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <usearch/index.hpp>

#include "basics/misc.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

using UIndex = unum::usearch::index_gt<float, uint32_t, uint32_t>;

inline constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();

// USearch keeps one scratch context -- both candidate heaps plus the visited
// set
// -- per reserved thread slot and reuses it across queries, which is why
// nothing on the search path allocates. A slot must never be shared: USearch's
// own bounds check on it is compiled out under NDEBUG, so a collision would be
// undefined behaviour rather than an assert.
//
// So a slot is leased per thread rather than per search: claimed the first time
// a thread searches, returned when the thread exits. Every graph reserves the
// same number of slots, so one global lease serves all of them, the search path
// needs no synchronisation at all, and slots are recycled instead of leaking as
// server threads come and go.
class SlotRegistry {
 public:
  static uint32_t Slots() noexcept {
    return std::max<uint32_t>(1, std::thread::hardware_concurrency());
  }

  static uint32_t Acquire() {
    auto& self = Instance();
    std::lock_guard lock{self._mutex};
    if (self._free.empty()) {
      return kNoSlot;
    }
    const uint32_t slot = self._free.back();
    self._free.pop_back();
    return slot;
  }

  static void Release(uint32_t slot) {
    if (slot == kNoSlot) {
      return;
    }
    auto& self = Instance();
    std::lock_guard lock{self._mutex};
    self._free.push_back(slot);
  }

 private:
  SlotRegistry() : _free(Slots()) { std::iota(_free.begin(), _free.end(), 0u); }

  static SlotRegistry& Instance() {
    static SlotRegistry instance;
    return instance;
  }

  std::mutex _mutex;
  std::vector<uint32_t> _free;
};

class SlotLease {
 public:
  SlotLease() : _slot{SlotRegistry::Acquire()} {}
  ~SlotLease() { SlotRegistry::Release(_slot); }

  SlotLease(const SlotLease&) = delete;
  SlotLease& operator=(const SlotLease&) = delete;

  uint32_t Get() const noexcept { return _slot; }

 private:
  uint32_t _slot;
};

uint32_t ThreadSearchSlot() noexcept {
  static const thread_local SlotLease lease;
  return lease.Get();
}

void EnsureOk(unum::usearch::error_t& error, std::string_view what) {
  if (!error) {
    return;
  }
  // release() clears the message: error_t's destructor raises on anything left
  // unchecked, so it has to be drained before we throw our own.
  const std::string message{error.release()};
  SDB_ENSURE(false, "ivf hnsw: ", what, ": ", message);
}

template<VectorMetric Metric>
struct CentroidMetric {
  const float* base;
  uint32_t d;

  // USearch keeps the smallest distance, while ComputeDistance returns a
  // similarity (larger is closer) for every metric, so negate it. Search only
  // hands back ids and centroids, never distances, so the sign convention does
  // not escape this file.
  float Dist(const float* l, const float* r) const noexcept {
    return -ComputeDistance<Metric>(l, r, static_cast<uint16_t>(d));
  }

  const float* Row(size_t slot) const noexcept { return base + slot * d; }

  float operator()(const float* query,
                   const UIndex::member_citerator_t& m) const noexcept {
    return Dist(query, Row(get_slot(m)));
  }
  float operator()(const UIndex::member_citerator_t& a,
                   const UIndex::member_citerator_t& b) const noexcept {
    return Dist(Row(get_slot(a)), Row(get_slot(b)));
  }
  float operator()(const float* query,
                   const UIndex::member_cref_t& m) const noexcept {
    return Dist(query, Row(get_slot(m)));
  }
  float operator()(const UIndex::member_cref_t& a,
                   const UIndex::member_cref_t& b) const noexcept {
    return Dist(Row(get_slot(a)), Row(get_slot(b)));
  }
};

uint32_t ResolveM(uint32_t m) noexcept { return m != 0 ? m : kHnswDefaultM; }

uint32_t ResolveEfConstruction(uint32_t ef) noexcept {
  return ef != 0 ? ef : kHnswDefaultEfConstruction;
}

}  // namespace

class HnswGraph {
 public:
  explicit HnswGraph(UIndex&& index) : _index{std::move(index)} {}

  static std::unique_ptr<HnswGraph> Build(const float* base, size_t n,
                                          uint32_t d, VectorMetric metric,
                                          uint32_t m,
                                          uint32_t ef_construction) {
    auto state = UIndex::make(unum::usearch::index_config_t{m});
    EnsureOk(state.error, "failed to create the graph");
    UIndex index = std::move(state.index);
    // Build is single-threaded, so one slot suffices here; Reserve below widens
    // it for the search side.
    SDB_ENSURE(index.try_reserve(unum::usearch::index_limits_t{n, 1}),
               "ivf hnsw: out of memory reserving ", n, " graph members");

    ResolveEnum<VectorMetric>(metric, [&]<VectorMetric Metric>() {
      const CentroidMetric<Metric> dist{.base = base, .d = d};
      for (size_t i = 0; i < n; ++i) {
        auto added =
          index.add(static_cast<uint32_t>(i), base + i * d, dist,
                    unum::usearch::index_update_config_t{ef_construction, 0});
        EnsureOk(added.error, "failed to add a centroid");
      }
    });
    return std::make_unique<HnswGraph>(std::move(index));
  }

  static std::unique_ptr<HnswGraph> Load(IndexInput& in) {
    auto state = UIndex::make();
    EnsureOk(state.error, "failed to create the graph");
    UIndex index = std::move(state.index);
    auto loaded = index.load_from_stream([&](void* p, size_t len) {
      in.ReadData(static_cast<byte_type*>(p), len);
      return true;
    });
    EnsureOk(loaded.error, "failed to read the graph");
    auto graph = std::make_unique<HnswGraph>(std::move(index));
    graph->Reserve();
    return graph;
  }

  void Save(IndexOutput& out) const {
    auto saved = _index.save_to_stream([&](void* p, size_t len) {
      out.WriteData(static_cast<const byte_type*>(p), len);
      return true;
    });
    EnsureOk(saved.error, "failed to write the graph");
  }

  size_t Size() const noexcept { return _index.size(); }

  // Returns false when this thread holds no slot, i.e. more threads are live
  // than the registry has slots for. The caller then scans the centroids, which
  // is always correct -- sharing a slot would corrupt the traversal.
  bool Search(const float* base, uint32_t d, VectorMetric metric,
              const float* query, uint32_t nprobe, uint32_t expansion,
              std::vector<uint32_t>& out_ids) const {
    const uint32_t slot = ThreadSearchSlot();
    if (slot >= _index.limits().threads_search) {
      return false;
    }
    ResolveEnum<VectorMetric>(metric, [&]<VectorMetric Metric>() {
      const CentroidMetric<Metric> dist{.base = base, .d = d};
      auto found =
        _index.search(query, nprobe, dist,
                      unum::usearch::index_search_config_t{expansion, slot});
      EnsureOk(found.error, "search failed");
      const size_t first = out_ids.size();
      out_ids.resize(first + found.count);
      out_ids.resize(first + found.dump_to(out_ids.data() + first));
    });
    return true;
  }

 private:
  void Reserve() {
    auto limits = _index.limits();
    limits.threads_search =
      std::max<size_t>(limits.threads_search, SlotRegistry::Slots());
    SDB_ENSURE(_index.try_reserve(limits), "ivf hnsw: out of memory reserving ",
               limits.threads_search, " search contexts");
  }

  UIndex _index;
};

HnswCentroids::HnswCentroids(IVFHeader&& head, std::vector<float>&& centroids,
                             std::unique_ptr<HnswGraph>&& graph)
  : CentroidsIndex{std::move(head)},
    _centroids{std::move(centroids)},
    _graph{std::move(graph)} {
  _n = _head.d != 0 ? _centroids.size() / _head.d : 0;
}

HnswCentroids::~HnswCentroids() = default;

std::unique_ptr<HnswCentroids> HnswCentroids::Deserialize(IVFHeader&& head,
                                                          IndexInput& in) {
  // Mirrors the tree's own root read: one flat, unrotated layer. It leaves `in`
  // sitting exactly on the graph blob, which is why no offset is stored for it.
  const size_t level = static_cast<size_t>(in.ReadI64());
  SDB_ENSURE(level == 0, "ivf hnsw: expected a flat centroid layer, got level ",
             level);
  const size_t n_total_pos = static_cast<size_t>(in.Position());
  const size_t n_total = static_cast<size_t>(in.ReadI64());
  in.Seek(n_total_pos);
  auto nodes = CentroidsNode::Deserialize(in, 0, head.d, {0}, {n_total},
                                          /*n_levels=*/0);
  auto centroids = std::move(nodes.front().centroids);

  auto graph = HnswGraph::Load(in);
  SDB_ENSURE(graph->Size() == n_total, "ivf hnsw: graph holds ", graph->Size(),
             " members but the centroid layer holds ", n_total);
  return std::make_unique<HnswCentroids>(std::move(head), std::move(centroids),
                                         std::move(graph));
}

void HnswCentroids::Search(std::span<const float> query, IndexInput& /*in*/,
                           uint32_t nprobe, std::vector<uint32_t>& out_ids,
                           std::vector<float>* out_centroids,
                           uint32_t max_search_fanout, bool /*prune*/,
                           CentroidsSearchStats* /*out_stats*/) const {
  if (_n == 0) {
    out_ids.push_back(0);
    return;
  }
  const auto d = static_cast<uint32_t>(_head.d);
  const auto append_centroids = [&] {
    if (out_centroids == nullptr) {
      return;
    }
    out_centroids->reserve(out_centroids->size() + out_ids.size() * d);
    for (const uint32_t id : out_ids) {
      const float* c = _centroids.data() + size_t{id} * d;
      out_centroids->insert(out_centroids->end(), c, c + d);
    }
  };
  const auto scan_all = [&] {
    out_ids.reserve(out_ids.size() + _n);
    for (size_t i = 0; i < _n; ++i) {
      out_ids.push_back(static_cast<uint32_t>(i));
    }
  };

  // ByRadius asks for every cluster; a graph walk cannot answer that, and does
  // not need to -- the flat array is right here.
  if (nprobe >= _n) {
    scan_all();
    append_centroids();
    return;
  }

  const uint32_t expansion =
    std::max({max_search_fanout, nprobe, kHnswDefaultEfSearch});
  if (!_graph->Search(_centroids.data(), d, EffectiveQuantMetric(_head.metric),
                      query.data(), nprobe, expansion, out_ids)) {
    scan_all();
  }
  append_centroids();
}

void WriteHnswGraph(IndexOutput& out, std::span<const float> centroids,
                    uint32_t d, VectorMetric metric, uint32_t m,
                    uint32_t ef_construction) {
  SDB_ASSERT(d != 0);
  const size_t n = centroids.size() / d;
  auto graph =
    HnswGraph::Build(centroids.data(), n, d, EffectiveQuantMetric(metric),
                     ResolveM(m), ResolveEfConstruction(ef_construction));
  graph->Save(out);
}

}  // namespace irs
