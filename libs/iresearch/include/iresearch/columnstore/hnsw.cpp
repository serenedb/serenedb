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
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/columnstore/hnsw.hpp"

#include <faiss/impl/DistanceComputer.h>
#include <faiss/impl/ResultHandler.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>

#include "basics/assert.h"
#include "basics/containers/node_hash_map.h"
#include "basics/containers/s3_fifo.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs::columnstore {
namespace {

// Cache granularity. Each chunk holds up to `kChunkSizeFloats` child
// elements (and at least one ARRAY row when d exceeds it); number of
// chunks resident is bounded by `kChunkCacheSlots`. Defaults picked
// from the cross-product sweep in scripts/perf/sweep_hnsw_cross.sh:
// chunk=8192 / cache=64 minimizes vector_search.test_slow runtime
// (matches main pre-deferral baseline). Memory ceiling per HNSW
// writer/reader is `chunk * cache * 4 bytes` ~ 2 MB for typical d.
constexpr uint64_t kChunkSizeFloats = 4 * STANDARD_VECTOR_SIZE;
constexpr size_t kChunkCacheSlots = 64;
// Row-group ColumnSegments kept open. OpenSegmentImpl reads the entire
// row-group payload from disk into a BufferManager block; reusing the
// segment across chunk loads landing in the same row group is the
// dominant speedup on chunk misses (was 16% of build CPU on memmove
// alone before this cache).
constexpr size_t kRgCacheSlots = 8;

float ComputeNegativeInnerProduct(const byte_type* l, const byte_type* r,
                                  uint16_t d) {
  return -irs::vector::DotProductImpl<float, float>::Compute(l, r, d);
}

float ComputeCosine(const byte_type* l, const byte_type* r, uint16_t d) {
  const auto [ll, lr, rr] =
    irs::vector::CosineDistanceImpl<float, float, float>::Compute(l, r, d);
  const float denom = std::sqrt(ll) * std::sqrt(rr);
  return denom == 0.f ? 1.f : 1.f - lr / denom;
}

auto ResolveDistanceFunction(HNSWMetric metric) {
  switch (metric) {
    case HNSWMetric::L2Sqr:
      return irs::vector::L2Space<float, float, float>::Dist;
    case HNSWMetric::NegativeIP:
      return ComputeNegativeInnerProduct;
    case HNSWMetric::L1:
      return irs::vector::L1Space<float, float, float>::Dist;
    case HNSWMetric::Cosine:
      return ComputeCosine;
  }
  SDB_UNREACHABLE();
}

class ChunkedVectorCache;

using PendingHook = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

struct ChunkSlot : public sdb::containers::s3_fifo::CacheHook {
  uint64_t chunk_id = std::numeric_limits<uint64_t>::max();
  duckdb::Vector data;
  const float* base;
  // Pin count, not a flag: HNSWWriter::Build pins the outer chunk and
  // ColumnDistance::symmetric_dis can pin a slot already in the outer
  // pin (when i happens to be a row of the outer chunk). Decrement on
  // Unpin so the inner Unpin doesn't drop the outer's pin.
  uint8_t pinned = 0;
  PendingHook pending_hook;

  explicit ChunkSlot(duckdb::idx_t size)
    : data{duckdb::LogicalType::FLOAT, size,
           duckdb::VectorDataInitialization::ZERO_INITIALIZE},
      base{duckdb::FlatVector::GetData<float>(data)} {}

  ChunkSlot(const ChunkSlot&) = delete;
  ChunkSlot& operator=(const ChunkSlot&) = delete;
  ChunkSlot(ChunkSlot&&) = delete;
  ChunkSlot& operator=(ChunkSlot&&) = delete;
};

struct RgSlot : public sdb::containers::s3_fifo::CacheHook {
  size_t rg = std::numeric_limits<size_t>::max();
  ColumnReader::ScanCursor cursor;
  PendingHook pending_hook;

  RgSlot() = default;
  RgSlot(const RgSlot&) = delete;
  RgSlot& operator=(const RgSlot&) = delete;
  RgSlot(RgSlot&&) = delete;
  RgSlot& operator=(RgSlot&&) = delete;
};

using ChunkPendingList =
  boost::intrusive::list<ChunkSlot,
                         boost::intrusive::member_hook<
                           ChunkSlot, PendingHook, &ChunkSlot::pending_hook>,
                         boost::intrusive::constant_time_size<false>>;
using RgPendingList = boost::intrusive::list<
  RgSlot,
  boost::intrusive::member_hook<RgSlot, PendingHook, &RgSlot::pending_hook>,
  boost::intrusive::constant_time_size<false>>;

struct ChunkEvictor {
  ChunkedVectorCache* self;
  bool operator()(ChunkSlot& slot) const noexcept;
};

struct RgEvictor {
  ChunkedVectorCache* self;
  bool operator()(RgSlot& slot) const noexcept;
};

class ChunkedVectorCache {
 public:
  using ChunkMap = sdb::containers::NodeHashMap<uint64_t, ChunkSlot>;
  using RgMap = sdb::containers::NodeHashMap<size_t, RgSlot>;

  ChunkedVectorCache(const ColumnReader& child, uint64_t array_size,
                     size_t capacity)
    : _child{&child},
      _d{array_size},
      _chunk_rows{
        std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(_d, 1), 1)},
      _slots{{.cache_size = capacity,
              .small_size = std::max<size_t>(capacity / 10, 1)},
             ChunkEvictor{this}},
      _rgs{{.cache_size = kRgCacheSlots,
            .small_size = std::max<size_t>(kRgCacheSlots / 4, 1)},
           RgEvictor{this}} {}

  const float* Get(uint64_t row) { return SliceOf(*FindOrLoad(row), row); }

  // Pin/Unpin nest LIFO: HNSWWriter::Build's outer pin must survive
  // ColumnDistance::symmetric_dis's inner Pin/Unpin, even when both
  // target the same slot. `slot.pinned` is a counter so the inner
  // decrement leaves the outer's pin in place; the stack records
  // which slot each Unpin maps to. Two-deep is enough today (Build
  // outer + symmetric_dis inner).
  const float* Pin(uint64_t row) {
    auto* slot = FindOrLoad(row);
    SDB_ASSERT(_pin_depth < _pin_stack.size());
    SDB_ASSERT(slot->pinned < std::numeric_limits<uint8_t>::max());
    ++slot->pinned;
    _pin_stack[_pin_depth++] = slot;
    return SliceOf(*slot, row);
  }

  void Unpin() noexcept {
    SDB_ASSERT(_pin_depth > 0);
    auto* slot = _pin_stack[--_pin_depth];
    SDB_ASSERT(slot->pinned > 0);
    --slot->pinned;
  }

  RgSlot& GetRgSlot(size_t rg) {
    auto [it, inserted] = _rg_index.try_emplace(rg);
    if (!inserted) {
      it->second.touch();
      return it->second;
    }
    RgSlot& slot = it->second;
    slot.rg = rg;
    slot.cursor = ColumnReader::ScanCursor{_child->OpenSegment(rg)};
    _rgs.Insert(slot);
    // s3_fifo evicts inside insert(); the evictor parks evicted slots
    // on `_rg_evicted` because it can't free them itself (cache iterator
    // would dangle). Erase here, after insert() returns.
    while (!_rg_evicted.empty()) {
      auto& s = _rg_evicted.front();
      _rg_evicted.pop_front();
      _rg_index.erase(s.rg);
    }
    return slot;
  }

  friend struct ChunkEvictor;
  friend struct RgEvictor;

 private:
  ChunkSlot* FindOrLoad(uint64_t row) {
    const uint64_t chunk_id = row / _chunk_rows;
    auto [it, inserted] = _chunk_index.try_emplace(
      chunk_id, static_cast<duckdb::idx_t>(_chunk_rows * _d));
    if (!inserted) {
      it->second.touch();
      return &it->second;
    }
    ChunkSlot& slot = it->second;
    slot.chunk_id = chunk_id;
    slot.pinned = false;
    Load(slot, chunk_id);
    _slots.Insert(slot);
    // s3_fifo evicts inside insert(); the evictor parks evicted slots
    // on `_chunk_evicted` because it can't free them itself (cache
    // iterator would dangle). Erase here, after insert() returns.
    while (!_chunk_evicted.empty()) {
      auto& s = _chunk_evicted.front();
      _chunk_evicted.pop_front();
      _chunk_index.erase(s.chunk_id);
    }
    return &slot;
  }

  const float* SliceOf(const ChunkSlot& slot, uint64_t row) const noexcept {
    const uint64_t in_chunk = row - slot.chunk_id * _chunk_rows;
    return slot.base + in_chunk * _d;
  }

  void Load(ChunkSlot& slot, uint64_t chunk_id) {
    const uint64_t total_rows = _child->RowCount() / _d;
    const uint64_t start_row = chunk_id * _chunk_rows;
    const uint64_t take =
      std::min<uint64_t>(_chunk_rows, total_rows - start_row);
    const uint64_t start_elem = start_row * _d;
    const uint64_t take_elems = take * _d;

    uint64_t produced = 0;
    while (produced < take_elems) {
      const uint64_t pos = start_elem + produced;
      _locate_hint = _child->Locate(pos, _locate_hint);
      const uint64_t in_rg = pos - _locate_hint.begin;
      const uint64_t to_take =
        std::min<uint64_t>(take_elems - produced, _locate_hint.end - pos);

      auto& rgs = GetRgSlot(_locate_hint.rg);
      if (in_rg < rgs.cursor.Position()) {
        rgs.cursor.Rewind();
      }
      rgs.cursor.SeekTo(in_rg);
      rgs.cursor.Scan(to_take, slot.data, static_cast<duckdb::idx_t>(produced));
      produced += to_take;
    }
  }

  const ColumnReader* _child;
  uint64_t _d;
  uint64_t _chunk_rows;

  std::array<ChunkSlot*, 2> _pin_stack{};
  size_t _pin_depth = 0;

  ChunkMap _chunk_index;
  sdb::containers::s3_fifo::Cache<ChunkSlot, ChunkEvictor> _slots;
  ChunkPendingList _chunk_evicted;

  RgMap _rg_index;
  sdb::containers::s3_fifo::Cache<RgSlot, RgEvictor> _rgs;
  RgPendingList _rg_evicted;
  RgWindow _locate_hint;
};

inline bool ChunkEvictor::operator()(ChunkSlot& slot) const noexcept {
  if (slot.pinned != 0) {
    return false;
  }
  self->_chunk_evicted.push_back(slot);
  return true;
}

inline bool RgEvictor::operator()(RgSlot& slot) const noexcept {
  self->_rg_evicted.push_back(slot);
  return true;
}

// Distance computer over the ARRAY child column. doc_id space is 1-based
// (faiss::idx_t == iresearch doc_id == row + doc_limits::min()), so the
// access key is (id - doc_limits::min()). set_query stores the
// caller-owned query pointer; dis(id) fetches one slice and forwards
// it to dist(). symmetric_dis(i, j) needs both slices to coexist for
// the dist() call -- pin the i-side chunk before fetching j so the
// second cache->Get's eviction can't free `a` out from under us.
struct ColumnDistance final : public faiss::DistanceComputer {
  ColumnDistance(HNSWInfo info, ChunkedVectorCache* cache) noexcept
    : dim{info.d}, dist{ResolveDistanceFunction(info.metric)}, cache{cache} {}

  void set_query(const float* x) final { q = x; }

  float operator()(faiss::idx_t id) final {
    const auto* slice =
      cache->Get(static_cast<uint64_t>(id - doc_limits::min()));
    return dist(reinterpret_cast<const byte_type*>(q),
                reinterpret_cast<const byte_type*>(slice),
                static_cast<uint16_t>(dim));
  }

  float symmetric_dis(faiss::idx_t i, faiss::idx_t j) final {
    const auto* a = cache->Pin(static_cast<uint64_t>(i - doc_limits::min()));
    const auto* b = cache->Get(static_cast<uint64_t>(j - doc_limits::min()));
    const auto r =
      dist(reinterpret_cast<const byte_type*>(a),
           reinterpret_cast<const byte_type*>(b), static_cast<uint16_t>(dim));
    cache->Unpin();
    return r;
  }

  int32_t dim;
  float (*dist)(const byte_type*, const byte_type*, uint16_t);
  ChunkedVectorCache* cache;
  const float* q = nullptr;
};

}  // namespace

HNSWWriter::HNSWWriter(HNSWInfo info) : _info{std::move(info)}, _hnsw{_info.m} {
  _hnsw.efConstruction = _info.ef_construction;
}

HNSWWriter::~HNSWWriter() = default;

void HNSWWriter::Build(const ColumnReader& vector_column) {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto array_size = vector_column.ArraySize();
  const auto rows = vector_column.RowCount();

  // faiss node N == iresearch doc_id (1-based) for row (N - 1).
  const auto graph_nodes = rows + doc_limits::min();
  faiss::VisitedTable vt{static_cast<int>(graph_nodes)};
  _hnsw.prepare_level_tab(graph_nodes, false);

  ChunkedVectorCache cache{*child, array_size, kChunkCacheSlots};
  ColumnDistance dis{_info, &cache};

  const uint64_t chunk_rows =
    std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(array_size, 1), 1);
  for (uint64_t start = 0; start < rows; start += chunk_rows) {
    const uint64_t take = std::min<uint64_t>(chunk_rows, rows - start);
    const float* base = cache.Pin(start);
    for (uint64_t k = 0; k < take; ++k) {
      dis.set_query(base + k * array_size);
      const faiss::idx_t id =
        static_cast<faiss::idx_t>(start + k + doc_limits::min());
      const int level = _hnsw.levels[id] - 1;
      vt.advance();
      _hnsw.add_with_locks(dis, level, id, vt, false);
    }
    cache.Unpin();
  }
}

void HNSWWriter::Serialize(DataOutput& out) { irs::WriteHNSW(out, _hnsw); }

HNSWReader::HNSWReader(field_id id, std::string name, HNSWInfo info,
                       const ColumnReader& vector_column,
                       const IndexInput& in_source, uint64_t graph_offset,
                       uint64_t graph_byte_size)
  : _id{id},
    _name{std::move(name)},
    _info{std::move(info)},
    _vector_column{vector_column},
    _in_source{&in_source},
    _graph_offset{graph_offset},
    _graph_byte_size{graph_byte_size} {
  SDB_ENSURE(vector_column.ArraySize() == static_cast<uint64_t>(_info.d),
             sdb::ERROR_INTERNAL, "columnstore::HNSWReader: ARRAY size ",
             vector_column.ArraySize(), " does not match HNSWInfo.d ", _info.d);
}

HNSWReader::~HNSWReader() = default;

std::shared_ptr<const faiss::HNSW> HNSWReader::GraphIfLoaded() const noexcept {
  return std::atomic_load_explicit(&_hnsw, std::memory_order_acquire);
}

void HNSWReader::UpdateGraph(
  std::shared_ptr<const faiss::HNSW> g) const noexcept {
  SDB_ASSERT(g);
  std::shared_ptr<const faiss::HNSW> expected;
  std::atomic_compare_exchange_strong_explicit(&_hnsw, &expected, std::move(g),
                                               std::memory_order_release,
                                               std::memory_order_acquire);
}

std::shared_ptr<const faiss::HNSW> HNSWReader::Graph() const {
  ResolveGraph();
  return std::atomic_load_explicit(&_hnsw, std::memory_order_acquire);
}

const faiss::HNSW& HNSWReader::ResolveGraph() const {
  if (auto g = std::atomic_load_explicit(&_hnsw, std::memory_order_acquire)) {
    return *g;
  }
  auto graph = std::make_shared<faiss::HNSW>();
  {
    auto in = _in_source->Reopen();
    in->Seek(_graph_offset);
    irs::ReadHNSW(*in, *graph);
    SDB_ASSERT(in->Position() - _graph_offset == _graph_byte_size);
  }
  std::shared_ptr<const faiss::HNSW> g{std::move(graph)};
  std::shared_ptr<const faiss::HNSW> expected;
  if (!std::atomic_compare_exchange_strong_explicit(
        &_hnsw, &expected, g, std::memory_order_release,
        std::memory_order_acquire)) {
    return *expected;
  }
  return *g;
}

void HNSWReader::Search(HNSWSearchContext& ctx) const {
  const auto& hnsw = ResolveGraph();
  const auto* child = _vector_column.Child();
  SDB_ASSERT(child);
  ChunkedVectorCache cache{*child, _vector_column.ArraySize(),
                           kChunkCacheSlots};
  ColumnDistance dis{_info, &cache};
  dis.set_query(reinterpret_cast<const float*>(ctx.info.query));

  HNSWSegmentResultHandler res{ctx.segment_id, ctx.handler,
                               ctx.info.global_threshold};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0, false);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

void HNSWReader::RangeSearch(HNSWRangeSearchContext& ctx) const {
  const auto& hnsw = ResolveGraph();
  const auto* child = _vector_column.Child();
  SDB_ASSERT(child);
  ChunkedVectorCache cache{*child, _vector_column.ArraySize(),
                           kChunkCacheSlots};
  ColumnDistance dis{_info, &cache};
  dis.set_query(reinterpret_cast<const float*>(ctx.info.query));

  HNSWRangeSegmentResultHandler res{ctx.segment_id, ctx.handler};
  ctx.vt.visited.resize(hnsw.levels.size(), 0);
  ctx.vt.advance();
  res.begin(0);
  hnsw.search(dis, nullptr, res, ctx.vt, &ctx.info.params);
}

}  // namespace irs::columnstore
