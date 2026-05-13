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

#pragma once

#include <faiss/impl/HNSW.h>

#include <array>
#include <boost/intrusive/list.hpp>
#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <limits>
#include <memory>
#include <string>

#include "basics/containers/node_hash_map.h"
#include "basics/containers/s3_fifo.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class DataOutput;
class IndexInput;

struct HNSWSearchContext;
struct HNSWRangeSearchContext;

namespace columnstore {

inline constexpr uint64_t kChunkSizeFloats = 4 * STANDARD_VECTOR_SIZE;
inline constexpr size_t kChunkCacheSlots = 64;
inline constexpr size_t kRgCacheSlots = 8;

class ChunkedVectorCache;

using PendingHook = boost::intrusive::list_member_hook<
  boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;

struct ChunkSlot : public sdb::containers::s3_fifo::CacheHook {
  uint64_t chunk_id = std::numeric_limits<uint64_t>::max();
  duckdb::Vector data;
  const float* base;
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

  ChunkedVectorCache()
    : _slots{{.cache_size = kChunkCacheSlots,
              .small_size = std::max<size_t>(kChunkCacheSlots / 10, 1)},
             ChunkEvictor{this}},
      _rgs{{.cache_size = kRgCacheSlots,
            .small_size = std::max<size_t>(kRgCacheSlots / 4, 1)},
           RgEvictor{this}} {}

  ChunkedVectorCache(const ColumnReader& child, uint64_t array_size)
    : ChunkedVectorCache{} {
    Rebind(child, array_size);
  }

  ChunkedVectorCache(const ChunkedVectorCache&) = delete;
  ChunkedVectorCache& operator=(const ChunkedVectorCache&) = delete;
  ChunkedVectorCache(ChunkedVectorCache&&) = delete;
  ChunkedVectorCache& operator=(ChunkedVectorCache&&) = delete;

  void Rebind(const ColumnReader& child, uint64_t array_size);

  const float* Get(uint64_t row) { return SliceOf(*FindOrLoad(row), row); }

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

  RgSlot& GetRgSlot(size_t rg);

  friend struct ChunkEvictor;
  friend struct RgEvictor;

 private:
  ChunkSlot* FindOrLoad(uint64_t row);

  const float* SliceOf(const ChunkSlot& slot, uint64_t row) const noexcept {
    const uint64_t in_chunk = row - slot.chunk_id * _chunk_rows;
    return slot.base + in_chunk * _d;
  }

  void Load(ChunkSlot& slot, uint64_t chunk_id);

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

// Builds a faiss::HNSW for an ARRAY<FLOAT, N> column. Construction is
// deferred until after the typed column is durable on disk: Build() reads
// vector slices on demand from the ARRAY child column through a small
// MRU chunk cache, so no flat in-memory copy of the segment's vectors is
// ever held -- not during ingest, not during graph construction.
class HNSWWriter final {
 public:
  HNSWWriter(HNSWInfo info);
  ~HNSWWriter();

  HNSWWriter(const HNSWWriter&) = delete;
  HNSWWriter& operator=(const HNSWWriter&) = delete;

  // Build the graph from the typed ARRAY column. Iterates the column in
  // STANDARD_VECTOR_SIZE-floats chunks; the distance computer reads
  // neighbor slices through an MRU chunk cache. Must be called before
  // Serialize().
  void Build(const ColumnReader& vector_column);

  void Serialize(DataOutput& out);

  const HNSWInfo& Info() const noexcept { return _info; }

  const std::shared_ptr<faiss::HNSW>& Graph() const noexcept { return _hnsw; }

 private:
  HNSWInfo _info;
  std::shared_ptr<faiss::HNSW> _hnsw;
};

// Reader-side counterpart: loads the faiss::HNSW graph from the .cs
// footer side-payload and exposes Search / RangeSearch that fetch
// vectors on demand from the ARRAY ColumnReader's child via the same
// MRU chunk cache used during Build. No per-Reader full-segment cache.
class HNSWReader final {
 public:
  HNSWReader(field_id id, std::shared_ptr<faiss::HNSW> hnsw, HNSWInfo info,
             const ColumnReader& vector_column);
  ~HNSWReader();

  HNSWReader(const HNSWReader&) = delete;
  HNSWReader& operator=(const HNSWReader&) = delete;

  field_id Id() const noexcept { return _id; }
  const HNSWInfo& Info() const noexcept { return _info; }

  void Search(HNSWSearchContext& ctx) const;
  void RangeSearch(HNSWRangeSearchContext& ctx) const;

  ChunkedVectorCache& PrepareCache(ChunkedVectorCache& slot) const;

  const std::shared_ptr<const faiss::HNSW>& Graph() const noexcept {
    return _hnsw;
  }

 private:
  field_id _id;
  HNSWInfo _info;
  const ColumnReader& _vector_column;
  std::shared_ptr<const faiss::HNSW> _hnsw;
};

}  // namespace columnstore
}  // namespace irs
