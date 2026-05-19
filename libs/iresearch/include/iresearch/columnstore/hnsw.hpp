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

#include <absl/container/node_hash_map.h>
#include <faiss/impl/HNSW.h>

#include <array>
#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "basics/containers/s3fifo.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class DataOutput;
class IndexInput;

struct HNSWSearchContext;
struct HNSWRangeSearchContext;

namespace columnstore {

class ReadContext;

inline constexpr uint64_t kChunkSizeFloats = 4 * STANDARD_VECTOR_SIZE;
inline constexpr size_t kChunkCacheSlots = 64;
inline constexpr size_t kRgCacheSlots = 8;

struct ChunkSlot : public sdb::containers::s3fifo::Node {
  uint64_t chunk_id = std::numeric_limits<uint64_t>::max();
  std::optional<duckdb::Vector> data;
  const float* base = nullptr;
  uint8_t pinned = 0;

  explicit ChunkSlot(duckdb::idx_t size) {
    data.emplace(duckdb::LogicalType::FLOAT, size,
                 duckdb::VectorDataInitialization::ZERO_INITIALIZE);
    base = duckdb::FlatVector::GetData<float>(*data);
  }

  ChunkSlot(const ChunkSlot&) = delete;
  ChunkSlot& operator=(const ChunkSlot&) = delete;
  ChunkSlot(ChunkSlot&&) = delete;
  ChunkSlot& operator=(ChunkSlot&&) = delete;

  bool Evictable() noexcept { return pinned == 0; }
};

struct RgSlot : public sdb::containers::s3fifo::Node {
  size_t rg = std::numeric_limits<size_t>::max();
  ColumnReader::ScanCursor cursor;

  RgSlot() = default;
  RgSlot(const RgSlot&) = delete;
  RgSlot& operator=(const RgSlot&) = delete;
  RgSlot(RgSlot&&) = delete;
  RgSlot& operator=(RgSlot&&) = delete;
};

class ChunkedVectorCache {
 public:
  using ChunkMap = sdb::containers::NodeHashMap<uint64_t, ChunkSlot>;
  using RgMap = sdb::containers::NodeHashMap<size_t, RgSlot>;

  ChunkedVectorCache()
    : _slots{{.capacity = kChunkCacheSlots,
              .small_size = std::max<size_t>(kChunkCacheSlots / 10, 1)}},
      _rgs{{.capacity = kRgCacheSlots,
            .small_size = std::max<size_t>(kRgCacheSlots / 4, 1)}} {}

  ChunkedVectorCache(const ChunkedVectorCache&) = delete;
  ChunkedVectorCache& operator=(const ChunkedVectorCache&) = delete;
  ChunkedVectorCache(ChunkedVectorCache&&) = delete;
  ChunkedVectorCache& operator=(ChunkedVectorCache&&) = delete;

  // Cache sentinels must be detached before the slot maps tear down.
  ~ChunkedVectorCache() noexcept {
    _slots.Clear();
    _rgs.Clear();
  }

  void Rebind(const ColumnReader& child, uint64_t array_size,
              ReadContext& ctx) {
    SDB_ASSERT(_pin_depth == 0);
    _slots.Clear();
    _rgs.Clear();
    _chunk_index.clear();
    _rg_index.clear();
    _locate_hint = {};
    _child = &child;
    _ctx = &ctx;
    _d = array_size;
    _chunk_rows =
      std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(_d, 1), 1);
  }

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

  RgSlot& GetRgSlot(size_t rg) {
    auto [it, inserted] = _rg_index.try_emplace(rg);
    auto& slot = it->second;
    if (!inserted && !slot.Detached()) {
      slot.Hit();
      return slot;
    }
    slot.rg = rg;
    slot.cursor = ColumnReader::ScanCursor{_child->OpenSegment(rg, *_ctx)};
    _rgs.Insert(slot, [](RgSlot& evicted) noexcept {
      evicted.cursor = ColumnReader::ScanCursor{};
    });
    if (_rg_index.size() > 4 * kRgCacheSlots) {
      Gc(_rg_index, _rgs);
    }
    return slot;
  }

 private:
  ChunkSlot* FindOrLoad(uint64_t row) {
    const uint64_t chunk_id = row / _chunk_rows;
    auto [it, inserted] = _chunk_index.try_emplace(
      chunk_id, static_cast<duckdb::idx_t>(_chunk_rows * _d));
    auto& slot = it->second;
    if (!inserted && !slot.Detached()) {
      slot.Hit();
      return &slot;
    }
    slot.chunk_id = chunk_id;
    slot.pinned = 0;
    if (!slot.data) {
      slot.data.emplace(duckdb::LogicalType::FLOAT,
                        static_cast<duckdb::idx_t>(_chunk_rows * _d),
                        duckdb::VectorDataInitialization::ZERO_INITIALIZE);
      slot.base = duckdb::FlatVector::GetData<float>(*slot.data);
    }
    Load(slot, chunk_id);
    _slots.Insert(slot, [](ChunkSlot& evicted) noexcept {
      evicted.data.reset();
      evicted.base = nullptr;
    });
    if (_chunk_index.size() > 4 * kChunkCacheSlots) {
      Gc(_chunk_index, _slots);
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
        rgs.cursor.Reset();
      }
      rgs.cursor.SeekTo(in_rg);
      rgs.cursor.Scan(to_take, *slot.data,
                      static_cast<duckdb::idx_t>(produced));
      produced += to_take;
    }
  }

  static void Gc(auto& map, auto& cache) noexcept {
    absl::erase_if(map, [&cache](const auto& entry) noexcept {
      const auto& slot = entry.second;
      return slot.Detached() && !cache.IsGhost(slot);
    });
  }

  ReadContext* _ctx = nullptr;
  const ColumnReader* _child = nullptr;
  uint64_t _d = 0;
  uint64_t _chunk_rows = 0;

  std::array<ChunkSlot*, 2> _pin_stack{};
  size_t _pin_depth = 0;

  ChunkMap _chunk_index;
  sdb::containers::s3fifo::Cache<ChunkSlot> _slots;

  RgMap _rg_index;
  sdb::containers::s3fifo::Cache<RgSlot> _rgs;
  RgWindow _locate_hint;
};

class HNSWWriter final {
 public:
  explicit HNSWWriter(HNSWInfo info);
  ~HNSWWriter();

  HNSWWriter(const HNSWWriter&) = delete;
  HNSWWriter& operator=(const HNSWWriter&) = delete;

  void Build(const ColumnReader& vector_column, ReadContext& ctx);

  void Serialize(DataOutput& out);

  const HNSWInfo& Info() const noexcept { return _info; }

  const std::shared_ptr<faiss::HNSW>& Graph() const noexcept { return _hnsw; }

 private:
  HNSWInfo _info;
  std::shared_ptr<faiss::HNSW> _hnsw;
};

class HNSWReader final {
 public:
  HNSWReader(field_id id, std::shared_ptr<const faiss::HNSW> hnsw,
             HNSWInfo info, const ColumnReader& vector_column);
  ~HNSWReader();

  HNSWReader(const HNSWReader&) = delete;
  HNSWReader& operator=(const HNSWReader&) = delete;

  field_id Id() const noexcept { return _id; }
  const HNSWInfo& Info() const noexcept { return _info; }
  const ColumnReader& VectorColumn() const noexcept { return _vector_column; }

  void Search(HNSWSearchContext& ctx) const;
  void RangeSearch(HNSWRangeSearchContext& ctx) const;

  ChunkedVectorCache& PrepareCache(ChunkedVectorCache& slot,
                                   ReadContext& ctx) const;

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
