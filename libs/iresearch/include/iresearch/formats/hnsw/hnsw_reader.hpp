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
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/HNSW.h>
#include <faiss/impl/ResultHandler.h>
#include <faiss/impl/io.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "basics/containers/s3fifo.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/types.hpp"

namespace irs {

class DataOutput;
class IndexInput;
class ReadContext;

inline constexpr uint64_t kChunkSizeFloats = 4 * STANDARD_VECTOR_SIZE;
inline constexpr size_t kChunkCacheSlots = 64;
inline constexpr size_t kBlockCacheSlots = 8;

inline constexpr size_t kMaxPinDepth = 2;

struct ChunkSlot : public sdb::containers::s3fifo::Node {
  uint64_t chunk_id = std::numeric_limits<uint64_t>::max();
  std::optional<duckdb::Vector> data;
  const float* base = nullptr;
  uint8_t pinned = 0;

  ChunkSlot() = default;
  ChunkSlot(const ChunkSlot&) = delete;
  ChunkSlot& operator=(const ChunkSlot&) = delete;
  ChunkSlot(ChunkSlot&&) = delete;
  ChunkSlot& operator=(ChunkSlot&&) = delete;

  void Allocate(duckdb::idx_t size) {
    data.emplace(duckdb::LogicalType::FLOAT, size);
    base = duckdb::FlatVector::GetData<float>(*data);
  }

  bool Evictable() noexcept { return pinned == 0; }
};

class ScanCursor {
 public:
  ScanCursor() noexcept = default;
  explicit ScanCursor(std::unique_ptr<duckdb::ColumnSegment> block)
    : _block{std::move(block)} {
    _block->InitializeScan(_state);
  }

  ScanCursor(const ScanCursor&) = delete;
  ScanCursor& operator=(const ScanCursor&) = delete;
  ScanCursor(ScanCursor&&) noexcept = default;
  ScanCursor& operator=(ScanCursor&&) noexcept = default;

  void Reset() noexcept {
    _state = duckdb::ColumnScanState{nullptr};
    _block->InitializeScan(_state);
    _cursor = 0;
  }

  void SeekTo(uint64_t target) noexcept {
    SDB_ASSERT(target >= _cursor);
    if (target > _cursor) {
      _state.offset_in_column = target;
      _block->Skip(_state);
      _cursor = target;
    }
  }

  void Scan(duckdb::idx_t count, duckdb::Vector& out_vec,
            duckdb::idx_t out_offset) {
    _block->Scan(_state, count, out_vec, out_offset,
                 duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
    _state.offset_in_column += count;
    _state.internal_index += count;
    _cursor += count;
  }

  uint64_t Position() const noexcept { return _cursor; }

 private:
  std::unique_ptr<duckdb::ColumnSegment> _block;
  duckdb::ColumnScanState _state{nullptr};
  uint64_t _cursor = 0;
};

struct BlockSlot : public sdb::containers::s3fifo::Node {
  size_t block = std::numeric_limits<size_t>::max();
  ScanCursor cursor;

  BlockSlot() = default;
  BlockSlot(const BlockSlot&) = delete;
  BlockSlot& operator=(const BlockSlot&) = delete;
  BlockSlot(BlockSlot&&) = delete;
  BlockSlot& operator=(BlockSlot&&) = delete;
};

class ChunkedVectorCache {
 public:
  using ChunkMap = sdb::containers::NodeHashMap<uint64_t, ChunkSlot>;
  using BlockMap = sdb::containers::NodeHashMap<size_t, BlockSlot>;

  ChunkedVectorCache()
    : _slots{{.capacity = kChunkCacheSlots,
              .small_size = std::max<size_t>(kChunkCacheSlots / 10, 1)}},
      _blocks{{.capacity = kBlockCacheSlots,
               .small_size = std::max<size_t>(kBlockCacheSlots / 4, 1)}} {}

  ChunkedVectorCache(const ChunkedVectorCache&) = delete;
  ChunkedVectorCache& operator=(const ChunkedVectorCache&) = delete;
  ChunkedVectorCache(ChunkedVectorCache&&) = delete;
  ChunkedVectorCache& operator=(ChunkedVectorCache&&) = delete;

  ~ChunkedVectorCache() noexcept {
    _slots.Clear();
    _blocks.Clear();
  }

  void Unbind() noexcept {
    SDB_ASSERT(_pin_depth == 0);
    _slots.Clear();
    _blocks.Clear();
    _chunk_index.clear();
    _block_index.clear();
    _locate_hint = {};
    _child = nullptr;
    _ctx = nullptr;
  }

  void Rebind(const ColumnReader& child, uint64_t array_size,
              ReadContext& ctx) {
    Unbind();
    _child = &child;
    _ctx = &ctx;
    _d = array_size;
    _chunk_rows =
      std::max<uint64_t>(kChunkSizeFloats / std::max<uint64_t>(_d, 1), 1);
    _total_rows = _child->RowCount() / _d;
  }

  uint64_t ChunkRows() const noexcept { return _chunk_rows; }

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

  BlockSlot& GetBlockSlot(size_t block) {
    auto [it, inserted] = _block_index.try_emplace(block);
    auto& slot = it->second;
    if (!inserted && !slot.Detached()) {
      slot.Hit();
      return slot;
    }
    slot.block = block;
    slot.cursor = ScanCursor{_child->OpenSegment(block, *_ctx)};
    _blocks.Insert(
      slot, [](BlockSlot& evicted) noexcept { evicted.cursor = ScanCursor{}; });
    if (_block_index.size() > 4 * kBlockCacheSlots) {
      Gc(_block_index, _blocks);
    }
    return slot;
  }

 private:
  ChunkSlot* FindOrLoad(uint64_t row) {
    const uint64_t chunk_id = row / _chunk_rows;
    auto [it, inserted] = _chunk_index.try_emplace(chunk_id);
    auto& slot = it->second;
    if (!inserted && !slot.Detached()) {
      slot.Hit();
      return &slot;
    }
    slot.chunk_id = chunk_id;
    slot.pinned = 0;
    if (!slot.data) {
      slot.Allocate(static_cast<duckdb::idx_t>(_chunk_rows * _d));
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
    const uint64_t start_row = chunk_id * _chunk_rows;
    const uint64_t take =
      std::min<uint64_t>(_chunk_rows, _total_rows - start_row);
    const uint64_t start_elem = start_row * _d;
    const uint64_t take_elems = take * _d;

    uint64_t produced = 0;
    while (produced < take_elems) {
      const uint64_t pos = start_elem + produced;
      _locate_hint = _child->Locate(pos, _locate_hint);
      const uint64_t in_block = pos - _locate_hint.begin;
      const uint64_t to_take =
        std::min<uint64_t>(take_elems - produced, _locate_hint.end - pos);

      auto& blocks = GetBlockSlot(_locate_hint.block);
      if (in_block < blocks.cursor.Position()) {
        blocks.cursor.Reset();
      }
      blocks.cursor.SeekTo(in_block);
      blocks.cursor.Scan(to_take, *slot.data,
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
  uint64_t _total_rows = 0;

  std::array<ChunkSlot*, kMaxPinDepth> _pin_stack{};
  size_t _pin_depth = 0;

  ChunkMap _chunk_index;
  sdb::containers::s3fifo::Cache<ChunkSlot> _slots;

  BlockMap _block_index;
  sdb::containers::s3fifo::Cache<BlockSlot> _blocks;
  BlockWindow _locate_hint;
};

void ReadHNSW(IndexInput& in, faiss::HNSW& hnsw);

uint64_t PackSegmentWithDoc(uint32_t segment, doc_id_t doc);

std::pair<uint32_t, doc_id_t> UnpackSegmentWithDoc(uint64_t id);

using HNSWResultHandler = faiss::HeapBlockResultHandler<faiss::HNSW::C>;
using HNSWRangeResultHandler =
  faiss::RangeSearchBlockResultHandler<faiss::HNSW::C>;

class HNSWSegmentResultHandler : public HNSWResultHandler::SingleResultHandler {
 public:
  explicit HNSWSegmentResultHandler(uint32_t segment_id,
                                    HNSWResultHandler& handler,
                                    float global_threshold,
                                    const DocumentMask* docs_mask = nullptr)
    : HNSWResultHandler::SingleResultHandler{handler},
      _segment_id{segment_id},
      _docs_mask{docs_mask},
      _global_threshold{global_threshold} {}

  void begin(size_t i, bool heapify = true) {
    HNSWResultHandler::SingleResultHandler::begin(i, heapify);
    if (faiss::HNSW::C::cmp(threshold, _global_threshold)) {
      threshold = _global_threshold;
    }
  }

  bool add_result(float dis, int64_t idx) final {
    if (_docs_mask && _docs_mask->contains(static_cast<doc_id_t>(idx))) {
      return true;
    }
    return HNSWResultHandler::SingleResultHandler::add_result(
      dis, PackSegmentWithDoc(_segment_id, static_cast<doc_id_t>(idx)));
  }

 private:
  uint32_t _segment_id;
  const DocumentMask* _docs_mask;
  float _global_threshold;
};

class HNSWRangeSegmentResultHandler
  : public HNSWRangeResultHandler::SingleResultHandler {
 public:
  explicit HNSWRangeSegmentResultHandler(
    uint32_t segment_id, HNSWRangeResultHandler& handler,
    const DocumentMask* docs_mask = nullptr)
    : HNSWRangeResultHandler::SingleResultHandler{handler},
      _segment_id{segment_id},
      _docs_mask{docs_mask} {}

  bool add_result(float dis, int64_t idx) final {
    if (_docs_mask && _docs_mask->contains(static_cast<doc_id_t>(idx))) {
      return true;
    }
    return HNSWRangeResultHandler::SingleResultHandler::add_result(
      dis, PackSegmentWithDoc(_segment_id, static_cast<doc_id_t>(idx)));
  }

 private:
  uint32_t _segment_id;
  const DocumentMask* _docs_mask;
};

struct HNSWSearchBaseBuffer {
  faiss::VisitedTable vt{0};
  ChunkedVectorCache cache;
};

struct HNSWAnnSearchBuffer : HNSWSearchBaseBuffer {
  std::span<float> dis;
  std::span<int64_t> ids;
  float max_dist;

  HNSWAnnSearchBuffer(float* dis_data, int64_t* ids_data, size_t size,
                      float max_dist = std::numeric_limits<float>::max())
    : dis{dis_data, size}, ids{ids_data, size}, max_dist{max_dist} {
    ResetValues();
  }

  void ReorderResult() {
    faiss::heap_reorder<faiss::HNSW::C>(dis.size(), dis.data(), ids.data());
  }

  void ResetValues() {
    std::ranges::fill(dis, max_dist);
    std::ranges::fill(ids, -1);
  }
};

struct HNSWSearchInfo {
  const byte_type* query;
  size_t top_k;
  faiss::SearchParametersHNSW params;
  float global_threshold = faiss::HNSW::C::neutral();
};

struct HNSWSearchContext {
  const HNSWSearchInfo& info;
  uint32_t segment_id;
  faiss::VisitedTable& vt;
  HNSWResultHandler& handler;
  ChunkedVectorCache& cache;
  const DocumentMask* docs_mask = nullptr;
};

struct HNSWRangeSearchInfo {
  const byte_type* query;
  float radius;
  faiss::SearchParametersHNSW params;
};

struct HNSWRangeSearchBuffer : HNSWSearchBaseBuffer {
  std::vector<float> dis;
  std::vector<int64_t> ids;
};

struct HNSWRangeSearchContext {
  const HNSWRangeSearchInfo& info;
  uint32_t segment_id;
  faiss::VisitedTable& vt;
  HNSWRangeResultHandler& handler;
  ChunkedVectorCache& cache;
  const DocumentMask* docs_mask = nullptr;
};

class HnswReader final {
 public:
  HnswReader(field_id id, std::shared_ptr<const faiss::HNSW> hnsw,
             HNSWInfo info, const ColumnReader& vector_column);
  ~HnswReader();

  HnswReader(const HnswReader&) = delete;
  HnswReader& operator=(const HnswReader&) = delete;

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

}  // namespace irs
