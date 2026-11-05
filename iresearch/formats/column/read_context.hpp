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

#include <atomic>
#include <deque>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/internal/block_manager.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

class ColReader;

bool StreamsFrames(const duckdb::LogicalType& type);

class ReadContext final : public BlockManager,
                          public duckdb::OverflowStringReader,
                          public duckdb::ColumnStreamReader {
 public:
  explicit ReadContext(duckdb::DatabaseInstance& db) noexcept;
  explicit ReadContext(const ColReader& reader, bool random_access = false);
  ReadContext(duckdb::DatabaseInstance& db, IndexInput::ptr in);
  ~ReadContext() final;

  ReadContext(const ReadContext&) = delete;
  ReadContext& operator=(const ReadContext&) = delete;
  ReadContext(ReadContext&&) = delete;
  ReadContext& operator=(ReadContext&&) = delete;

  void Reset(const ColReader& reader);

  IndexInput& In() noexcept { return *_in; }
  const IndexInput& In() const noexcept { return *_in; }
  bool HasIn() const noexcept { return _in != nullptr; }
  bool RandomAccess() const noexcept { return _random_access; }
  bool ReadAhead() const noexcept { return _read_ahead; }
  void SetReadAhead(bool read_ahead) noexcept { _read_ahead = read_ahead; }
  bool MergeRead() const noexcept { return _merge_read; }
  void SetMergeRead(bool merge_read) noexcept { _merge_read = merge_read; }
  duckdb::ObjectCache* Cache() const noexcept { return _cache; }
  void SetCache(duckdb::ObjectCache* cache) noexcept { _cache = cache; }
  bool Streams() const noexcept { return _merge_read && _streams; }
  void SetStreams(bool streams) noexcept { _streams = streams; }

  duckdb::string_t ReadString(duckdb::Vector& result, duckdb::block_id_t block,
                              int32_t offset) final;

  duckdb::const_data_ptr_t TryReadStable(duckdb::idx_t position,
                                         duckdb::idx_t size) final;
  void Read(duckdb::idx_t position, duckdb::data_ptr_t target,
            duckdb::idx_t size) final;

  struct CacheSlot {
    uint64_t file_id = 0;
    uint64_t file_offset = 0;
    std::atomic<bool>* touched = nullptr;
    duckdb::ObjectCache* cache = nullptr;
    const codecs::TrainedDictionaries* dictionaries = nullptr;
    bool streams = false;
  };

  duckdb::shared_ptr<duckdb::BlockHandle> RegisterColBlock(uint64_t offset,
                                                           uint64_t size,
                                                           CacheSlot slot);
  const CacheSlot& CacheSlotOf(duckdb::block_id_t block) const noexcept {
    const auto id = static_cast<size_t>(block);
    SDB_ASSERT(id < _cache_slots.size());
    return _cache_slots[id];
  }

  duckdb::unique_ptr<duckdb::Block> CreateBlock(
    duckdb::block_id_t block_id, duckdb::FileBuffer* source_buffer) final;

  void Read(duckdb::QueryContext context, duckdb::Block& block) final;
  void ReadBlocks(duckdb::QueryContext context, duckdb::FileBuffer& buffer,
                  duckdb::block_id_t start_block,
                  duckdb::idx_t block_count) final;
  void UnregisterBlock(duckdb::block_id_t id) final;

  duckdb::block_id_t GetFreeBlockId() final;
  duckdb::block_id_t PeekFreeBlockId() final;
  void Write(duckdb::FileBuffer& block, duckdb::block_id_t block_id) final;
  void Write(duckdb::QueryContext context, duckdb::FileBuffer& block,
             duckdb::block_id_t block_id) final;
  bool IsRootBlock(duckdb::MetaBlockPointer root) final;
  duckdb::idx_t GetMetaBlock() final;
  void WriteHeader(duckdb::QueryContext context,
                   duckdb::DatabaseHeader header) final;
  duckdb::idx_t TotalBlocks() final;

 private:
  void ResetMapping();
  bool Maps(uint64_t offset, uint64_t size) const noexcept;

  IndexInput::ptr _in;
  duckdb::unique_ptr<duckdb::MemoryMappedFile> _mapping;
  std::vector<std::pair<uint64_t, uint64_t>> _ranges;
  std::deque<CacheSlot> _cache_slots;
  bool _random_access = false;
  bool _read_ahead = true;
  bool _merge_read = false;
  duckdb::ObjectCache* _cache;
  bool _streams = false;
  std::atomic<size_t> _live_handles{0};
};

const ReadContext::CacheSlot& CacheSlotOf(
  duckdb::ColumnSegment& segment) noexcept;

inline bool Touched(const ReadContext::CacheSlot& slot) noexcept {
  return slot.touched->load(std::memory_order_relaxed);
}

inline bool SecondTouch(const ReadContext::CacheSlot& slot) noexcept {
  return Touched(slot) ||
         slot.touched->exchange(true, std::memory_order_relaxed);
}

}  // namespace irs
