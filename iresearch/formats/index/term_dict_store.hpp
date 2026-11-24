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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <iterator>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

class BlockStore {
 public:
  BlockStore() = default;
  ~BlockStore();

  BlockStore(const BlockStore&) = delete;
  BlockStore& operator=(const BlockStore&) = delete;

  void Open(const duckdb::InputFile* in, duckdb::BufferManager& buffers);
  void Bind(const TermsMeta& terms);
  void Finish();
  void Account(uint64_t bytes) {
    if (bytes != 0) {
      _buffers->ReserveMemory(bytes);
      _reserved += bytes;
    }
  }

  bool Opened() const noexcept { return _in; }
  bool Mapped() const noexcept { return _base; }
  const duckdb::InputFile& Input() const noexcept { return *_in; }
  const byte_type* Base() const noexcept { return _base; }

  uint32_t BlockCount() const noexcept { return _block_count; }
  uint64_t BlockOffset(size_t block) const noexcept {
    const uint64_t page =
      _first_page + (_pages.empty() ? block : _pages[block]);
    return std::min(page * kPage, _blocks_end);
  }

  bytes_view Copy(uint64_t offset, uint64_t size);
  const byte_type* CopyBlock(uint32_t block);
  bytes_view Read(uint64_t offset, uint64_t size, bstring& buffer) const;

 private:
  struct PendingCopy {
    uint64_t offset;
    uint64_t size;
    byte_type* data;
  };

  const duckdb::InputFile* _in = nullptr;
  duckdb::BufferManager* _buffers = nullptr;
  const byte_type* _base = nullptr;
  std::vector<uint32_t> _pages;
  containers::FlatHashMap<uint64_t, duckdb::AllocatedData> _copies;
  std::vector<PendingCopy> _pending;
  uint64_t _first_page = 0;
  uint64_t _blocks_end = 0;
  uint64_t _reserved = 0;
  uint32_t _block_count = 0;
};

}  // namespace irs::term_dict
