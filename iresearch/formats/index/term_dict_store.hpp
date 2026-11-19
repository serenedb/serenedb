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
#include <iterator>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/containers/node_hash_map.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

class BlockStore {
 public:
  explicit BlockStore(IResourceManager& rm) noexcept : _rm{&rm} {}
  ~BlockStore();

  BlockStore(const BlockStore&) = delete;
  BlockStore& operator=(const BlockStore&) = delete;

  void Open(IndexInput::ptr in);
  void Bind(const TermsMeta& terms);
  void FillCopies();
  void Account(uint64_t bytes) {
    if (bytes != 0) {
      _rm->Increase(bytes);
      _bytes += bytes;
    }
  }

  bool Opened() const noexcept { return _in != nullptr; }
  uint64_t CountMappedMemory() const;
  const IndexInput& Input() const noexcept { return *_in; }
  const byte_type* Base() const noexcept { return _base; }

  uint32_t BlockCount() const noexcept { return _block_count; }
  uint64_t BlockOffset(size_t block) const noexcept {
    const uint64_t page = _first_page + (_pages.empty() ? block : _pages[block]);
    return std::min(page * kPage, _blocks_end);
  }

  bytes_view Copy(uint64_t offset, uint64_t size);
  const byte_type* CopyBlock(uint32_t block);

 private:
  struct PendingCopy {
    uint64_t offset;
    uint64_t size;
    byte_type* data;
  };

  IResourceManager* _rm;
  IndexInput::ptr _in;
  const byte_type* _base = nullptr;
  bstring _file;
  std::vector<uint32_t> _pages;
  containers::NodeHashMap<uint64_t, bstring> _copies;
  std::vector<PendingCopy> _pending;
  uint64_t _first_page = 0;
  uint64_t _blocks_end = 0;
  uint64_t _bytes = 0;
  uint32_t _block_count = 0;
};

}  // namespace irs::term_dict
