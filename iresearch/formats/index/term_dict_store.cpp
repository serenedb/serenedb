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

#include "iresearch/formats/index/term_dict_store.hpp"

#include <duckdb/storage/external_file_cache/input_file_reader.hpp>
#include <limits>
#include <string_view>
#include <utility>

namespace irs::term_dict {

BlockStore::~BlockStore() {
  if (_reserved != 0) {
    _buffers->FreeReservedMemory(_reserved);
  }
}

void BlockStore::Open(const duckdb::InputFile* in,
                      duckdb::BufferManager& buffers) {
  _in = in;
  _buffers = &buffers;
  if (_in) {
    _base = _in->Data();
  }
}

bytes_view BlockStore::Copy(uint64_t offset, uint64_t size) {
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    copy = _buffers->GetBufferAllocator().Allocate(size);
    _pending.push_back({.offset = offset, .size = size, .data = copy.get()});
  }
  return {copy.get(), size};
}

const byte_type* BlockStore::CopyBlock(uint32_t block) {
  const uint64_t offset = BlockOffset(block);
  return Copy(offset, BlockOffset(block + 1) - offset).data();
}

void BlockStore::Finish() {
  absl::c_sort(_pending, [](const PendingCopy& lhs, const PendingCopy& rhs) {
    return lhs.offset < rhs.offset;
  });
  for (size_t i = 0; i != _pending.size();) {
    const uint64_t begin = _pending[i].offset;
    uint64_t end = begin + _pending[i].size;
    while (++i != _pending.size() && _pending[i].offset <= end) {
      end = std::max(end, _pending[i].offset + _pending[i].size);
    }
    _in->Prefetch(begin, end - begin);
  }
  duckdb::InputFileReader in{*_in};
  for (const auto& copy : _pending) {
    in.Seek(copy.offset);
    in.ReadData(copy.data, copy.size);
  }
  _pending.clear();
  _pending.shrink_to_fit();
  if (!_base && _block_count != 0) {
    _in->Prefetch(BlockOffset(0), BlockOffset(_block_count) - BlockOffset(0));
  }
}

void BlockStore::Bind(const TermsMeta& terms) {
  _block_count = terms.blocks;
  _first_page = terms.first_page;
  _blocks_end = terms.blocks_end;
  const auto blocks = _block_count;
  if (_blocks_end > _in->GetSize()) {
    Corrupted("block directory");
  }
  if (terms.spans.size() > blocks) {
    Corrupted("block directory");
  }
  if (!terms.spans.empty()) {
    _pages.resize(size_t{blocks} + 1);
    uint64_t extra = 0;
    uint32_t next = 0;
    for (const auto& span : terms.spans) {
      if (span.extra == 0 || span.block >= blocks || span.block < next) {
        Corrupted("block directory");
      }
      for (; next <= span.block; ++next) {
        _pages[next] = static_cast<uint32_t>(next + extra);
      }
      extra += span.extra;
      if (extra + blocks > std::numeric_limits<uint32_t>::max()) {
        Corrupted("block directory");
      }
    }
    for (; next <= blocks; ++next) {
      _pages[next] = static_cast<uint32_t>(next + extra);
    }
  }
  if (blocks != 0 && (_first_page > _blocks_end / kPage ||
                      BlockOffset(blocks - 1) >= _blocks_end)) {
    Corrupted("block directory");
  }
  Account(_pages.capacity() * sizeof(uint32_t));
}

}  // namespace irs::term_dict
