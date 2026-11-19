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

#include <limits>
#include <string_view>
#include <utility>

namespace irs::term_dict {

BlockStore::~BlockStore() { _rm->DecreaseChecked(_bytes); }

void BlockStore::Open(IndexInput::ptr in) {
  _in = std::move(in);
  if (!_in) {
    return;
  }
  const uint64_t length = _in->Length();
  _base = _in->ReadStable(0, length);
  if (!_base) {
    Account(length);
    _file.resize(length);
    _in->ReadData(0, _file.data(), length);
    _base = _file.data();
  }
}

uint64_t BlockStore::CountMappedMemory() const {
  return _in ? _in->CountMappedMemory() : 0;
}

bytes_view BlockStore::Copy(uint64_t offset, uint64_t size) {
  const auto [it, inserted] = _copies.try_emplace(offset);
  auto& copy = it->second;
  if (inserted) {
    Account(size);
    copy.resize(size);
    _pending.push_back({.offset = offset, .size = size, .data = copy.data()});
  }
  return copy;
}

const byte_type* BlockStore::CopyBlock(uint32_t block) {
  const uint64_t length = _in->Length();
  const uint64_t offset = BlockOffset(block);
  return Copy(offset, std::min(BlockOffset(block + 1), length) - offset).data();
}

void BlockStore::FillCopies() {
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
  for (const auto& copy : _pending) {
    _in->ReadData(copy.offset, copy.data, copy.size);
  }
  _pending.clear();
  _pending.shrink_to_fit();
}

void BlockStore::Bind(const TermsMeta& terms) {
  _block_count = terms.blocks;
  _first_page = terms.first_page;
  _blocks_end = terms.inline_offset;
  const auto blocks = _block_count;
  if (terms.inline_size != 0 || !terms.inline_starts.empty() ||
      _blocks_end > _in->Length()) {
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
