////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "store_utils.hpp"

#include "iresearch/utils/crc.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/std.hpp"

namespace irs {

void BytesViewInput::Prefetch(uint64_t offset, uint64_t count) const noexcept {
  const auto window = Window(offset, count);
  file_utils::Prefetch(window.data(), window.size());
}

bool BytesViewInput::Resident(uint64_t offset, uint64_t count) const noexcept {
  const auto window = Window(offset, count);
  return !window.empty() &&
         file_utils::IsResident(window.data(), window.size());
}

void BytesViewInput::OnSequentialRead(const byte_type* begin,
                                      uint64_t count) noexcept {
  const auto* end = begin + count;
  if (_seq_end == nullptr) {
    _run = count;
    _skipped = 0;
    _window = kMinReadahead;
    _seq_end = end;
  } else if (begin >= _seq_end) {
    const auto gap = static_cast<uint64_t>(begin - _seq_end);
    if (gap >= file_utils::kPage) {
      _skipped += gap;
    } else {
      _run += gap;
    }
    _run += count;
    _seq_end = end;
  } else if (end > _seq_end) {
    _run += static_cast<uint64_t>(end - _seq_end);
    _seq_end = end;
  }
  if (_run >= file_utils::kPage && _run * 2 >= _skipped &&
      (_prefetch_end <= _seq_end ||
       static_cast<uint64_t>(_prefetch_end - _seq_end) < kReadaheadSlack)) {
    ReadaheadFrom(begin);
  }
}

void BytesViewInput::ReadaheadFrom(const byte_type* begin) noexcept {
  const auto* end = _data.data() + _data.size();
  if (_readahead_limit != nullptr && _readahead_limit < end) {
    end = _readahead_limit;
  }
  const auto* from = _prefetch_end > begin ? _prefetch_end : begin;
  if (from >= end) {
    return;
  }
  const auto len = std::min<uint64_t>(_window, end - from);
  if (_readahead == Readahead::Probe) {
    if (file_utils::IsResident(from, len)) {
      _readahead = Readahead::Suspended;
      return;
    }
    _readahead = Readahead::Active;
  }
  file_utils::Prefetch(from, len);
  _prefetch_end = from + len;
  _window = std::min<uint64_t>(_window * 2, file_utils::kMaxReadahead);
}

void BytesViewInput::ReadData(byte_type* b, uint64_t size) {
  const uint64_t remain = std::distance(_pos, _data.data() + _data.size());
  SDB_ENSURE(size <= remain, "short read (need ", size, " bytes, ", remain,
             " remaining)");
  if (size != 0) {
    std::memcpy(b, _pos, sizeof(byte_type) * size);
    _pos += size;
  }
}

void BytesViewInput::ReadData(uint64_t offset, byte_type* b,
                              size_t size) noexcept {
  if (offset < _data.size()) {
    size = std::min(size, size_t(_data.size() - offset));
    std::memcpy(b, _data.data() + offset, sizeof(byte_type) * size);
    _pos = _data.data() + offset + size;
    return;
  }

  _pos = _data.data() + _data.size();
}

uint32_t BytesViewInput::Checksum(uint64_t offset) const {
  Crc32c crc;

  crc.process_bytes(
    _pos, std::min<size_t>((_pos - _data.data()) + offset, _data.size()));

  return crc.checksum();
}

uint64_t RemappedBytesViewInput::Position() const noexcept {
  const auto addr = _input.Position();
  auto diff = std::numeric_limits<size_t>::max();
  SDB_ASSERT(!_mapping.empty());
  MappingValue src = _mapping.front();
  for (const auto& m : _mapping) {
    if (m.second < addr) {
      if (addr - m.second < diff) {
        diff = addr - m.second;
        src = m;
      }
    }
  }
  if (diff == std::numeric_limits<size_t>::max()) [[unlikely]] {
    SDB_ASSERT(false);
    return 0;
  }
  return src.first + (addr - src.second);
}

uint64_t RemappedBytesViewInput::SourceToInternal(
  uint64_t offset) const noexcept {
  SDB_ASSERT(!_mapping.empty());
  auto it = absl::c_lower_bound(
    _mapping, offset, [](const auto& l, const auto& r) { return l.first < r; });
  if (it == _mapping.end()) {
    --it;
  } else if (it->first > offset) {
    SDB_ASSERT(it != _mapping.begin());
    --it;
  }
  return it->second + (offset - it->first);
}

}  // namespace irs
