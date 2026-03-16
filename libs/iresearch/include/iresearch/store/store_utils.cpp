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

#include "basics/crc.hpp"
#include "basics/memory.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"

namespace irs {

size_t BytesViewInput::ReadBytes(byte_type* b, size_t size) noexcept {
  size =
    std::min<size_t>(size, std::distance(_pos, _data.data() + _data.size()));
  if (size != 0) {
    std::memcpy(b, _pos, sizeof(byte_type) * size);
    _pos += size;
  }
  return size;
}

size_t BytesViewInput::ReadBytes(uint64_t offset, byte_type* b,
                                 size_t size) noexcept {
  if (offset < _data.size()) {
    size = std::min(size, size_t(_data.size() - offset));
    std::memcpy(b, _data.data() + offset, sizeof(byte_type) * size);
    _pos = _data.data() + offset + size;
    return size;
  }

  _pos = _data.data() + _data.size();
  return 0;
}

void BytesViewInput::ReadBytes(bstring& buf, size_t size) {
  const auto used = buf.size();

  buf.resize(used + size);

  [[maybe_unused]] const auto read = ReadBytes(buf.data() + used, size);
  SDB_ASSERT(read == size);
}

uint32_t BytesViewInput::Checksum(uint64_t offset) const {
  Crc32c crc;

  crc.process_bytes(_pos,
                    std::min((_pos - _data.data()) + offset, _data.size()));

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
