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

#include <iresearch/store/data_output.hpp>
#include <iresearch/utils/crc.hpp>
#include <iresearch/utils/resource_manager.hpp>
#include <iresearch/utils/string.hpp>
#include <iterator>
#include <utility>

namespace irs {

class MemoryFile {
 public:
  explicit MemoryFile(IResourceManager& /*rm*/) noexcept {}

  uint64_t Length() const noexcept { return _bytes.size(); }
  bytes_view Bytes() const noexcept { return _bytes; }

  template<typename Visitor>
  void Visit(const Visitor& visitor) const {
    visitor(_bytes.data(), _bytes.size());
  }

  void Append(const byte_type* data, size_t size) { _bytes.append(data, size); }

 private:
  bstring _bytes;
};

class MemoryIndexOutput final : public IndexOutput {
 public:
  explicit MemoryIndexOutput(MemoryFile& file) noexcept : _file{file} {
    _begin = _pos = _buf;
    _end = std::end(_buf);
    _offset = file.Length();
  }

  void Flush() final {
    Append(_begin, static_cast<size_t>(_pos - _begin));
    _pos = _begin;
  }

  uint32_t Checksum() final {
    Flush();
    return std::exchange(_crc, Crc32c{}).checksum();
  }

 private:
  void CloseImpl() final { Flush(); }

  void Overflow(const byte_type* data, size_t size) final {
    Flush();
    Append(data, size);
  }

  void Append(const byte_type* data, size_t size) {
    if (size == 0) {
      return;
    }
    _file.Append(data, size);
    _crc.process_bytes(data, size);
    _offset += size;
  }

  byte_type _buf[64];
  MemoryFile& _file;
  Crc32c _crc;
};

struct MemoryOutput {
  explicit MemoryOutput(IResourceManager& rm) noexcept : file{rm} {}

  MemoryFile file;
  MemoryIndexOutput stream{file};
};

}  // namespace irs
namespace tests {

inline irs::bstring Bytes(const irs::MemoryFile& file) {
  return irs::bstring{file.Bytes()};
}

}  // namespace tests
