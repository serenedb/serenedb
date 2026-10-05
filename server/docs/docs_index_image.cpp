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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <vector>

#include "docs/docs_index_data.h"

namespace sdb::docs {
namespace {

constexpr std::size_t kAlignment = 64;

struct alignas(kAlignment) Header {
  char tag[16] = {'s', 'd', 'b', '-', 'd', 'o', 'c', 's',
                  '-', 'i', 'n', 'd', 'e', 'x', '-', '1'};
  std::uint64_t capacity = 0;
  std::uint64_t size = 0;
};

#ifdef __APPLE__
constexpr std::size_t kCapacity = std::size_t{4} << 20;

struct Region {
  Header header{.capacity = kCapacity};
  std::uint8_t payload[kCapacity] = {};
};

[[gnu::used]] constinit Region gRegion;

const Header* GetHeader() { return &gRegion.header; }
#else
[[gnu::used, gnu::section(".sdb_docs")]] constinit const Header gHeader{};

const Header* GetHeader() { return &gHeader; }
#endif

class Cursor {
 public:
  explicit Cursor(const std::uint8_t* data) : _data{data} {}

  template<typename T>
  T Read() {
    T value;
    std::memcpy(&value, _data + _offset, sizeof(T));
    _offset += sizeof(T);
    return value;
  }

  std::span<const std::uint8_t> Take(std::size_t size) {
    std::span<const std::uint8_t> bytes{_data + _offset, size};
    _offset += size;
    return bytes;
  }

  void Align() {
    _offset = (_offset + kAlignment - 1) / kAlignment * kAlignment;
  }

 private:
  const std::uint8_t* _data;
  std::size_t _offset = 0;
};

std::vector<IndexFile> ReadIndex(Cursor& cursor) {
  std::vector<IndexFile> files(cursor.Read<std::uint32_t>());
  for (auto& file : files) {
    const auto name = cursor.Take(cursor.Read<std::uint32_t>());
    file.name = {reinterpret_cast<const char*>(name.data()), name.size()};
    const auto size = cursor.Read<std::uint64_t>();
    cursor.Align();
    file.bytes = cursor.Take(size);
  }
  return files;
}

struct Image {
  std::vector<IndexFile> docs;
  std::vector<IndexFile> objects;
};

const Image& GetImage() {
  static const Image kImage = [] {
    const Header* header = GetHeader();
    asm volatile("" : "+r"(header));
    Image image;
    if (header->size != 0) {
      Cursor cursor{reinterpret_cast<const std::uint8_t*>(header) +
                    sizeof(Header)};
      image.docs = ReadIndex(cursor);
      image.objects = ReadIndex(cursor);
    }
    return image;
  }();
  return kImage;
}

}  // namespace

std::span<const IndexFile> GetDocsIndex() { return GetImage().docs; }

std::span<const IndexFile> GetObjectsIndex() { return GetImage().objects; }

}  // namespace sdb::docs
